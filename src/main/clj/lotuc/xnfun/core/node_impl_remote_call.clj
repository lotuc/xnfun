(ns lotuc.xnfun.core.node-impl-remote-call
  (:require
   [clojure.core.async :refer [<! <!! >!! chan close! dropping-buffer go-loop]]
   [clojure.tools.logging :as log]
   [lotuc.xnfun.core.node-defaults :as defaults]
   [lotuc.xnfun.core.node-utils :refer [send-msg add-sub]]
   [lotuc.xnfun.core.utils :refer [swap!-swap-in-delayed!]]
   [lotuc.xnfun.protocols :as xnp]))

(defn select-remote-node
  "Filter out node that satisties the condition and randomly selects one.
  - `match-fn`: matches if (match-fn node)"
  [{:as node} {:keys [match-fn]}]
  (let [node-ids
        (->> @(xnp/node-remote-nodes node)
             (filter #(if match-fn (match-fn (second %)) true))
             (map first))]
    (if (empty? node-ids) nil (rand-nth node-ids))))

;; RPC related data
;; :req
;;   :typ :xnfun/call | :xnfun/to-callee | :xnfun/cancel
;; :resp
;;   :typ :xnfun/to-caller | :xnfun/hb | :xnfun/resp

(defn- rpc-client--handle-caller-msg
  "Message will be forward to [[rpc-server--on-req-msg]]"
  [{:keys [req-id in-c send-req]}]
  (go-loop []
    (let [d (<! in-c)]
      (when-let [{:keys [typ]} d]
        (try
          (if (or (= typ :xnfun/to-callee)
                  (= typ :xnfun/cancel))
            (send-req d)
            (log/warnf "rpc client unkown message from caller: %s" d))
          (catch Exception e
            (log/warnf e "[%s] error handle caller msg: %s" req-id d)))
        (recur)))))

(defn- rpc-client--handle-resp-from-callee
  "Message from [[rpc-server--handle-callee-msg]]"
  [{:keys [node req-id out-c]} {:as msg :keys [typ data]}]
  (when-not (= typ :resp)
    (log/warnf "[%s] should be :resp, but got %s" (:node-id node) msg)
    (->> {:msg msg} (ex-info (str "should be :resp, but got " typ)) throw))

  (if-let [{:as res-promise :keys [hb]} (xnp/get-promise node req-id)]
    (let [{:as msg-data :keys [typ data]} data]
      (cond
        (= typ :xnfun/resp)
        (let [{:keys [status data]} data]
          (->>
           (cond
             (= status :ok)        {:status :ok :data data}
             (= status :err)       {:status :err :data data}
             (= status :xnfun/err) {:status :xnfun/remote-err :data data}

             :else {:status :xnfun/remote-err
                    :data {:typ :xnfun/err-invalid-resp :data data}})
           (xnp/fullfill-promise! node req-id)))

        (#{:xnfun/to-caller :xnfun/hb} typ)
        (do (when hb (hb)) (>!! out-c msg-data))

        :else
        (log/warnf "[%s] recv unkown :resp message from %s - %s"
                   (:node-id node) req-id typ)))
    (log/warnf "the response promise is already fullfilled: %s" req-id)))

(defn- rpc-server--handle-req--call
  "Handle the callee message/result to caller.

  1. waiting on callee's `out-c`, and forward message to caller.

  2. waiting on callee's result, send back to caller.

  Notice that the message is forward to caller and handle
  by [[rpc-client--handle-resp-data-from-callee]]
  "
  [{:keys [out-c send-resp fut]}]
  (go-loop [d (<!! out-c)]
    (when-let [{:keys [typ]} d]
      (if (#{:xnfun/hb :xnfun/to-caller} typ)
        (send-resp d)
        (log/warnf "illegal message from callee: %s" typ))
      (recur (<!! out-c))))

  (future
    (send-resp
     (try
       (let [r @fut]
         {:typ :xnfun/resp
          :data {:status :ok :data r}})
       (catch Exception e
         {:typ :xnfun/resp
          :data {:status :err
                 :data {:exception-class (str (class e))}}})))))

(defn- rpc-server--handle-req-from-caller
  "Handle the `:req` message from remote caller to current `node`.

  1. For new call, initiate the call by [[submit-call!]] to current node. and
  waiting message via [[remote-call-server-handle-callee-msg]].

  2. For initiated call, forward message to callee.
  "
  [{:as node :keys [node-id]}
   {:as msg
    {:as msg-typ}                   :typ
    {:as msg-data :keys [typ data]} :data}]
  (when-not (= msg-typ :req)
    (throw (ex-info "should be :req message" {:msg msg})))

  (let [{:as msg-meta :keys [req-id caller-node-id]} (meta msg)]
    (log/debugf "[%s] recv req msg %s - %s" node-id msg msg-meta)
    (cond
      (= typ :xnfun/call)
      (let [[[fun-name params] req-meta] data
            _ (log/debugf "[%s] recv call %s %s %s"
                          node-id fun-name params req-meta)

            ;; handles to the callee for its interactive message
            out-c (chan 1)
            options {:out-c out-c :req-meta req-meta}

            send-resp
            (fn [d] (send-msg node {:typ :resp :data d} msg-meta))

            {:keys [fut]}
            (xnp/submit-call! node fun-name params options)]

        (log/infof "[%s] submit call %s: %s" node-id fun-name params)
        (rpc-server--handle-req--call
         {:out-c out-c :send-resp send-resp :fut fut}))

      (#{:xnfun/to-callee :xnfun/cancel} typ)
      (if-let [{:keys [in-c]} (xnp/get-call node req-id)]
        (>!! in-c msg-data)
        (log/warnf "[%s] request [%s] not found from [%s]"
                   node-id req-id caller-node-id))

      :else
      (log/warnf "[%s] illegal :req message type: %s[%s] from %s"
                 node-id typ req-id caller-node-id))))

(defn- rpc-client--submit-call-to-remote-node!
  "Here we do the submit.

  1. Send request to remote callee
  2. Register remote callee message handler
  3. Register caller message handler

  And this function assumes that `req-meta` is already initialized
  via [[make-req-meta]].
  "
  [{:as node :keys [node-id]}
   fun-name params callee-node-id
   & {:as options :keys [req-meta out-c]}]

  (let [in-c  (chan 1)
        out-c (or out-c (chan (dropping-buffer 1)))
        req-meta (assoc req-meta :callee-node-id callee-node-id)

        p (->> {:req-meta req-meta
                :fun-name fun-name
                :params params
                :in-c in-c
                :out-c out-c}
               (xnp/submit-promise! node))

        req-id (:req-id req-meta)
        res-promise (:res-promise p)
        msg-meta {:callee-node-id callee-node-id :req-id req-id}

        send-req (fn [msg]
                   (log/debugf "send req msg: %s - %s" msg msg-meta)
                   (send-msg node {:typ :req :data msg} msg-meta))]

    (log/debugf "[%s] [-> %s %s] start handle caller message"
                node-id callee-node-id req-id)
    (rpc-client--handle-caller-msg
     {:req-id req-id :in-c in-c :send-req send-req})

    (log/debugf "[%s] [-> %s %s] start handle callee message"
                node-id callee-node-id req-id)
    (let [handle-fn (->> {:node node :req-id req-id :out-c out-c}
                         (partial rpc-client--handle-resp-from-callee))
          unsub (add-sub node :resp handle-fn msg-meta)]
      (future (try @res-promise (finally (unsub)))))

    (future (try @res-promise (finally (close! in-c) (close! out-c))))

    (log/debugf "[%s] [-> %s %s] initiate request :req :xnfun/call"
                node-id callee-node-id req-id)
    (send-req {:typ :xnfun/call :data [[fun-name params] req-meta]})
    p))

(defn submit-remote-call!
  "Submit call to remote nodes.

  Node is selected by the optional `match-node-fn` and the `fun-name`."
  [node fun-name params
   & {:as options :keys [req-meta out-c match-node-fn]}]
  (let [options
        (assoc options :req-meta (defaults/make-req-meta req-meta))

        callee-node-id
        (->> {:match-fn
              (if match-node-fn
                (fn [n] (and (get-in n [:functions fun-name])
                             (match-node-fn n)))
                (fn [n] (get-in n [:functions fun-name])))}
             (select-remote-node node))]
    (when-not callee-node-id
      (throw (ex-info "no matching node" {:fun-name fun-name})))
    (rpc-client--submit-call-to-remote-node!
     node fun-name params callee-node-id options)))

(defn stop-serve-remote-call!
  [node]
  (swap!-swap-in-delayed!
   (xnp/node-rpc-listener node)
   {:ignores-nil? false}
   (fn [v]
     (when-let [{:keys [stop]} (and v @v)] (stop))
     nil)))

(defn start-serve-remote-call!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (xnp/node-rpc-listener node)
   {:ignores-nil? false}
   (fn [v]
     (when-let [{:keys [stop]} (and v @v)] (stop))

     (log/infof "[%s] start serving remote call" node-id)
     (let [handle-fn (partial rpc-server--handle-req-from-caller node)
           unsub (add-sub node :req handle-fn)]
       {:stop #(do (log/infof "[%s] stop serving remote call" node-id)
                   (unsub))}))))
