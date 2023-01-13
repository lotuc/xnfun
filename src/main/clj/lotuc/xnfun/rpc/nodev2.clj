(ns lotuc.xnfun.rpc.nodev2
  "Node abstraction for xnfun.

  - You can start a link (ex. MQTT link) for connecting with other nodes.
  - You can register (capability) functions to current node.
  - You can submit function call to current node (call you registered functions).
  - You can register a promise to current node and maybe fullfill it later.
  - You can do call to other node through current node via your established
    link

  When caller call from one node to other node's function (callee), this will
  - Submit a promise in current node waiting for result.
  - Submit a future (representing the remote computation, the callee) in remote
    node.
  - Create a bi-directional communication channel between caller and callee.

  **Node**
  - [[make-node]]  [[node-info]] [[start-node-link!]]
  - [[on-heartbeat!]]  [[on-remove-dead-workers!]]
  - [[start-heartbeat!]]
  - [[get-remote-nodes]]  [[select-remote-node]]

  **Capabilities**
  - [[add-function!]] [[call-function!]]

  **Promises**
  - [[fullfill-promise!]] [[submit-promise!]]

  **Function**
  - [[submit-call!]]

  **Remote function**
  - [[submit-remote-call!]] [[serve-remote-call!]]"
  (:require
   [clojure.core.async :refer [<! <!! >!! chan close! dropping-buffer go-loop]]
   [clojure.tools.logging :as log]
   [lotuc.xnfun.utils :refer [max-arity *now-ms* *run-at* *periodic-run*]]
   [lotuc.xnfun.rpc.mqtt-link :refer [new-mqtt-link]]
   [lotuc.xnfun.rpc.link :as l]))

(defn- make-req-meta [req-meta]
  (let [req-id         (or (:req-id req-meta) (str (random-uuid)))
        timeout-ms     (or (:timeout-ms req-meta) 60000)
        hb-interval-ms (:hb-interval-ms req-meta)
        hb-lost-ratio  (if hb-interval-ms (or (:hb-lost-ratio req-meta) 2.5) nil)]
    (cond-> (or req-meta {})
      req-id         (assoc :req-id req-id)
      timeout-ms     (assoc :timeout-ms timeout-ms)
      hb-interval-ms (assoc :hb-interval-ms hb-interval-ms)
      hb-lost-ratio  (assoc :hb-lost-ratio hb-lost-ratio))))

(defn- make-node-hb-options
  [options]
  (-> (or options {})
      (update :hb-interval-ms #(or % 30000))
      (update :hb-lost-ratio  #(or % 2.5))))

(defn- make-node-link-options
  "We use mqtt as the default communication link."
  [node-id link]
  (or link
      {:xnfun/module 'xnfun.mqtt
       :mqtt-topic-prefix ""
       :mqtt-config
       {:broker "tcp://127.0.0.1:1883"
        :client-id node-id
        :connect-options {:max-in-flight 1000
                          :automatic-reconnect true}}}))

(defn- make-node-options
  "Options of the node.

  Options contains:
  - `hb-options`:
    - `:hb-interval-ms`: Heartbeat interval for this node.
    - `:hb-lost-ratio`: Consider this node to be heartbeat lost in
      (* hb-lost-ratio hb-interval-ms) when no heartbeat occurs.
  - `:link`: The link this node would make to connect to other nodes.

  Arguments:
  - `node-options`: default options."
  [node-id node-options]
  (-> (or node-options {})
      (update :hb-options #(make-node-hb-options %))
      (update :link #(make-node-link-options node-id %))))

(defn- make-node-state
  "State of the node.

  State contains:
  - `:local`: All loccal states.
    - `:functions`: Functions we locally support.
    - `:futures`: Locally running function instances.
    - `:link`: Node's link.
  - `:remote`: What we know about remote nodes and interactions with remote nodes.
    - `:nodes`: Knowleges about remote nodes. It's driven by *message*, so we
      choose agent for it's message's async handling.
    - `:promises`: Promises waiting for remote responses."
  []
  {:local  {:hb        (atom {})
            :functions (atom {})
            :futures   (atom {})
            :link      (atom nil)}
   :remote {:nodes     (agent {})
            :promises  (atom {})}})

(defn make-node
  "Create a node.

  A node contains:
  - `:node-id`: The global identifier for given connection link.
  - `:node-options`: The options specified for given node. Check [[make-node-options]]
     for details.
  - `:node-state`: The mutable state for this node. Check [[make-node-state]] for
    details."
  [& {:keys [node-id node-options]}]
  (let [node-id (or node-id (str (random-uuid)))]
    {:node-id      node-id
     :node-options (make-node-options node-id node-options)
     :node-state   (make-node-state)}))

(defn node-info
  "Retrieve node's supported functions as:
  `function-name -> {:keys [arity]}`"
  [{:as node :keys [node-state]}]
  (->> node-state :local :functions deref
       (map (fn [[k v]] [k (select-keys v [:arity])]))
       (into {})))

(defn start-node-link!
  "Start node link."
  [{:as node :keys [node-id] {:keys [link]} :node-state}]
  (let [link-options (-> node :node-options :link)
        link-module  (:xnfun/module link-options)]
    (if (= link-module 'xnfun.mqtt)
      (let [link (-> node :node-state :local :link)
            [old new]
            (swap-vals! link (fn [_] (delay (new-mqtt-link node-id link-options))))]
        (if (or (nil? old) (realized? old))
          (try (when (and (not (nil? old)) (not (l/closed? @old)))
                 (l/close! @old))
               @new
               (catch Exception _ (reset! link old)))
          (do (reset! link old)
              (->> {:node node} (ex-info "someone else is starting") throw))))
      (->> {:link (-> node :node-options :link)}
           (ex-info "unkown link")
           throw))))

(defn start-heartbeat!
  [{:as node :keys [node-id]}]
  (let [hb (-> node :node-state :local :hb)

        {:keys [hb-interval-ms]}
        (get-in node [:node-options :hb-options])

        link @(-> node :node-state :local :link)

        v
        (swap!
         hb
         (fn [v]
           (when v (try (.close @v) (catch Exception _)))
           (delay (*periodic-run*
                   (*now-ms*)
                   hb-interval-ms
                   (fn [_]
                     (println "run...")
                     (->> (with-meta
                            {:typ :hb :data (node-info node)}
                            {:node-id node-id})
                          (l/send-msg link)))))))]
    (println "!!!!" v)
    @v))

(defn- on-heartbeat*!
  [remote-nodes {:keys [node-id hb-options functions]}]
  (->>
   (fn [{:keys [hb-at] old-functions :functions old-hb-options :hb-options}]
     {:hb-at      (*now-ms*)
      :hb-options (if hb-options hb-options (or old-hb-options {}))
      :functions  (if functions functions (or old-functions {}))})
   (update remote-nodes node-id)))

(defn on-heartbeat!
  "Handle heartbeat message from `node-id`.

  The node's hb message may contains the functions it now supports."
  [{:as node} {:keys [node-id functions hb-options] :as hb-msg}]
  (-> (-> node :node-state :remote :nodes)
      (send on-heartbeat*! hb-msg)))

(defn- on-remove-dead-workers*!
  "If the remote node itself does not report it's hb options, we'll use current
  node's hb options as the default."
  [remote-nodes {:keys [hb-interval-ms hb-lost-ratio] :as node-hb-options}]
  (let [now-ms (*now-ms*)]
    (->>
     remote-nodes
     (filter
      (fn [[_ {:keys [hb-at hb-options]}]]
        (> now-ms (+ hb-at
                     (* (or (:hb-interval-ms hb-options) hb-interval-ms)
                        (or (:hb-lost-ratio hb-options)  hb-lost-ratio))))))
     (into {}))))

(defn on-remove-dead-workers!
  "Try remove all node that losts their heartbeat."
  [{:as node :keys [node-options node-state]}]
  (-> (-> node :node-state :remote :nodes)
      (send on-remove-dead-workers*! (:hb-options node-options))))

(defn get-remote-nodes
  "get all remote nodes."
  [node]
  (-> node :node-state deref :worker-nodes))

(defn select-remote-node
  "Filter out node that satisties the condition and randomly selects one.
  - `match-fn`: matches if (match-fn node)"
  [{:as node} {:keys [match-fn]}]
  (let [node-ids
        (->> (get-remote-nodes node)
             (filter #(if match-fn (match-fn (second %)) true))
             (map first))]
    (if (empty? node-ids) nil (rand-nth node-ids))))

(defn- add-function*!
  [local-functions fun-name fun &
   {:keys [overwrite] :or {overwrite true}}]
  (let [func-arity (max-arity fun)]
    (when-not (#{1 2} func-arity)
      (throw (ex-info "invalid rpc function (arity not match)"
                      {:function-name fun-name :function fun})))
    (->>
     (fn [prev-fun]
       (when (and prev-fun (not overwrite))
         (->> {:function-name fun-name :function fun}
              (ex-info "function already exists")
              throw))
       {:function fun :arity func-arity})
     (update local-functions fun-name))))

(defn add-function!
  "Register a supported function to current node.

  With `overwrite` be false, we through if the same name already exists.
  "
  [{:as node} fun-name fun &
   {:as opts :keys [overwrite]}]
  (-> (-> node :node-state :local :functions)
      (swap! add-function*! fun-name fun opts)))

(defn call-function!
  "Call registered function with name.

  All data should be format of {:keys [typ data]}

  Arguments:
  - `out-c` (optional): function can send message to caller via the channel
    - `:typ=:xnfun/hb`: Long-running function heartbeat
    - `:typ=:xnfun/to-caller`: Message to caller
  - `in-c` (optional): caller send some signal to callee
    - `:typ=:xnfun/cancel`: Cancellation message.
    - `:typ=:xnfun/to-callee`: Message to callee."
  [node fun-name params
   & {:keys [out-c in-c req-meta]}]
  (let [{:keys [function arity]}
        (-> node
            (get-in [:node-state :local :functions])
            deref
            (get fun-name))]
    (when-not function
      (->> {:function-name fun-name :params params}
           (ex-info "function not found")
           throw))
    (if (= arity 2)
      (apply function [params {:out-c (or out-c (chan (dropping-buffer 1)))
                               :in-c  (or in-c  (chan (dropping-buffer 1)))
                               :req-meta req-meta}])
      (apply function [params]))))

(defn fullfill-promise!
  "Handling data from callee.

  Arguments:
  - `:status`: `:ok` or `:xnfun/err` or `:xnfun/remote-err` or `:err`
    - When error is assured triggered by the user's function code, no matter it
      runs locally or remotely, the status should be `:err`
    - When error is not assured triggered by user's function code:
      - `:xnfun/err`: Error occurs locally
      - `:xnfun/remote-err`: Error occurs remotely
  - `:meta`: the call metadata.
  - `:data`: If `status=:ok`, means the fullfilled data; else the data
    describing the error.
  "
  [{:as node :keys [node-id]} req-id {:as r :keys [status data]}]
  (let [[old _]
        (swap-vals!
         (-> node :node-state :remote :promises)
         (fn [m] (dissoc m req-id)))]
    (if-let [p (get old req-id)]
      (let [{:keys [hb-lost-timer timeout-timer res-promise]} @p]
        (log/debugf "[%s] fullfilled [%s]: %s" node-id req-id data)
        (deliver res-promise r)
        (when hb-lost-timer (.close hb-lost-timer))
        (when timeout-timer (.close timeout-timer)))
      (log/debugf "[%s] request not found [%s]: %s" node-id req-id r))))

(defn submit-promise!
  "Submit promise to node.

  The submitted promise may be fullfilled on timeout. Or can be fullfilled
  manually with: [[fullfill-promise!]]

  Returns: {:keys [res-promise request timeout-timer hb-lost-timer]}
  - `request`: {:keys [`req-meta`]}"
  [{:as node :keys [node-id]}
   {:as request :keys [req-meta]}]
  (let [promises (-> node :node-state :remote :promises)
        req-meta (make-req-meta req-meta)
        req-id   (:req-id req-meta)
        request  (assoc request :req-meta req-meta)

        {:keys [timeout-ms hb-lost-ratio hb-interval-ms]}
        req-meta

        on-timeout
        (fn [_]
          (->> {:status :xnfun/err :data {:typ :timeout :reason "timeout"}}
               (fullfill-promise! node req-id)))

        on-hb-lost
        (fn [_]
          (->> {:status :xnfun/err :data {:typ :timeout :reason "hb-lost"}}
               (fullfill-promise! node req-id)))

        do-hb
        (fn []
          (let [hb-lost-at (+ (*now-ms*) (* hb-lost-ratio hb-interval-ms))]
            {:hb-lost-at hb-lost-at
             :hb-lost-timer (*run-at* hb-lost-at on-hb-lost)}))

        hb
        (fn []
          (-> (swap!
               promises
               (fn [m]
                 (if-let [p (get m req-id)]
                   (delay
                     (let [{:as p :keys [hb-lost-timer]} @p]
                       (when hb-lost-timer (.close hb-lost-timer))
                       (log/debugf "[%s][%s] heartbeat will lost in (* %s %s)ms"
                                   node-id req-id hb-lost-ratio hb-interval-ms)
                       (merge p (do-hb))))
                   m)))
              (get req-id (delay nil))
              deref))

        p
        (-> (swap!
             promises
             (fn [m]
               (if (contains? m req-id)
                 m
                 (->> (delay
                        (cond-> {:res-promise   (promise)
                                 :request       request
                                 :timeout-timer (*run-at*
                                                 (+ (*now-ms*) timeout-ms)
                                                 on-timeout)}
                          hb-interval-ms (assoc :hb hb)
                          hb-interval-ms (merge (do-hb))))
                      (assoc m req-id)))))
            (get req-id)
            deref)]
    p))

(defn- cleanup-function-call
  [{:as node :keys [node-id]} req-id]
  (let [[old _] (swap-vals!
                 (-> node :node-state :local :futures)
                 #(dissoc % req-id))]
    (when-let [req (get old req-id)]
      (let [{:keys [in-c out-c hb-lost-timer timeout-timer]} @req]
        (future
          (Thread/sleep 100)
          (close! out-c)
          (close! in-c)
          (when hb-lost-timer (.close hb-lost-timer))
          (when timeout-timer (.close timeout-timer)))))))

(defn- submit-function-call!*
  [{:as node :keys [node-id]}
   fun-name params {:as options :keys [req-meta in-c out-c]}]
  (let [futures        (-> node :node-state :local :futures)
        in-c-internal  (chan 1)
        out-c-internal (chan 1)

        {:keys [timeout-ms hb-interval-ms hb-lost-ratio req-id]}
        req-meta

        cleanup
        (fn []
          (future
            (Thread/sleep 100)
            (close! in-c-internal)
            (close! out-c-internal))
          (cleanup-function-call node req-id))

        f
        (future
          (try
            (let [p {:in-c in-c-internal :out-c out-c-internal :req-meta req-meta}
                  r (call-function! node fun-name params p)]
              (>!! out-c {:status :ok :data r})
              r)
            (catch Exception e
              (>!! out-c {:status :err :exception e})
              (throw e))
            (finally
              (cleanup))))

        cancel-run
        (fn [data]
          (>!! in-c-internal {:typ :xnfun/cancel :data data})
          (future (Thread/sleep 100) (future-cancel f)))

        on-timeout
        (fn [_]
          (let [d {:typ :timeout :reason "timeout"}]
            (cancel-run d)
            (>!! out-c {:status :xnfun/err :data d})))

        on-hb-lost
        (fn [_]
          (let [d {:typ :timeout :reason "hb-lost"}]
            (cancel-run d)
            (>!! out-c {:status :xnfun/err :data d})))

        do-hb
        (fn []
          (let [hb-lost-at (+ (*now-ms*) (* hb-lost-ratio hb-interval-ms))]
            {:hb-lost-at hb-lost-at
             :hb-lost-timer (*run-at* hb-lost-at on-hb-lost)}))

        hb
        (fn []
          (-> (swap!
               futures
               (fn [m]
                 (if-let [p (get m req-id)]
                   (delay
                     (let [{:as p :keys [hb-lost-timer]} @p]
                       (when hb-lost-timer (.close hb-lost-timer))
                       (log/debugf "[%s][%s] heartbeat will lost in (* %s %s)ms"
                                   node-id req-id hb-lost-ratio hb-interval-ms)
                       (merge p (do-hb))))
                   m)))
              (get req-id (delay nil))
              deref))]

    (log/debugf "[%s][%s] waiting for callee message" node-id req-id)
    (go-loop [d (<! out-c-internal)]
      (when-let [{:keys [typ]} d]
        (try
          (log/debugf "[%s][%s][%s] callee message: %s" node-id req-id typ d)
          ;; all message counts as heartbeat message.
          (when hb-interval-ms (future (hb)))

          ;; Forward message, and always blocking when forwarding.
          (>!! out-c d)
          (catch Exception e
            (log/warnf e "[%s][%s] error handling msg from callee: %s" node-id req-id d)))
        (recur (<! out-c-internal))))

    (log/debugf "[%s][%s] waiting for caller message" node-id req-id)
    (go-loop [d (<! in-c)]
      (when-let [{:keys [typ data]} d]
        (try
          (if (= typ :xnfun/cancel)
            (cancel-run {:typ :xnfun/caller-cancel :data data})
            (>!! in-c-internal d))
          (catch Exception e
            (log/warnf e "[%s][%s] error handling msg from caller: %s" node-id req-id d)))
        (recur (<! in-c))))

    (cond-> {:req-id    req-id
             :submit-at (*now-ms*)
             :request   {:fun-name fun-name
                         :params params
                         :req-meta req-meta}
             :running-future f
             :in-c           in-c
             :out-c          out-c
             :timeout-timer (*run-at*
                             (+ (*now-ms*) timeout-ms)
                             on-timeout)}
      hb-interval-ms (assoc :hb hb)
      hb-interval-ms (merge (do-hb)))))

(defn- get-call [node req-id]
  (let [f (-> @(-> node :node-state :local :futures)
              (get req-id))]
    (if f @f nil)))

(defn submit-call!
  "Submit function to node and run it asynchronously.

  Arguments
  - `out-c`: callee will send message (and heartbeat) throught this
     channel.
     - If not given, will create a dropping buffer channel
     - Notice that the out-c would block callee, so you should handle the message
       as soon as possible.
  - `req-meta`: Check [[make-req-meta]] for details.

  Returns {:keys [req-id submit-at request running-future in-c out-c
                  timeout-timer hb-lost-timer]}

  Check [[call-function!]] for `in-c` and `out-c`.
  "
  [{:as node :keys [node-id]}
   fun-name params
   & {:as options :keys [req-meta out-c]}]
  (let [req-meta (make-req-meta req-meta)
        req-id   (:req-id req-meta)
        options  (-> options
                     (assoc :req-meta req-meta)
                     (assoc :in-c (chan 1))
                     (assoc :out-c (or out-c (chan (dropping-buffer 1)))))]
    (-> (swap!
         (-> node :node-state :local :futures)
         (fn [m]
           (->>
            #(or % (delay (submit-function-call!* node fun-name params options)))
            (update m req-id))))
        (get req-id)
        deref)))

(defn- check-link [node]
  (let [link @(-> node :node-state :local :link)]
    (when (not link)
      (->> {:node node}
           (ex-info "link is not created")
           throw))

    (when (l/closed? link)
      (->> {:node node}
           (ex-info "link is closed")
           throw))

    link))

;; RPC related data
;; :req
;;   :typ :xnfun/call | :xnfun/to-callee | :xnfun/cancel
;; :resp
;;   :typ :xnfun/to-caller | :xnfun/hb | :xnfun/resp

(defn- submit-remote-call-to-node!
  [{:as node :keys [node-id]}
   fun-name params callee-node-id
   & {:as options :keys [req-meta out-c]}]

  (let [link    (check-link node)
        in-c  (chan 1)
        out-c (or out-c (chan (dropping-buffer 1)))

        p        (submit-promise! node {:req-meta req-meta})
        hb       (:hb p)
        req-meta (-> p :request :req-meta)
        req-id   (:req-id req-meta)

        m {:callee-node-id callee-node-id :req-id req-id}]

    (go-loop []
      (let [d (<! in-c)]
        (when-let [{:keys [typ]} d]
          (try
            (if
             (or (= typ :xnfun/to-callee)
                 (= typ :xnfun/cancel))
              (->> (with-meta {:type :req :data d} m)
                   (l/send-msg link))
              (log/warnf "unkown message from caller: %s" d))
            (catch Exception e
              (log/warnf e "[%s] error handle caller msg: %s" req-id d)))
          (recur))))

    (->>
     (with-meta
       {:types [:resp]
        :handle-fn
        (fn [{:as msg :keys [typ data]}]
          ;; Message from remote
          (case typ
            :xnfun/resp
            (let [{:keys [status data]} data]
              (->>
               (cond
                 (= status :ok)        {:status :ok :data data}
                 (= status :err)       {:status :err :data data}
                 (= status :xnfun/err) {:status :xnfun/remote-err :data data}

                 :else {:status :xnfun/remote-err
                        :data {:typ :xnfun/err-invalid-resp :data data}})
               (fullfill-promise! node req-id)))

            :xnfun/to-caller
            (do (when hb (hb)) (>!! out-c data))

            :xnfun/hb
            (when hb (hb))))}
       m)
     (l/sub-msg link))

    (->>
     (with-meta {:typ :req
                 :data {:typ :xnfun/call
                        :data [[fun-name params] req-meta]}}
       m)
     (l/send-msg link))

    p))

(defn submit-remote-call!
  [{:as node :keys [node-id]}
   fun-name params
   & {:as options :keys [req-meta out-c match-node-fn]}]
  (let [callee-node-id
        (select-remote-node node {:match-fn match-node-fn})]
    (submit-remote-call-to-node! node fun-name params callee-node-id options)))

(defn serve-remote-call!
  [{:as node :keys [node-id]}]
  (let [link (check-link node)

        handle-msg
        (fn [msg {:as m :keys [req-id caller-node-id]}]
          (if-let [{:keys [in-c]} (get-call node req-id)]
            (>!! in-c msg)
            (log/warnf "request [%s] not found from [%s]" req-id caller-node-id)))

        handle-call
        (fn [[[fun-name params] req-meta m]]
          (let [out-c (chan 1)
                options {:out-c out-c :req-meta req-meta}

                {:keys [out-c running-future]}
                (submit-call! node fun-name params options)]

            (go-loop [d (<!! out-c)]
              (when d
                (->> (with-meta {:typ :resp :data d} m)
                     (l/send-msg link))
                (recur (<!! out-c))))

            (future
              (->>
               (with-meta
                 (try
                   (let [r @running-future]
                     {:typ :resp
                      :data {:typ :xnfun/resp
                             :data {:status :ok :data r}}})
                   (catch Exception e
                     {:typ :resp
                      :data
                      {:typ :xnfun/resp
                       :data {:status :err
                              :data {:exception-class (str (class e))}}}}))
                 m)
               (l/send-msg link)))))]
    (->>
     (with-meta
       {:types [:req]
        :handle-fn
        (fn [{:as msg :keys [typ data]}]
          (let [{:as m :keys [req-id caller-node-id]} (meta msg)]
            (if (= typ :xnfun/call)
              (handle-call data)
              (handle-msg msg m))
            (case typ
              :xnfun/call
              :xnfun/to-callee (handle-to-callee)
              :xnfun/cancel (handle-cancel)
              (log/warnf "unkown message from caller: %s %s"
                         caller-node-id req-id))))})
     (l/sub-msg link))))
