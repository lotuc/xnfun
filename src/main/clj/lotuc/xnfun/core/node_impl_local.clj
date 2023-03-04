(ns lotuc.xnfun.core.node-impl-local
  (:require
   [clojure.core.async :refer [<! >!! chan close! dropping-buffer go-loop]]
   [clojure.tools.logging :as log]
   [lotuc.xnfun.core.node-defaults :as defaults]
   [lotuc.xnfun.core.utils
    :refer [max-arity swap!-swap-in-delayed! *now-ms* *run-at*]]
   [lotuc.xnfun.protocols
    :refer [node-functions node-futures node-promises]]))

(def ^:dynamic *debug* nil)

(defn- add-function*
  [local-functions fun-name fun & {:keys [overwrite] :or {overwrite true}}]
  (let [func-arity (max-arity fun)]
    (when-not (#{1 2} func-arity)
      (-> "invalid rpc function (arity not match)"
          (ex-info {:function-name fun-name :function fun})
          throw))
    (update local-functions fun-name
            (fn [prev-fun]
              (when (and prev-fun (not overwrite))
                (->> {:function-name fun-name :function fun}
                     (ex-info "function already exists")
                     throw))
              {:function fun :arity func-arity}))))

(defn add-function!
  "Register a supported function to current node.

  With `overwrite` being false, throw if the same name already exists."
  [{:as node} fun-name fun & {:as opts :keys [overwrite]}]
  (-> (node-functions node)
      (swap! add-function* fun-name fun opts)
      (get fun-name)))

(defn- request-hb!
  [{:as node :keys [node-id]}
   {:as req-meta :keys [req-id req-meta hb-lost-ratio hb-interval-ms]}
   request-atom-repo
   on-hb-lost]
  (swap!-swap-in-delayed!
   request-atom-repo
   {:ignores-nil? true :ks [req-id]}
   (fn [p]
     (let [{:keys [hb-lost-timer]} @p]
       (when-let [{:keys [cancel]} hb-lost-timer] (cancel))

       (let [hb-lost-at (+ (*now-ms*) (* hb-lost-ratio hb-interval-ms))]
         (log/debugf "[%s][%s] heartbeat will lost in (* %s %s)ms"
                     node-id req-id hb-lost-ratio hb-interval-ms)
         (-> @p
             (assoc :hb-lost-at hb-lost-at)
             (assoc :hb-lost-timer (*run-at* hb-lost-at on-hb-lost))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; function call

(defn call-function!
  [node fun-name params & {:as opts :keys [out-c in-c req-meta]}]
  (let [{:keys [function arity]}
        (-> (node-functions node) deref (get fun-name))]
    (when-not function
      (->> {:function-name fun-name :params params}
           (ex-info "function not found")
           throw))
    (if (= arity 2)
      (function params {:out-c (or out-c (chan (dropping-buffer 1)))
                        :in-c  (or in-c  (chan (dropping-buffer 1)))
                        :req-meta req-meta})
      (function params))))

(defn- req-cleanup!
  [{:as node :keys [node-id]} req-id]
  (let [[old _] (swap-vals! (node-futures node) #(dissoc % req-id))]
    (when-let [{:keys [in-c out-c in-c' out-c' hb-lost-timer timeout-timer]}
               (some-> (get old req-id) deref)]
      (close! in-c')
      (close! out-c')
      (close! out-c)
      (close! in-c)
      (when-let [{:keys [cancel]} hb-lost-timer] (cancel))
      (when-let [{:keys [cancel]} timeout-timer] (cancel)))))

(defn- req-cancel!
  "Cancel the call.

  Arguments:

  - `:reason`: {:keys [typ data]}, `typ` may be:
      - `:timeout`
      - `:xnfun/caller-cancel`
  "
  [node req-id reason]
  (when-let [{:keys [in-c' fut]}
             (some-> @(node-futures node) (get req-id) deref)]
    (>!! in-c' {:typ :xnfun/cancel :data reason})
    (future (Thread/sleep 100) (future-cancel fut))))

(defn- req-on-hb-lost!
  [node req-id hb-lost-at]
  (->> {:typ :timeout :data {:reason "hb-lost" :at hb-lost-at}}
       (req-cancel! node req-id)))

(defn- req-on-callee
  [{:as node :keys [node-id]} req-id out-c out-c' hb]
  (go-loop []
    (when-let [{:as d :keys [typ]} (<! out-c')]
      (try
        (log/debugf "[%s][%s][%s] callee message: %s" node-id req-id typ d)
        ;; all message counts as heartbeat message.
        (when hb (hb))

        ;; Forward message, and always blocking when forwarding.
        (if (#{:xnfun/hb :xnfun/to-caller} typ)
          (>!! out-c d)
          (log/warnf "unkown message type from callee: %s" typ))
        (catch Exception e
          (log/warnf e "[%s][%s] error handling msg from callee: %s"
                     node-id req-id d)))
      (recur))))

(defn- req-on-caller
  [{:as node :keys [node-id]} req-id in-c in-c']
  (go-loop []
    (when-let [{:as d :keys [typ data]} (<! in-c)]
      (try
        (cond
          (= typ :xnfun/cancel)
          (->> {:typ :xnfun/caller-cancel :data data}
               (req-cancel! node req-id))

          (= typ :xnfun/to-callee)
          (>!! in-c' d)

          :else
          (log/warnf "[%s] unkown message from caller: %s" node-id d))
        (catch Exception e
          (log/warnf e "[%s][%s] error handling msg from caller: %s"
                     node-id req-id d)))
      (recur))))

(defn- call-timeout-handler
  [node req-id timeout-ms]
  (->> #(->> {:typ :timeout :data {:reason "timeout" :at %}}
             (req-cancel! node req-id))
       (*run-at* (+ (*now-ms*) timeout-ms))))

(defn- submit-call'
  [{:as node :keys [node-id]}
   fun-name params {:as opts :keys [req-meta in-c out-c]}]
  (let [{:keys [req-id timeout-ms hb-interval-ms hb-lost-ratio]}
        req-meta

        out-c' (chan 1)
        in-c'  (chan 1)
        opt'   {:in-c in-c' :out-c out-c' :req-meta req-meta}

        hb
        (when (and hb-interval-ms hb-lost-ratio)
          #(request-hb! node req-meta (node-futures node)
                        (partial req-on-hb-lost! node req-id)))

        timeout
        (when timeout-ms
          #(call-timeout-handler node req-id timeout-ms))

        run
        #(future
           (try (call-function! node fun-name params opt')
                (finally
                  (future (Thread/sleep 1000)
                          (req-cleanup! node req-id)))))]

    (log/debugf "[%s][%s] waiting for callee message" node-id req-id)
    (req-on-callee node req-id out-c out-c' hb)

    (log/debugf "[%s][%s] waiting for caller message" node-id req-id)
    (req-on-caller node req-id in-c in-c')

    (cond-> {:req   {:req-meta req-meta
                     :fun-name fun-name
                     :params params}
             :in-c  in-c
             :out-c out-c
             :fut   (run)}

      ;; follwing are internal states.
      true    (assoc :submit-at (*now-ms*)
                     :in-c' in-c'
                     :out-c' out-c')
      timeout (assoc :timeout-timer (timeout))
      hb      (assoc :hb hb))))

(defn- call-data
  [d]
  (if-not *debug*
    (select-keys d [:req :in-c :out-c :fut])
    d))

(defn submit-call!
  [{:as node :keys [node-id]}
   fun-name params & {:as options :keys [req-meta out-c]}]
  (let [req-meta (defaults/make-req-meta req-meta)
        req-id   (:req-id req-meta)
        options  (assoc options
                        :req-meta req-meta
                        :in-c     (chan 1)
                        :out-c    (or out-c (chan (dropping-buffer 1))))]
    (log/debugf "[%s] submitted request %s" node-id req-id)
    (some-> (swap!-swap-in-delayed!
             (node-futures node)
             {:ignores-nil? false :ks [req-id]}
             #(if % @% (submit-call' node fun-name params options)))
            (get req-id) deref call-data)))

(defn get-call
  "Get the successfully submitted call."
  [node req-id]
  (some-> @(node-futures node) (get req-id) deref call-data))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; promises

(defn fullfill-promise!
  [{:as node :keys [node-id]} req-id {:as r :keys [status data]}]
  (let [[old _] (swap-vals! (node-promises node) (fn [m] (dissoc m req-id)))]
    (if-let [p (get old req-id)]
      (let [{:keys [hb-lost-timer timeout-timer res-promise]} @p]
        (log/debugf "[%s] fullfilled [%s]: %s" node-id req-id data)
        (deliver res-promise r)
        (when-let [{:keys [cancel]} hb-lost-timer] (cancel))
        (when-let [{:keys [cancel]} timeout-timer] (cancel)))
      (log/debugf "[%s] request not found [%s]: %s" node-id req-id r))))

(defn- promise-submit-timeout-handler
  [node req-id timeout-ms]
  (->> #(->> {:status :xnfun/err
              :data {:typ :timeout :reason "timeout" :timeout-at %}}
             (fullfill-promise! node req-id))
       (*run-at* (+ (*now-ms*) timeout-ms))))

(defn- promise-data
  [d]
  (if-not *debug*
    (select-keys d [:req :hb :res-promise])
    d))

(defn- promise-on-hb-lost
  [node req-id hb-lost-at]
  (->> {:status :xnfun/err
        :data {:typ :timeout :reason "hb-lost" :timeout-at hb-lost-at}}
       (fullfill-promise! node req-id)))

(defn submit-promise!
  [{:as node :keys [node-id]} {:as req :keys [req-meta]}]
  (let [req-meta (defaults/make-req-meta req-meta)

        {:keys [req-id timeout-ms hb-lost-ratio hb-interval-ms]}
        req-meta

        req  (assoc req :req-meta req-meta)

        timeout
        (when timeout-ms
          #(promise-submit-timeout-handler node req-id timeout-ms))

        hb
        (if (and hb-lost-ratio hb-interval-ms)
          (fn [] (request-hb!
                  node req-meta
                  (node-promises node)
                  (partial promise-on-hb-lost node req-id)))
          (fn []))

        create-promise
        #(cond-> {:req         req
                  :hb          hb
                  :res-promise (promise)}

           ;; following are internal states
           timeout (assoc :timeout-timer (timeout)))

        r (some-> (swap!-swap-in-delayed!
                   (node-promises node)
                   {:ignores-nil? false :ks [req-id]}
                   #(if % @% (create-promise)))
                  (get req-id) deref promise-data)]
    (hb)
    r))

(defn get-promise
  "Get the submitted promsie by [[submit-promise!]]."
  [node req-id]
  (some-> @(node-promises node) (get req-id) deref promise-data))
