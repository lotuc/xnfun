(ns lotuc.xnfun.core.node-impl-remote
  (:require
   [clojure.tools.logging :as log]
   [lotuc.xnfun.core.node-utils :refer [send-msg add-sub]]
   [lotuc.xnfun.core.transport-mqtt :refer [make-mqtt-transport]]
   [lotuc.xnfun.core.utils :refer [swap!-swap-in-delayed!
                                   *now-ms* *periodic-run*]]
   [lotuc.xnfun.protocols :as xnp]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; node transport

(defn stop-node-transport* [node-id transport]
  (when (and transport @transport (not (xnp/closed? @transport)))
    (log/debugf "[%s] closing previous transport" node-id)
    (xnp/close! @transport)))

(defn stop-node-transport!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (xnp/node-transport node)
   {:ignores-nil? true}
   (partial stop-node-transport* (:node-id node))))

(defn start-node-transport!
  [{:as node :keys [node-id]}]
  (let [transport-options (get-in node [:node-options :transport])
        transport-module  (:xnfun/module transport-options)]
    (if (= transport-module 'xnfun.mqtt)
      (swap!-swap-in-delayed!
       (xnp/node-transport node)
       {:ignores-nil? false}
       #(do
          (stop-node-transport* node-id %)
          (log/debugf "[%s] start transport" node-id)
          (make-mqtt-transport node-id transport-options)))
      (->> {:transport-options transport-options}
           (ex-info "unkown transport")
           throw))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; node heartbeat

(defn- node-hb-info
  [{:as node :keys [node-id node-options node-state]}]
  (let [local-state (-> node-state :local)
        functions (->> @(:functions local-state)
                       (map (fn [[k v]] [k (select-keys v [:arity])]))
                       (into {}))]
    {:node-id node-id
     :node-options (select-keys node-options [:hb-options])
     :functions functions}))

(defn- do-heartbeat*
  ([node] (do-heartbeat* node (*now-ms*)))
  ([node time-in-ms]
   (let [data (assoc (node-hb-info node) :trigger-at time-in-ms)]
     (send-msg node {:typ :hb :data data}))))

(defn- watch-heartbeat-state*
  "Watch node state and trigger heartbeat as state change."
  [node]
  (add-watch
   (xnp/node-functions node) :hb
   (fn [_k _r _old _new]
     (do-heartbeat* node))))

(defn- unwatch-hearbteat-state*
  [node]
  (remove-watch (xnp/node-functions node) :hb))

(defn- stop-heartbeat* [node-id hb-timer]
  (when-let [{:keys [cancel]} (and hb-timer @hb-timer)]
    (when cancel
      (log/debugf "[%s] stop previous heartbeat" node-id)
      (cancel)))
  nil)

(defn stop-heartbeat!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (xnp/node-hb-timer node)
   {:ignores-nil? true}
   (partial stop-heartbeat* node-id)))

(defn start-heartbeat!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (xnp/node-hb-timer node)
   {:ignores-nil? false}
   (fn [v]
     (stop-heartbeat* node-id v)

     (log/debugf "[%s] start heartbeat" node-id)
     (let [hb-timer
           (*periodic-run*
            (*now-ms*)
            (get-in node [:node-options :hb-options :hb-interval-ms])
            (partial do-heartbeat* node))

           cancel-hb-timer
           #(do (log/debugf "[%s] cancel heartbeat" node-id)
                ((:cancel hb-timer)))

           unwatch-hb-state
           #(do (log/debugf "[%s] unwatch heartbeat related state" node-id)
                (unwatch-hearbteat-state* node))]

       (log/debugf "[%s] start watch heartbeat related state" node-id)
       (watch-heartbeat-state* node)

       {:cancel #(do (unwatch-hb-state) (cancel-hb-timer))
        :hb-timer hb-timer
        :hb-state-watcher {:cancel unwatch-hb-state}}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; heartbeat listener

(defn- remove-dead-workers*
  "If the remote node itself does not report it's hb options, we'll use current
  node's hb options as the default."
  [remote-nodes now-ms {:keys [hb-interval-ms hb-lost-ratio]}]
  (->> remote-nodes
       (filter
        (fn [[_ {:keys [hb-at node-options]}]]
          (< (- now-ms hb-at)
             (* (get-in node-options [:hb-options :hb-interval-ms]
                        hb-interval-ms)
                (get-in node-options [:hb-options :hb-lost-ratio]
                        hb-lost-ratio)))))
       (into {})))

(defn- remove-dead-workers!
  "Try remove all node that losts their heartbeat."
  ([node]
   (remove-dead-workers! node (*now-ms*)))
  ([{:as node :keys [node-options]} now-ms]
   (let [nodes (xnp/node-remote-nodes node)
         hb-options (get-in node [:node-options :hb-options])]
     (send nodes remove-dead-workers* now-ms hb-options))))

(defn- on-heartbeat*
  "Update top-level non-nil fields, keep the top-level nil one what it previous
  is."
  [remote-nodes {:as msg :keys [node-id node-options functions]} now-ms]
  (->>
   (fn [{:keys [hb-at] old-functions :functions old-node-options :node-options}]
     {:hb-at      now-ms
      :node-options (if node-options node-options (or old-node-options {}))
      :functions  (if functions functions (or old-functions {}))})
   (update remote-nodes node-id)))

(defn- on-heartbeat!
  "Handle heartbeat message from `node-id`.

  Heartbeat message derived from [[node-hb-info]], the structure is the same.

  The node's hb message may contains the functions it now supports."
  ([node msg]
   (on-heartbeat! node msg (*now-ms*)))
  ([node hb-msg now-ms]
   (let [nodes (xnp/node-remote-nodes node)]
     (send nodes on-heartbeat* hb-msg now-ms))))

(defn stop-heartbeat-listener* [node-id hb-listener]
  (when-let [{:keys [cancel]} (and hb-listener @hb-listener)]
    (when cancel
      (log/debugf "[%s] stop heartbeat listener" node-id)
      (cancel))))

(defn start-heartbeat-listener!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (xnp/node-hb-listener node)
   {:ignores-nil? false}
   (fn [v]
     (stop-heartbeat-listener* node-id v)
     (log/infof "[%s] start heartbeat listener" node-id)

     (log/debugf "[%s] start remote node heartbeat checker" node-id)
     (let [hb-check-timer
           (*periodic-run*
            (*now-ms*)
            (let [{:keys [hb-interval-ms hb-lost-ratio]}
                  (get-in node [:node-options :hb-options])]
              (* hb-interval-ms hb-lost-ratio))
            (partial remove-dead-workers! node))

           cancel-hb-check
           (fn []
             (log/debugf "[%s] stop remote node heartbeat checker" node-id)
             ((:cancel hb-check-timer)))

           unsub-hb-msg
           (do (log/debugf "[%s] start subscribe to remote node heartbeat"
                           node-id)
               (->> (fn [{:keys [data]}] (on-heartbeat! node data))
                    (add-sub node :hb)))

           unsub-hb-msg
           (fn []
             (log/debugf "[%s] unsub remote node heartbeat" node-id)
             (unsub-hb-msg))]
       {:cancel #(do (unsub-hb-msg) (cancel-hb-check))
        :hb-check-timer hb-check-timer
        :unsub-hb-msg unsub-hb-msg}))))

(defn stop-heartbeat-listener!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (xnp/node-hb-listener node)
   {:ignores-nil? true}
   (partial stop-heartbeat-listener* node-id)))
