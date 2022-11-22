(ns lotuc.remote.mqtt.rpc-internal
  (:require [chime.core :as chime]
            [clojure.tools.logging :as log]
            [lotuc.remote.mqtt.mqtt :as mqtt]
            [lotuc.remote.node.registry :as r]
            [lotuc.remote.mqtt.topics :as topics])
  (:import (java.time Duration Instant)))

(defn- add-cleanup
  "atom of func set."
  [cleanup-funcs func]
  (swap! cleanup-funcs #(conj % func)))

(defn- cleanup
  [cleanup-funcs]
  (swap! cleanup-funcs (fn [funcs] (r/run-funcs-ignore-error funcs) #{})))

(defn- registry-start-listen
  "node-state & node-id"
  [{:keys [nodes cleanup-funcs]} node-id]

  ;; listen for node registration
  (log/info "*>" node-id "start listen for node registration")
  (let [topic-filter (topics/registry-topic-create {})]
    (mqtt/sub-edn
     topic-filter
     (fn [t data]
       (log/debugf "[%s] on registry %s" node-id t)
       (let [{:keys [action node-id]} (topics/registry-topic-parse t)]
         (case action
           :heartbeat (swap! nodes #(r/on-heartbeat % node-id data))

           (log/warn "unkown action" t)))))
    (add-cleanup cleanup-funcs #(mqtt/unsub topic-filter)))

  ;; check heartbeat
  (log/info "*>" node-id "start periodically checking registered node")
  (let [handlers {:error-handler
                  (fn [e] (log/warn e "*>" node-id "error run check heartbeat"))}
        interval (Duration/ofMillis r/*heartbeat-interval-in-millis*)
        check-hb (fn [_] (swap! nodes r/remove-dead-nodes))
        v (-> (chime/periodic-seq (Instant/now) interval)
              (chime/chime-at check-hb handlers))]
    (add-cleanup cleanup-funcs #(.close v))))

(defn- registry-rpc-on-call [{:keys [methods running-futures]}
                             req-id [method params]
                             {:keys [with-error-stacktrace]}
                             pub-res]
  (swap!
   running-futures
   (fn [m]
     (if (get m req-id)
       (do ; (pub-res {:type :final :status :err-rpc :msg :duplicate-req-id})
         (log/warn "duplicate request id" req-id method params)
         m)
       (let [f (future (r/call-method
                        @methods method params
                        {:send #(pub-res {:type :partial :data %})}))]
         (future
           (-> (try (let [r @f]
                      {:type :final :status :ok :data r})
                    (catch Exception e
                      (cond-> {:type :final :status :err :msg (str e)}
                        with-error-stacktrace
                        (assoc :stacktrace (map str (.getStackTrace (Exception.))))))
                    (finally
                      ;; cleanup on finish
                      (swap! running-futures #(dissoc % req-id))))
               pub-res))
         (assoc m req-id f))))))

(defn- registry-rpc-on-cancel [{:keys [running-futures]} req-id]
  (swap!
   running-futures
   (fn [m]
     (when-let [f (get m req-id)]
       (future-cancel f)))))

(defn- registry-listen-for-rpc [node-state node-id]
  (let [sub-t {:action :req :node-id node-id}
        sub-topic (topics/rpc-topic-create sub-t)]
    (mqtt/sub-edn
     sub-topic
     (fn [topic [req req-meta]]
       ;; req: [method params]
       ;; req-meta: {:keys [action, with-error-stacktrace]}
       (let [{:keys [req-id]} (topics/rpc-topic-parse topic)
             action (:action req-meta)
             pub-t {:action :resp :node-id node-id :req-id req-id}

             pub-topic (topics/rpc-topic-create pub-t)
             pub-res (partial mqtt/pub-edn pub-topic)]
         (log/info ">" node-id "recv request" req-id action)
         (case action
           :call
           (registry-rpc-on-call node-state req-id req req-meta pub-res)

           :cancel
           (registry-rpc-on-cancel node-state req-id)

           (log/warn "unkown action" req-id action)))))))

(defn- registry-listen-for-rpc-resp-on-partial [resp]
  (log/info "recv partial response" (:data resp)))

(defn- registry-listen-for-rpc-resp-on-final [resp res-promise cancel-timeout]
  (deliver res-promise (dissoc resp :type))
  (when-let [c @cancel-timeout] (.close c)))

(defn- registry-listen-for-rpc-resp-on-call-timeout
  [res-promise req-id {:keys [timeout-in-sec req-topic resp-topic]}]

  (log/warn "request" req-id "timeout")
  (deliver res-promise {:status :err-rpc
                        :msg "call timeout" :timeout-sec timeout-in-sec})
  (mqtt/unsub resp-topic)
  (mqtt/pub-edn req-topic [nil {:action :cancel}]))

(defn- registry-listen-for-rpc-resp
  "res: the response promise"
  [req-id res-promise
   {:keys [timeout-in-sec resp-topic] :as req-meta}]
  (let [handlers {:error-handler
                  (fn [e] (log/warn e "*>" req-id "error handling rpc response"))}
        now (Instant/now)
        expires-at (.plusSeconds now (or timeout-in-sec 60))
        cancel-timeout (atom nil)]
    (log/info "wait for request" req-id "response")
    (mqtt/sub-edn
     resp-topic
     ;; check for pub-res
     (fn [topic {:keys [type] :as r}]
       (let [{:keys [req-id]} (topics/rpc-topic-parse topic)]
         (log/info "recv response for" req-id type)
         (case type
           :partial
           (registry-listen-for-rpc-resp-on-partial r)

           :final
           (registry-listen-for-rpc-resp-on-final
            r res-promise cancel-timeout)))))
    (reset! cancel-timeout
            (chime/chime-at
             [expires-at]
             (fn [_]
               (registry-listen-for-rpc-resp-on-call-timeout
                res-promise req-id req-meta))
             handlers))))

(defn- registry-start-worker-heartbeat
  "methods is the ref object that holds the node's supported methods"
  [{:keys [cleanup-funcs methods] :as node-state} node-id]

  ;; heartbeat
  (log/info ">" node-id "start heartbeating")
  (let [hb-topic (topics/registry-topic-create {:action :heartbeat :node-id node-id})

        hb-interval (Duration/ofMillis r/*heartbeat-interval-in-millis*)
        hb (fn [_] (mqtt/pub-edn hb-topic {:methods (keys @methods)}))
        method-watch-key :heartbeat-on-methods-change

        closable
        (-> (chime/periodic-seq (Instant/now) hb-interval)
            (chime/chime-at hb))]

    (add-cleanup cleanup-funcs #(.close closable))

    ;; watcher for method registration
    ;; triggers heartbeat instally
    (add-watch methods method-watch-key
               (fn [_ _ old new]
                 (when (not= old new) (hb (r/*current-time-millis*)))))
    (add-cleanup cleanup-funcs #(remove-watch methods method-watch-key)))

  ;; rpc listener
  (log/info ">" node-id "start listen for rpc request")
  (registry-listen-for-rpc node-state node-id))

(defn registry-start
  "start node"
  [{:keys [id roles state]}]
  (let [roles                   (or roles #{:caller :worker})
        {:keys [cleanup-funcs]} state]
    (cleanup cleanup-funcs)
    (try
      (when (:caller roles) (registry-start-listen state id))
      (when (:worker roles) (registry-start-worker-heartbeat state id))
      (catch Exception e
        (log/warn e "error starting")
        (cleanup cleanup-funcs)))))

(defn registry-stop
  "stop node"
  [node]
  (-> node :state :cleanup-funcs cleanup))

(defn registry-add-worker-method
  [node method func]
  (let [mths (-> node :state :methods)]
    (swap! mths (fn [ms] (r/add-method ms method func)))))

(defn- registry-rpc-call-node [node-id method params req-meta]
  (let [req-id (str (random-uuid))
        req-topic (topics/rpc-topic-create {:action :req :node-id node-id :req-id req-id})
        resp-topic (topics/rpc-topic-create {:action :resp :node-id node-id :req-id req-id})
        call-meta (-> (or req-meta {}) (assoc :action :call))
        req-meta (-> (or req-meta {})
                     (assoc :req-id req-id)
                     (assoc :req-topic req-topic)
                     (assoc :resp-topic resp-topic))
        res-promise (promise)]
    (registry-listen-for-rpc-resp req-id res-promise req-meta)
    (mqtt/pub-edn req-topic [[method params] call-meta])
    (future
      (let [{:keys [status] :as r} @res-promise]
        (case status
          :ok {:res (:data r)
               :meta {:node-id node-id
                      :request {:method method
                                :params :params
                                :meta req-meta}}}
          :err (throw (ex-info "error running" r))
          :err-rpc (throw (ex-info "error calling" r))
          (throw (ex-info "unkown result" r)))))))

(defn registry-rpc-call
  [node method params & {:keys [timeout-in-sec node-id with-resp-meta]}]
  (let [nodes (-> node :state :nodes)

        selected-node-id
        (r/find-node @nodes {:node-id node-id :method method})]
    (when-not selected-node-id
      (throw (ex-info "node not found" {:method method :node-id node-id})))
    (let [r (registry-rpc-call-node selected-node-id method params
                                    {:timeout-in-sec timeout-in-sec})]
      (if with-resp-meta r (future (:res @r))))))

(defn registry-make-state []
  {:cleanup-funcs (atom #{})

   ;; as caller, the registed nodes ;;
   :nodes (atom {:heartbeat {} :methods {}})
   :waiting-promises (atom {})

   ;; as worker ;;
   ;; supported methods
   :methods (atom {})
   ;; running processes
   :running-futures (atom {})})

(defn registry-make-node [& {:keys [node-id roles node-state]}]
  {:id (or node-id (str (random-uuid)))
   :roles (if roles (set roles) #{:caller :worker})
   :state (or node-state (registry-make-state))})

#_(require '[lotuc.remote.mqtt.rpc-internal-funcs
             :refer [func-delayed-sum func-delayed-add]])
#_(let [workers 100
        round 100

        caller-node (registry-make-node
                     {:node-id "node-caller" :roles #{:caller}})

        worker-nodes (map #(registry-make-node
                            {:node-id (str "node-w-" %) :roles [:worker]})
                          (range workers))

        nodes (conj worker-nodes caller-node)]

    (try
      (doseq [n nodes] (registry-start n))

      (doseq [n worker-nodes]
        (registry-add-worker-method n "func-sum" #'func-delayed-sum)
        (registry-add-worker-method n "func-add" #'func-delayed-add))

      (Thread/sleep (* 2 r/*heartbeat-interval-in-millis*))

      (let [r
            (->> (range round)
                 (map #(registry-rpc-call caller-node "func-add" [1 %]))
                 (map deref)
                 time)

            sum-of-all
            (-> (registry-rpc-call caller-node "func-sum" r)
                deref
                time)]
        (println "!! result of remote call" r)
        (println "!! sum of all" sum-of-all))

      (catch Exception e (log/error e (str e)))
      (finally
        (doseq [n nodes] (registry-stop n)))))
