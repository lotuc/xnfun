(ns lotuc.xnfun.rpc.impl
  (:require
   [chime.core :as chime]
   [clojure.core.async :refer [<! chan close! dropping-buffer go-loop put!]]
   [clojure.tools.logging :as log]
   [lotuc.xnfun.mqtt.client :as mqtt]
   [lotuc.xnfun.mqtt.pubsub :refer [pub-edn sub-edn unsub]]
   [lotuc.xnfun.rpc.mqtt-topics :as topics]
   [lotuc.xnfun.rpc.node :as n])
  (:import
   (java.time Duration Instant)))

(def ^:private cleanup-func-count (atom 0))

(defn- add-cleanup
  [{:as node :keys [node-id]
    {:keys [cleanup]} :node-state}
   {:as cleaner :keys [func name]}]
  (let [name (str node-id "--" (or name (swap! cleanup-func-count inc)))]
    (swap! cleanup #(conj % (assoc cleaner :name name)))))

(defn- run-cleanups [cleanup]
  (doseq [{:keys [func name handle-exception]} cleanup]
    (log/infof "cleanup %s" name)
    (try (func) (catch Exception e
                  (if handle-exception (handle-exception e) (throw e))))))

(defn- node-sub-edn [{:as node
                      {:keys [mqtt-client]} :node-state}
                     topic-filter listener
                     & {:as options :keys [add-to-node-cleanup]}]
  (cond->> {:func (sub-edn mqtt-client topic-filter listener options)
            :name (str "unsub-" topic-filter)
            :handle-exception (constantly nil)}
    (or (nil? add-to-node-cleanup) add-to-node-cleanup)
    (add-cleanup node)))

(defn- node-pub-edn [{:as node {:keys [mqtt-client]} :node-state} topic data]
  (pub-edn mqtt-client topic data))

(defn- node-unsub [{:as node {:keys [mqtt-client]} :node-state} topic-filter]
  (unsub mqtt-client topic-filter))

(defn- on-registry-data [node topic {:keys [functions] :as data}]
  (log/debugf "on registry: %s: %s" topic data)
  (let [{:keys [action node-id]} (topics/registry-topic-parse topic)]
    (case action
      :heartbeat (n/on-heartbeat! node (assoc data :node-id node-id))
      (log/warnf "[%s] unkown action" topic))))

(defn- on-call
  "Handle rpc call

  Arguments
  - `req-meta`:
    - `:with-stacktrace`: should err response include the stacktrace
    Check `submit-function-call` for following options.
    - `:timeout-ms`
    - `:hb-interval-ms`
    - `:hb-lost-ratio`
  "
  [node req-id [fun-name params] {:as req-meta :keys [with-stacktrace]} pub-res]

  (let [out-c (chan 1)

        on-res
        #(-> {:type :xnfun/resp :status :ok :data %} pub-res)

        on-exception
        #(let [d (cond-> {:type :xnfun/resp :status :err :msg (str %)}
                   with-stacktrace
                   (assoc :stacktrace (map str (.getStackTrace %))))]
           (pub-res d))

        callbacks {:on-res on-res :on-exception on-exception}]
    (go-loop [{:as d} (<! out-c)]
      (when d
        (pub-res d)
        (recur (<! out-c))))
    (let [r (n/submit-function-call
             node
             fun-name params
             {:out-c out-c
              :req-meta  (-> req-meta (assoc :req-id req-id))
              :callbacks callbacks})]
      (future (try @r (finally (close! out-c))))
      r)))

(defn- on-rpc-msg [{:keys [node-id] :as node {:keys [running-requests]} :node-state}
                   req-id
                   {:keys [type] :as msg}]
  (if-let [in-c (get-in @running-requests [req-id :in-c])]
    (put! in-c msg)
    (log/debugf "[%s][%s] on rpc message" node-id req-id)))

(defn- on-rpc [{:as node :keys [node-id]} topic [data req-meta]]
  (let [{:keys [req-id]} (topics/rpc-topic-parse topic)
        action (:action req-meta)
        pub-t  {:action :resp :node-id node-id :req-id req-id}

        pub-topic (topics/rpc-topic-create pub-t)
        pub-res (partial node-pub-edn node pub-topic)]
    (log/debugf "[%s][%s] recv request %s" node-id req-id action)
    (if (= action :call)
      (on-call node req-id data req-meta pub-res)
      (on-rpc-msg node req-id {:type action :data data}))))

(defn- listen-for-rpc
  [{:as node :keys [node-id]
    {:keys [mqtt-client]} :node-state}]
  (log/infof "[%s] start listen for rpc request" node-id)
  (let [topic (-> {:action :req :node-id node-id}
                  topics/rpc-topic-create)]
    (node-sub-edn node topic #(on-rpc node %1 %2))))

(defn- listen-for-registration
  [{:as node :keys [node-id]
    {:keys [hb-interval-ms]} :node-options
    {:keys [mqtt-client]} :node-state}]
  ;; listen for node registration
  (let [topic (-> {} topics/registry-topic-create)]
    (log/infof "[%s] start listen for node registration [%s]" node-id topic)
    (node-sub-edn node topic #(on-registry-data node %1 %2)))

  ;; check heartbeat
  (log/infof "[%s] start periodically checking registered node" node-id)
  (let [handlers {:error-handler #(log/warn % "[%s] error checking heattbeat" node-id)}
        interval (Duration/ofMillis hb-interval-ms)
        check-hb (fn [_] (n/remove-dead-workers! node))
        v        (-> (chime/periodic-seq (Instant/now) interval)
                     (chime/chime-at check-hb handlers))]
    (add-cleanup node {:func #(.close v) :name "hb-check-task"})))

(defn- start-worker-heartbeat
  [{:as node
    :keys [node-id]
    {:keys [hb-interval-ms]} :node-options
    {:keys [functions]} :node-state}]
    ;; heartbeat
  (log/infof "[%s] start worker heartbeat" node-id)
  (let [hb-topic (topics/registry-topic-create {:action :heartbeat :node-id node-id})

        hb-interval (Duration/ofMillis hb-interval-ms)
        hb          (fn [_] (node-pub-edn node hb-topic (n/calc-heartbeat-msg node)))
        watch-key   :heartbeat-on-functions-change

        closable
        (-> (chime/periodic-seq (Instant/now) hb-interval)
            (chime/chime-at hb))]

    (add-cleanup node {:func #(.close closable) :name (str "hb-" node-id)})

    ;; watcher for function registration
    ;; triggers heartbeat instally
    (add-watch functions watch-key
               (fn [_key _ref old-state new-state]
                 (when (not= old-state new-state)
                   (hb (n/*now-ms*)))))

    (->> {:func #(remove-watch functions watch-key)
          :name (str "wathcer-heartbeat-on-functions-change")}
         (add-cleanup node))))

(defn- register-cleanups
  [{:as node
    :keys [node-id]
    {:keys [running-requests mqtt-client worker-nodes]} :node-state}]
  (->> {:func (fn [] (swap! running-requests
                            #(do (doseq [[_ {:keys [running-future]}] %]
                                   (future-cancel running-future))
                                 %)))
        :name "cleanup-running-requests"}
       (add-cleanup node))
  (->> {:func (fn [] (swap! worker-nodes (fn [_] {})))
        :name "clear-worker-nodes"}
       (add-cleanup node)))

(def nodes (atom {}))

(defn- cleanup-node [{:as node
                      :keys [node-id]
                      {:keys [cleanup mqtt-client]} :node-state}]
  (swap! cleanup #(do (run-cleanups %) #{}))

  (when (and mqtt-client (mqtt/connected? mqtt-client))
    (mqtt/disconnect mqtt-client)
    (log/warnf "[%s] already disconnected from broker" node-id)))

(defn start-node
  [{:as n :keys [node-id]}]
  (let [{:as n1 :keys [node-id]} (n/make-node n)]
    (-> (swap!
         nodes
         #(let [n2 (get % node-id)]
            (when n2 (throw (ex-info "node already exists" {:node-id node-id})))
            (try
              (let [mqtt-spec   (or (-> n1 :node-options :mqtt-spec)
                                    {:broker "tcp://127.0.0.1:1883"
                                     :client-id node-id
                                     :connect-options {:max-in-flight 1000
                                                       :automatic-reconnect true}})
                    mqtt-client (mqtt/make-client mqtt-spec)
                    node        (assoc-in n1 [:node-state :mqtt-client] mqtt-client)]
                (doto node
                  start-worker-heartbeat
                  listen-for-registration
                  listen-for-rpc
                  register-cleanups)
                (assoc % node-id node))
              (catch Exception e
                (swap! (-> n1 :node-state :cleanup) (fn [v] (run-cleanups v) #{}))
                (throw e)))))
        (get node-id))))

(defn stop-node [{:as node :keys [node-id]}]
  (swap! nodes #(do (cleanup-node node) (dissoc % node-id))) node)

(defn- call-function-to-node
  "Call function to given node.
  Argument list:
  - `req-meta`:
    Check `on-call` for following options.
    - `:with-stacktrace`
    - `:timeout-ms`
    - `:hb-interval-ms`
    - `:hb-lost-ratio`

  Returns: {}
  - `in-c`: caller can send message to callee through this channel
  - `out-c`: caller can recv message from callee through this channel
  - `res-future`: response
  "
  [{:as node :keys [node-id] {:keys [mqtt-client]} :node-state}
   callee-node-id fun-name params
   {:as options :keys [out-c req-meta]}]
  (let [in-c (chan 1) out-c (or out-c (chan (dropping-buffer 1)))
        req-id     (str (random-uuid))
        req-topic  (-> {:action :req  :node-id callee-node-id :req-id req-id}
                       topics/rpc-topic-create)
        resp-topic (-> {:action :resp :node-id callee-node-id :req-id req-id}
                       topics/rpc-topic-create)
        call-meta  (-> (or req-meta {})
                       (assoc :req-id req-id)
                       (assoc :caller-node-id node-id)
                       (assoc :callee-node-id callee-node-id)
                       (assoc :action :call))
        req-meta   (-> (dissoc call-meta :action)
                       (assoc :req-topic req-topic)
                       (assoc :resp-topic resp-topic))

        req  {:req-id req-id
              :function-name fun-name
              :params params
              :req-meta req-meta}

        ;; submit promise waiting for result
        {:keys [res-promise hb]}
        (n/submit-wait-for-function-call node req)

        handle-caller-msg
        (fn [{:as d :keys [type data]}]
          (try
            (node-pub-edn node req-topic [(:data d) {:action type}])
            (catch Exception e
              (log/warnf e "[%s] error handling: %s" req-id d))))

        handle-callee-msg
        (fn [req-id {:as data :keys [type]}]
          (if (= type :xnfun/resp)
            (n/fullfill-wait-function-call node req-id (dissoc data :type))
            (do (future (hb))
                (put! out-c data))))]

    ;; handle message from callee
    (node-sub-edn
     node
     resp-topic
     (fn [topic data]
       (let [{:keys [req-id]} (topics/rpc-topic-parse topic)]
         (handle-callee-msg req-id data)))
     :add-to-node-cleanup false)

    ;; handle message from caller
    (go-loop []
      (let [d (<! in-c)]
        (when (some? d)
          (try
            (handle-caller-msg d)
            (catch Exception e
              (log/warnf e "[%s] error handle caller msg: %s" req-id d)))
          (recur))))

    ;; send the request
    (node-pub-edn node req-topic [[fun-name params] call-meta])

    {:in-c in-c :out-c out-c
     :res-future
     (future
       (let [{:keys [status] :as r} @res-promise]
         (log/debugf "[%s][%s] recv response from [%s]: %s"
                     node-id req-id callee-node-id r)
         (close! in-c)
         (close! out-c)
         (node-unsub node resp-topic)
         (case status
           :ok      {:res (:data r) :request req}
           :err     (throw (ex-info "error running" r))
           :err-rpc (do
                      (node-pub-edn node req-topic [nil {:action :xnfun/cancel}])
                      (throw (ex-info "error calling" r)))
           (throw (ex-info "unkown result" r)))))}))

(defn call-function
  "
  Arguments:
  - `options.out-c`: If not supplied, will default to be a dropping buffer
    channel. Recv callee's message from it.

  Returns {:keys [in-c out-c res-future]}
  "
  [node fun-name params
   & {:as options
      :keys [req-meta out-c]
      {:keys [node-id]} :req-meta}]
  (let [nid (->> {:node-id node-id :function-name fun-name}
                 (n/select-worker-node node))]
    (when-not nid
      (->> {:function-name fun-name :node-id node-id}
           (ex-info "node not found")
           throw))
    (call-function-to-node node nid fun-name params options)))

(comment
  (def n0 (start-node {:node-id "n0"
                       :node-options {:hb-interval-ms 3000
                                      :hb-lost-ratio 2}}))

  ;;
  )
