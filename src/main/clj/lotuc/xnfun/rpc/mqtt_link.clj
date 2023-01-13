(ns lotuc.xnfun.rpc.mqtt-link
  "MQTT implementation for [[XNFunLink]].

  We consider encode our data into MQTT's topic and payload.

  - [[create-send-data]]: Convert our data to topic and payload to be sent.
  - [[create-sub-data]]:: Convert our subscription to topic filter and handler
    function."
  (:require [lotuc.xnfun.rpc.link :refer [XNFunLink]]
            [lotuc.xnfun.mqtt.client :as mqtt]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

(def ^:dynamic *ns-prefix* "")
(def ^:private ver "v0")

(defn- remove-subscription
  [topic-filter-handlers client subscriptions]
  (let [[old new]
        (swap-vals!
         subscriptions
         (fn [m0]
           (reduce
            (fn [m [topic-filter handler]]
              (if-let [{:keys [handlers sub-fn]} (m topic-filter)]
                (let [handlers (disj handlers handler)]
                  (if (empty? handlers)
                    (dissoc m topic-filter)
                    (assoc m topic-filter {:handlers handlers :sub-fn sub-fn})))
                m))

            m0
            topic-filter-handlers)))]
    (doseq [[topic-filter _] topic-filter-handlers]
      (when (and (contains? old topic-filter) (not (contains? new topic-filter)))
        (let [{:keys [sub-fn]} (old topic-filter)]
          (when (realized? sub-fn)
            (mqtt/unsubscribe client topic-filter)))))))

(defn- add-subscription
  [topic-filter-handlers client subscriptions]
  (let [ms
        (swap!
         subscriptions
         (fn [m0]
           (reduce
            (fn [m [topic-filter handler]]
              (->> (if-let [{:keys [handlers sub-fn]} (m topic-filter)]
                     {:handlers (conj handlers handler)
                      :sub-fn sub-fn}
                     {:handlers #{handler}
                      :sub-fn (delay
                                (fn [topic raw-data]
                                  (let [data (nippy/thaw (:payload raw-data))
                                        fs (->> [topic-filter :handlers]
                                                (get-in @subscriptions)
                                                (map #(future (% topic data))))]
                                    (doseq [f fs]
                                      (try
                                        @f
                                        (catch Exception e
                                          (log/infof e "error handling %s" topic)))))))})
                   (assoc m topic-filter)))
            m0
            topic-filter-handlers)))]
    (doseq [[topic-filter {:keys [sub-fn]}]
            (->> topic-filter-handlers (map (fn [[t _]] [t (ms t)])))]
      (when-not (realized? sub-fn)
        (mqtt/subscribe client topic-filter @sub-fn)))))

(defmulti create-send-data
  "Convert the message to MQTT topic & payload.

  Arguments: [node-id, {:as msg :keys [typ data}]

  Returns: [topic, payload]"
  (fn [_node-id {:as msg :keys [typ]}] typ))

(defmulti create-sub-data
  "Create a topic filter and a message adapter for subscription.

  The message adapter convert the MQTT topic & payload to the sent message for
  `handler-fn`'s usage.

  Arguments: [node-id {:as subscription :keys [handle-fn]} typ]
  - `node`: current node

  Returns: [topic-filter, message-adapter]
  "
  (fn [_node-id _subscription typ] typ))

;; Heartbeat
;; *ns-prefix*/<ver>/registry/hb/<node-id>
(defn- topic:hb:parse [topic]
  (let [[node-id] (-> (re-pattern (str *ns-prefix* "/" ver "/registry/hb/([^/]+)"))
                      (re-matches topic))]
    {:node-id node-id}))

(defmethod create-send-data :hb
  [node-id {:as msg :keys [data]}]
  [(str *ns-prefix* "/" ver "/registry/hb/" node-id)
   data])

(defmethod create-sub-data :hb
  [_node-id {:as handler} _]
  [(str *ns-prefix* "/" ver "/registry/hb/+")
   (fn [topic data]
     (let [m (topic:hb:parse topic)]
       (with-meta
         {:typ :hb :data data}
         m)))])

;; Request
;; *ns-prefix*/<ver>/rpc/req/<callee-node-id>/<caller-node-id>/<request-id>
(defn- topic:req:parse [topic]
  (let [[a b c] (-> (re-pattern (str *ns-prefix* "/" ver "/rpc/req/([^/]+)/([^/]+)/([^/]+)"))
                    (re-matches topic))]
    {:caller-node-id b :callee-node-id a :req-id c}))

(defmethod create-send-data :req
  [node-id {:as msg :keys [data]}]
  [(let [msg-meta (meta msg)
         {:keys [callee-node-id req-id]} msg-meta]
     (when (or (not callee-node-id) (not req-id))
       (->> {:msg msg :msg-meta msg-meta}
            (ex-info ":callee-node-id not found")
            throw))
     (str *ns-prefix* "/" ver "/rpc/req/" callee-node-id "/" node-id "/" req-id))
   data])

(defmethod create-sub-data :req
  [node-id {:as subscription} _]
  [(let [{:keys [caller-id req-id]} (meta subscription)]
     (str *ns-prefix* "/" ver "/rpc/req/"
          node-id "/" (or caller-id "+") "/" (or req-id "+")))
   (fn [topic data]
     (let [m (topic:req:parse topic)]
       (with-meta {:typ :req :data data} m)))])

;; Response topic
;; *ns-prefix*/<ver>/rpc/resp/<caller-node-id>/<request-id>/<callee-node-id>
(defn- topic:resp:parse [topic]
  (let [[a b c] (-> (re-pattern (str *ns-prefix* "/" ver "/rpc/resp/([^/]+)/([^/]+)/([^/]+)"))
                    (re-matches topic))]
    {:caller-node-id a :callee-node-id c :req-id b}))

(defmethod create-send-data :resp
  [node-id {:as msg :keys [data]}]
  [(let [msg-meta                        (meta msg)
         {:keys [req-id caller-node-id]} msg-meta]
     (when (or (not req-id) (not caller-node-id))
       (->> {:msg msg :msg-meta msg-meta}
            (ex-info ":req-id not found")
            throw))
     (str *ns-prefix* "/" ver "/rpc/req/" caller-node-id "/" req-id "/" node-id))
   data])

(defmethod create-sub-data :resp
  [node-id {:as subscription} _]
  [(let [{:keys [req-id callee-node-id]} (meta subscription)]
     (when-not req-id
       (->> {:subscription subscription}
            (ex-info "error register subscription handler: no req-id specified")
            throw))
     (if callee-node-id
       (str *ns-prefix* "/" ver "/rpc/resp/" node-id "/" req-id "/" callee-node-id)
       (str *ns-prefix* "/" ver "/rpc/resp/" node-id "/" req-id "/+")))
   (fn [topic data]
     (let [m (topic:resp:parse topic)]
       (with-meta {:typ :resp :data data} m)))])

(defn create-mqtt-xnfun-link
  [{:keys [client closed create-send-data* create-sub-data* subscriptions]}]
  (reify XNFunLink
    (send-msg [_ {:as msg :keys [data]}]
      (let [[topic data] (create-send-data* msg)]
        (mqtt/publish client topic (nippy/freeze data))))

    (sub-msg [_ {:as subscription :keys [types handle-fn]}]
      (let [topic-filter-handlers
            (->> (set types)
                 (map (fn [typ]
                        (let [[topic-filter message-adapter]
                              (create-sub-data* subscription typ)]
                          [topic-filter
                           (fn [topic data]
                             (handle-fn (message-adapter topic data)))]))))

            do-unsub
            #(remove-subscription topic-filter-handlers client subscriptions)]

        (try
          (add-subscription topic-filter-handlers client subscriptions)
          do-unsub
          (catch Exception e
            (do-unsub)
            (throw e)))))
    (close! [_]
      (try (mqtt/disconnect client)
           (finally (reset! closed true))))))

(defn new-mqtt-link* [node-id {:as mqtt-link :keys [topic-prefix mqtt-config]}]
  (let [mqtt-topic-prefix (str/replace (or topic-prefix "") #"/+$" "")

        state
        {:client (mqtt/make-client mqtt-config)
         ;; topic to handler functions
         :subscriptions (atom {})
         ;; if the link is closed
         :closed (atom {})}

        options
        (-> state
            (assoc :create-send-data*
                   #(binding [*ns-prefix* mqtt-topic-prefix]
                      (create-send-data node-id %)))
            (assoc :create-sub-data*
                   #(binding [*ns-prefix* mqtt-topic-prefix]
                      (create-sub-data node-id %1 %2))))]
    {:link (create-mqtt-xnfun-link options)
     :state state}))

(defn new-mqtt-link [node-id {:as mqtt-link :keys [topic-prefix mqtt-config]}]
  (:link (new-mqtt-link* node-id mqtt-link)))

(comment

  (def l (new-mqtt-link*
          "node-0"
          {:mqtt-config
           {:broker "tcp://127.0.0.1:1883"
            :client-id "node-0"
            :connect-options {:automatic-reconnect true}}}))

  (def u
    (.sub-msg (:link l)
              {:types [:hb]
               :handle-fn (fn [msg] (println "recv:" msg))}))

  (.send-msg (:link l) {:typ :hb :data "hello world!"})

  (u)

  (.close! (:link l))

                                        ;
  )
