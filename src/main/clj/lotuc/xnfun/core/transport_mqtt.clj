(ns lotuc.xnfun.core.transport-mqtt
  "MQTT implementation for [[lotuc.xnfun.core.transport/XNFunTransport]].

  We encode our transferring data into MQTT's topic and payload.

  - [[create-send-data]]: Convert our data to topic and payload to be sent.
  - [[create-sub-data]]:: Convert our subscription to topic filter and a message
    adapter. The message adapter adapts the received payload & topic to the
    message sent and pass it to the handler function.

  Topics:

  - Node heartbeat: `<prefix>/<ver>/registry/hb/<node-id>`
  - Request: `<prefix>/<ver>/rpc/req/<callee-node-id>/<caller-node-id>/<request-id>`
  - Response: `<prefix>/<ver>/rpc/resp/<caller-node-id>/<request-id>/<callee-node-id>`

  The `<ver>` is now constantly `v0`. You can specify `<prefix>` when creating
  the transport with `make-mqtt-transport`.
  "
  (:require [lotuc.xnfun.core.transport :refer [XNFunTransport]]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojurewerkz.machine-head.client :as mh]))

(def ^:dynamic ^:no-doc *ns-prefix* "")
(def ^:private ver "v0")

(def ^{:dynamic true
       :no-doc true
       :doc "A function accepts clojure object and serialize it to byte array.
  Defaults to be [taoensso.nippy/freeze](https://github.com/ptaoussanis/nippy)"}
  *to-mqtt-payload* nippy/freeze)

(def ^{:dynamic true
       :no-doc true
       :doc "A function accepts byte array and deserialize it to clojure object.
  Defaults to be [taoensso.nippy/thaw](https://github.com/ptaoussanis/nippy)"}
  *from-mqtt-payload* nippy/thaw)

(defn- handle-mqtt-message
  "Handle MQTT message with given handlers.

  The `get-handler` returns what we registered for a `topic-filter`."
  [get-handlers topic _meta payload]
  (let [data (*from-mqtt-payload* payload)
        futures
        (->> (get-handlers)
             (map (fn [[[_typ handle-fn] {:keys [message-adapter]}]]
                    (future
                      (try
                        (let [msg (message-adapter topic data)]
                          (handle-fn msg))
                        (catch Exception e
                          (log/infof e "error handling %s with handler: %s"
                                     topic handle-fn)))))))]
    (doseq [f futures] @f)))

(defn- add-subscription*
  "Add some subscriptions.

  For each `topic-filter`, we only submit one subscription function to the MQTT.
  That function will dispatch message to related `handle-fn` set.

  Arguments:

  * `type-handlers`: Array of handlers, each of which is a map:

    {:keys [`typ` `handle-fn` `topic-filter` `message-adatper`]}


  * `client`: The MQTT client

  * The `subscriptions` is a atom of map:

    `topic-filter` -> [`typ` `handle-fn`] -> {:keys [`handlers` `sub-fn`]}

    The `sub-fn` is a delayed value which deref to the actual MQTT subscription
    function.

    The `handlers` is a map, contains the `handle-fn` set for given
    `topic-filter`.
  "
  [type-handlers client subscriptions]
  (let [ms
        (swap!
         subscriptions
         (fn [m0]
           (reduce
            (fn [m {:keys [typ handle-fn topic-filter message-adapter]}]
              (let [h {[typ handle-fn] {:message-adapter message-adapter}}]
                (->> (if-let [{:keys [handlers sub-fn]} (m topic-filter)]
                       {:handlers (conj handlers h)
                        :sub-fn sub-fn}
                       {:handlers h
                        :sub-fn
                        (delay (partial handle-mqtt-message
                                        #(get-in @subscriptions [topic-filter :handlers])))})
                     (assoc m topic-filter))))
            m0
            type-handlers)))]
    (doseq [[topic-filter {:keys [sub-fn]}]
            (->> type-handlers
                 (map (fn [{:keys [topic-filter]}]
                        [topic-filter (ms topic-filter)])))]
      (when-not (realized? sub-fn)
        (log/debugf "start listen on %s" topic-filter)
        (mh/subscribe client {topic-filter 1} @sub-fn)))))

(defn- remove-subscription*
  "Remove some subscriptions.

  Try remove given handler from corresponding `topic-filter`'s handler map, if
  no handlers for given `topic-filter` exists, remove its subscription from the
  MQTT client.

  The parameter is the same as [[add-subscription*]]. Except for the
  `type-handlers`, here we don't need the `message-adapter`.
  "
  [type-handlers client subscriptions]
  (let [[old new]
        (swap-vals!
         subscriptions
         (fn [m0]
           (reduce
            (fn [m {:keys [typ handle-fn topic-filter]}]
              (if-let [{:keys [handlers sub-fn]} (m topic-filter)]
                (let [handlers (dissoc handlers [typ handle-fn])]
                  (if (empty? handlers)
                    (dissoc m topic-filter)
                    (assoc m topic-filter {:handlers handlers :sub-fn sub-fn})))
                m))

            m0
            type-handlers)))]
    (doseq [{:keys [topic-filter]} type-handlers]
      (when (and (contains? old topic-filter) (not (contains? new topic-filter)))
        (let [{:keys [sub-fn]} (old topic-filter)]
          (when (and (realized? sub-fn)
                     (mh/connected? client))
            (mh/unsubscribe client topic-filter)))))))

(defmulti create-send-data
  "Convert the message to MQTT topic & payload.

  Arguments: `[node-id, {:as msg :keys [typ data}]`

  Returns: `[topic, payload]`"
  (fn [_node-id {:as msg :keys [typ]}] typ))

(defmulti create-sub-data
  "Create a topic filter and a message adapter for subscription.

  The message adapter convert the MQTT topic & payload to the sent message for
  `handler-fn`'s usage.

  Arguments: `[node-id {:as subscription :keys [handle-fn]} typ]`

  - `node`: current node

  Returns: `[topic-filter, message-adapter]`
  "
  (fn [_node-id _subscription typ] typ))

;; Heartbeat
;; *ns-prefix*/<ver>/registry/hb/<node-id>
(defn- topic:hb:parse [topic]
  (let [[_ node-id] (-> (re-pattern (str *ns-prefix* "/" ver "/registry/hb/([^/]+)"))
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
  (let [[_ a b c] (-> (re-pattern (str *ns-prefix* "/" ver "/rpc/req/([^/]+)/([^/]+)/([^/]+)"))
                      (re-matches topic))]
    {:caller-node-id b :callee-node-id a :req-id c}))

(defmethod create-send-data :req
  [node-id {:as msg :keys [data]}]
  [(let [msg-meta (meta msg)
         {:keys [callee-node-id req-id]} msg-meta]
     (when (or (not callee-node-id) (not req-id))
       (->> {:msg msg :msg-meta msg-meta}
            (ex-info "metadata :callee-node-id and :req-id is required for sending :req")
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
  (let [[_ a b c] (-> (re-pattern (str *ns-prefix* "/" ver "/rpc/resp/([^/]+)/([^/]+)/([^/]+)"))
                      (re-matches topic))]
    {:caller-node-id a :callee-node-id c :req-id b}))

(defmethod create-send-data :resp
  [node-id {:as msg :keys [data]}]
  [(let [msg-meta                        (meta msg)
         {:keys [req-id caller-node-id]} msg-meta]
     (when (or (not req-id) (not caller-node-id))
       (->> {:msg msg :msg-meta msg-meta}
            (ex-info "metadata :caller-node-id and :req-id is required for sending :resp")
            throw))
     (str *ns-prefix* "/" ver "/rpc/resp/" caller-node-id "/" req-id "/" node-id))
   data])

(defmethod create-sub-data :resp
  [node-id {:as subscription} _]
  [(let [{:keys [req-id callee-node-id]} (meta subscription)]
     (str *ns-prefix* "/" ver "/rpc/resp/" node-id
          "/" (or req-id "+") "/" (or callee-node-id "+")))
   (fn [topic data]
     (let [m (topic:resp:parse topic)]
       (with-meta {:typ :resp :data data} m)))])

(defn- create-mqtt-xnfun-transport
  [{:keys [client closed create-send-data* create-sub-data* subscriptions]}]
  (let [subscription-to-type-handlers
        (fn [{:as subscription :keys [types handle-fn]}]
          (->> (set types)
               (map (fn [typ]
                      (let [[topic-filter message-adapter]
                            (create-sub-data* subscription typ)]
                        {:typ typ
                         :topic-filter topic-filter
                         :message-adapter message-adapter
                         :handle-fn handle-fn})))))]
    (reify XNFunTransport
      (send-msg [_ {:as msg :keys [data]}]
        (let [[topic data] (create-send-data* msg)]
          (mh/publish client topic (*to-mqtt-payload* data))))

      (add-subscription [_ subscription]
        (try
          (-> (subscription-to-type-handlers subscription)
              (add-subscription*  client subscriptions))
          #(.remove-subscription _ subscription)
          (catch Exception e
            (.remove-subscription _ subscription)
            (throw e))))

      (remove-subscription [_ subscription]
        (-> (subscription-to-type-handlers subscription)
            (remove-subscription* client subscriptions)))

      (closed? [_]
        @closed)

      (close! [_]
        (try (mh/disconnect client)
             (finally (reset! closed true)))))))

(defn- make-mqtt-transport*
  "This is the internal implementation that expose the internal state for
  development."
  [node-id {:as mqtt-transport :keys [topic-prefix mqtt-config]}]
  (let [mqtt-topic-prefix (str/replace (or topic-prefix "") #"/+$" "")

        client
        (->> (cond-> {}
               (:client-id mqtt-config)
               (assoc :client-id (:client-id mqtt-config))

               (:connect-options mqtt-config)
               (assoc :opts (:connect-options mqtt-config)))
             (mh/connect (:broker mqtt-config)))

        state
        {:client client
         ;; topic to handler functions
         :subscriptions (atom {})
         ;; if the transport is closed
         :closed (atom false)}

        options
        (-> state
            (assoc :create-send-data*
                   #(binding [*ns-prefix* mqtt-topic-prefix]
                      (create-send-data node-id %)))
            (assoc :create-sub-data*
                   #(binding [*ns-prefix* mqtt-topic-prefix]
                      (create-sub-data node-id %1 %2))))]
    {:transport (create-mqtt-xnfun-transport options)
     :state state}))

(defn make-mqtt-transport
  "Create the mqtt transport for node `node-id` using the given transport
  configuration `mqtt-transport`.

  MQTT transport configuration `mqtt-trans-port` contains two parts:

  - `topic-prefix`: all message's topics would be prefixed with it, consider it
    as a namespace.
  - `mqtt-config`: the mqtt connection configuration, it contains two part
      - `:broker`: Connection uri, ex. `tcp://127.0.0.1:1883`
      - `:client-id` (optional)
      - `:connect-options`: a map, all fields are optional:
          * `:username` (string)
          * `:password` (string or char array)
          * `:auto-reconnect` (bool)
          * `:connection-timeout` (int)
          * `:clean-session` (bool)
          * `:keep-alive-interval` (int)
          * `:max-in-flight` (int)
          * `:will`: `{:keys [topic payload qos retain]}`
  "
  [node-id {:as mqtt-transport :keys [topic-prefix mqtt-config]}]
  (:transport (make-mqtt-transport* node-id mqtt-transport)))

(comment

  ;; Playground
  ;; install `mosquitto` and start.

  ;; Setup
  (do
    (defn n [v node-id]
      (when v (try (.close! (:transport v)) (catch Exception _)))
      (make-mqtt-transport*
       node-id
       {:mqtt-config
        {:broker "tcp://127.0.0.1:1883"
         :client-id node-id
         :connect-options {:automatic-reconnect true}}}))
    (defonce l0 (atom nil))
    (defonce l1 (atom nil))

    (defonce h (fn [name msg]
                 (->> (format "\n  msg:%s\n  meta: %s" msg (meta msg))
                      (println name "recv:"))))

    (defonce sub0 (fn [s] (.add-subscription (:transport @l0) s)))
    (defonce unsub0 (fn [s] (.remove-subscription (:transport @l0) s)))
    (defonce pub0 (fn [m] (.send-msg (:transport @l0) m)))

    (defonce sub1 (fn [s] (.add-subscription (:transport @l1) s)))
    (defonce unsub1 (fn [s] (.remove-subscription (:transport @l1) s)))
    (defonce pub1 (fn [m] (.send-msg (:transport @l1) m)))

    (defonce restart!
      (fn [] (swap! l0 n "node-0") (swap! l1 n "node-1") 'ok))
    (defonce cleanup
      (fn [] (.close! (:transport @l0)) (.close! (:transport @l1)) 'ok)))

  ;; Heartbeat
  (do
    (def h0 (partial h "h0"))
    (def s0 {:types [:hb] :handle-fn h0})

    (def u0 (sub0 s0))
    (pub0 {:typ :hb :data "hello world!"})
                                        ;
    )
  (unsub0 s0)
  (u0)

  ;; Request
  (do
    (def h1 (partial h "h1"))
    (def s1 {:types [:req] :handle-fn h1})

    (def u1 (sub0 s1))
    (pub0 (with-meta
            {:typ :req :data "request message"}
            {:callee-node-id "node-0" :req-id "req-0"}))
    ;; should recv this one (callee is not me)
    (pub0 (with-meta
            {:typ :req :data "request message"}
            {:callee-node-id "node-1" :req-id "req-0"}))
    ;
    )
  (u1)

  ;; Response
  (do
    (def caller-node-0 (partial h "caller-node-0"))
    (def s-caller-node-0 (with-meta
                           {:types [:resp] :handle-fn caller-node-0}
                           {}))

    (def u-caller-node-0 (sub0 s-caller-node-0))

    (def resp-node-0-only (partial h "node-0 only"))
    (def s-resp-node-0-only (with-meta
                              {:types [:resp] :handle-fn resp-node-0-only}
                              {:callee-node-id "node-0"}))
    (def u-resp-node-0-only (sub0 s-resp-node-0-only))

    (def resp-req-0-only (partial h "req-0 only"))
    (def s-resp-req-0-only (with-meta
                             {:types [:resp] :handle-fn resp-req-0-only}
                             {:req-id "req-0"}))
    (def u-resp-req-0-only (sub0 s-resp-req-0-only))

    (pub0 (with-meta
            {:typ :resp :data "response"}
            {:caller-node-id "node-0" :req-id "req-0"}))

    (pub0 (with-meta
            {:typ :resp :data "response"}
            {:caller-node-id "node-0" :req-id "req-1"}))

    (pub1 (with-meta
            {:typ :resp :data "response"}
            {:caller-node-id "node-0" :req-id "req-0"}))

    (pub1 (with-meta
            {:typ :resp :data "response"}
            {:caller-node-id "node-0" :req-id "req-1"}))

    ;; caller node-1 does not have any subscription
    (pub0 (with-meta
            {:typ :resp :data "response"}
            {:caller-node-id "node-1" :req-id "req-1"}))
    ;;
    )

  ;; cleanup
  (cleanup)

  ;; end of playground
  )
