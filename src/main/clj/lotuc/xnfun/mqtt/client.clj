(ns lotuc.xnfun.mqtt.client
  (:import (org.eclipse.paho.client.mqttv3 MqttClient
                                           MqttConnectOptions
                                           MqttMessage
                                           MqttCallbackExtended
                                           IMqttMessageListener)
           (org.eclipse.paho.client.mqttv3.persist MemoryPersistence))
  (:require [clojure.tools.logging :as log]))

(defmacro ^:private set-when-not-nil [obj setter val]
  `(when-not (nil? ~val) (~setter ~obj ~val)))

(defmacro ^:private client-of [client]
  `(if (instance? MqttClient ~client) ~client (:client ~client)))

(defn- MqttMessage->map [m]
  (dissoc (bean m) :class))

(defn- qos-of
  "Get qos from options with default qos."
  [{:keys [qos]}]
  (if (nil? qos) 2 qos))

(defn- map->MqttMessage
  "send payload on default. when no payload given, try send message"
  [{:keys [qos retained id payload] :as m}]
  (let [r (MqttMessage.)
        message (:message m)]
    (set-when-not-nil r .setQos qos)
    (set-when-not-nil r .setRetained retained)
    (set-when-not-nil r .setId id)
    (.setPayload
     r
     (if-not (nil? payload)
       payload
       (if message
         (cond
           (bytes? message) message
           (string? message) (.getBytes message)
           :else
           (throw (ex-info "invalid message type" {:message m})))
         (throw (ex-info "invalid message - no payload or message given"
                         {:message m})))))
    r))

(defn- new-option
  [{:keys [username password
           clean-session
           connection-timeout keep-alive-interval max-reconnect-delay
           max-in-flight
           will automatic-reconnect]}]
  (let [opts (MqttConnectOptions.)
        {:keys [topic payload qos retained]} will]
    (set-when-not-nil opts .setUserName username)
    (set-when-not-nil opts .setPassword password)
    (set-when-not-nil opts .setCleanSession clean-session)
    (set-when-not-nil opts .setConnectionTimeout connection-timeout)
    (set-when-not-nil opts .setKeepAliveInterval keep-alive-interval)
    (set-when-not-nil opts .setMaxReconnectDelay max-reconnect-delay)
    (set-when-not-nil opts .setAutomaticReconnect automatic-reconnect)
    (set-when-not-nil opts .setMaxInflight max-in-flight)
    (when-not (nil? will) (.setWill opts topic payload qos retained))
    opts))

(defn- new-callback
  [client
   {:keys [message-arrived delivery-complete
           connection-lost connect-complete]}]
  (reify MqttCallbackExtended
    (connectComplete [_this reconnect server-uri]
      (when connect-complete
        (connect-complete client reconnect server-uri)))
    (connectionLost [_this cause]
      (when connection-lost
        (connection-lost client cause)))
    (messageArrived [_this topic message]
      (when message-arrived
        (message-arrived client topic (MqttMessage->map message))))
    (deliveryComplete [_this token]
      (when delivery-complete
        (delivery-complete client token)))))

(defn new-listener
  [listener]
  (reify IMqttMessageListener
    (messageArrived [_this topic m]
      (try (listener topic (MqttMessage->map m))
           (catch Exception e
             (log/warnf
              e "error handling data from %s: %s" topic listener))))))

(defn make-client
  [{:keys [broker client-id
           persistence ;; defaults to be memory persistence
           connect-options]
    {:as callback :keys [on-disconnect]} :callback}]
  (log/infof "make-client: %s" client-id)
  (try
    (let [persistence (or persistence (MemoryPersistence.))
          client-id (or client-id (.toString (java.util.UUID/randomUUID)))
          client (MqttClient. broker client-id persistence)]
      (when callback
        (.setCallback client (new-callback client callback)))
      (when connect-options
        (.connect client (new-option connect-options)))
      {:client client
       :callback callback
       :subscriptions (atom {})})
    (catch Exception e (throw (RuntimeException. e)))))

(defn connect
  "Connect or reconnect. And resubscribe all subscriptions before `disconnect`."
  [client]
  (let [c (client-of client)]
    (when-not (.isConnected c)
      (.connect c)
      (when-let [subscriptions (-> client :subscriptions)]
        (swap!
         subscriptions
         #(->> %
               (map (fn [[k {:as v
                             :keys [mqtt-message-listener]
                             {:keys [qos]} :options}]]
                      (.subscribe c k qos mqtt-message-listener)
                      [k v]))
               (into {})))))
    client))

(defn connected? [client]
  (-> (client-of client) .isConnected))

(defn disconnect
  "Disconnect from the server.

  https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttClient.html

  disconnect-timeout-ms is ignored when forcibly=false.
  "
  [client &
   {:keys [quiesce-timeout-ms forcibly disconnect-timeout-ms]
    :as options}]
  (let [c (client-of client)
        on-disconnect (-> client :callback :on-disconnect)]
    (if forcibly
      (cond
        (and (nil? quiesce-timeout-ms) (nil? disconnect-timeout-ms))
        (.disconnectForcibly c)

        (and (some? quiesce-timeout-ms) (some? disconnect-timeout-ms))
        (.disconnectForcibly c quiesce-timeout-ms disconnect-timeout-ms)

        (nil? quiesce-timeout-ms)
        (.disconnectForcibly c disconnect-timeout-ms)

        :else (throw (ex-info "invalid disconnect options" options)))
      (if (nil? quiesce-timeout-ms)
        (.disconnect c)
        (.disconnect c quiesce-timeout-ms)))
    (when on-disconnect (future (on-disconnect c)))
    client))

(defn subscribe [client topic-filter listener & {:as options :keys [qos]}]
  (let [c (client-of client)
        l (new-listener listener)
        qos (qos-of options)
        subscriptions (-> client :subscriptions)
        options (-> options (assoc :qos qos))]
    (.subscribe c topic-filter (qos-of options) l)
    (when subscriptions
      (->> #(let [s (get % topic-filter)]
              (when s
                (log/debugf "[%s] overwriting subscription: %s" c topic-filter))
              (assoc % topic-filter {:listener listener
                                     :options options
                                     :mqtt-message-listener l}))
           (swap! subscriptions)))
    client))

(defn unsubscribe [client topic-filter]
  (let [c (client-of client)
        subscriptions (-> client :subscriptions)]
    (.unsubscribe c topic-filter)
    (swap! subscriptions #(dissoc % topic-filter))
    client))

(defn publish
  [client topic message & options]
  (let [c   (if (instance? MqttClient client) client (:client client))
        msg (if (instance? MqttMessage message)
              message
              (map->MqttMessage (assoc options :message message)))]
    (.publish c topic msg)))
