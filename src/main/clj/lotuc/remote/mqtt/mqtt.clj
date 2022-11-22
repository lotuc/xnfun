(ns lotuc.remote.mqtt.mqtt
  (:require [taoensso.nippy :as nippy])
  (:import (org.eclipse.paho.client.mqttv3 MqttClient
                                           MqttConnectOptions
                                           MqttMessage
                                           MqttCallbackExtended
                                           IMqttMessageListener)
           (org.eclipse.paho.client.mqttv3.persist MemoryPersistence)))

(defmacro ^:private set-when-not-nil [obj setter val]
  `(when-not (nil? ~val) (~setter ~obj ~val)))

(defn- MqttMessage->map [m]
  (dissoc (bean m) :class))

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
           (catch Exception e (prn "error handling data from" topic e))))))

(defn make-client
  [{:keys [broker client-id
           persistence ;; defaults to be memory persistence
           connect-options
           callback]}]
  (try
    (let [persistence (or persistence (MemoryPersistence.))
          client-id (or client-id (.toString (java.util.UUID/randomUUID)))
          client (MqttClient. broker client-id persistence)]
      (when callback
        (.setCallback client (new-callback client callback)))
      (when connect-options
        (.connect client (new-option connect-options)))
      client)
    (catch Exception e (throw (RuntimeException. e)))))

(defmacro subscribe [client topic-filter listener & {:keys [qos]}]
  (let [qos (if (nil? qos) 2 qos)]
    `(.subscribe ~client ~topic-filter ~qos (new-listener ~listener))))

(defmacro unsubscribe [client topic-filter]
  `(.unsubscribe ~client ~topic-filter))

(defn send-message
  [client topic message & options]
  (.publish
   client topic
   (cond
     (instance? MqttMessage message) message
     :else (map->MqttMessage (assoc options :message message)))))

(def ^:dynamic *client*
  (make-client {:broker "tcp://127.0.0.1:1883"
                :client-id (str (random-uuid))
                :connect-options {:max-in-flight 1000}}))

(defmacro client []
  `(do (assert (not (nil? *client*)) "*client* not bound")
       *client*))

(defn unsub [topic-filter]
  (unsubscribe (client) topic-filter))

(defn pub-edn [topic data]
  (send-message (client) topic (nippy/freeze data)))

(defn sub-edn [topic-filter listener & options]
  (subscribe (client) topic-filter
             (fn [topic data] (listener topic (nippy/thaw (:payload data))))
             options))
