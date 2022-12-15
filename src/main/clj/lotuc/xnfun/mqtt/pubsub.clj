(ns lotuc.xnfun.mqtt.pubsub
  (:require [lotuc.xnfun.mqtt.client :as mqtt]
            [taoensso.nippy :as nippy]))

(defn unsub [client topic-filter]
  (mqtt/unsubscribe client topic-filter))

(defn pub-edn [client topic data]
  (mqtt/publish client topic (nippy/freeze data)))

(defn sub-edn [client topic-filter listener & {:as options}]
  (let [l (fn [topic data] (listener topic (nippy/thaw (:payload data))))]
    (mqtt/subscribe client topic-filter l options))
  #(unsub client topic-filter))

(comment
  (def client
    (-> {:broker "tcp://127.0.0.1:1883"
         :client-id (str (random-uuid))
         :connect-options {:max-in-flight 1000
                           :automatic-reconnect true}
         :callback
         {:message-arrived nil
          :delivery-complete nil
          :on-disconnect
          (fn [_c] (println "disconnected"))
          :connection-lost
          (fn [_c cause] (println "connection lost on %s" cause))
          :connect-complete
          (fn [_c reconnect uri]
            (-> (format "%s to %s" (if reconnect "reconnected" "connected") uri)
                println))}}
        mqtt/make-client))

  (sub-edn client "/hello/#" (fn [t d] (println t d)))
  (pub-edn client "/hello/world" "hello world")
  (mqtt/disconnect client)
  ;;
  )
