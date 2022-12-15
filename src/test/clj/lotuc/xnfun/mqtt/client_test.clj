(ns lotuc.xnfun.mqtt.client-test
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [lotuc.xnfun.mqtt.client-fixture :as f]
            [lotuc.xnfun.mqtt.client :as mqtt]))

(def ^:private count-disconnect (atom 0))
(def ^:private count-connect (atom 0))

(t/use-fixtures :once
  (f/with-mqtt-client-for-test "client"
    {:broker "tcp://127.0.0.1:1883"
     :client-id (str (random-uuid))
     :connect-options {:max-in-flight 1000
                       :automatic-reconnect true}
     :callback
     {:message-arrived nil
      :delivery-complete nil
      :on-disconnect
      (fn [_c]
        (swap! count-disconnect inc)
        (log/infof "disconnected"))
      :connection-lost
      (fn [_c cause]
        (log/infof "connection lost on %s" cause))
      :connect-complete
      (fn [_c reconnect uri]
        (swap! count-connect inc)
        (log/infof
         "%s to %s"
         (if reconnect "reconnected" "connected") uri))}}))

(t/deftest client
  (let [msg        "hello world"
        sub-topic  "/hello/#"
        send-topic "/hello/world"
        c-disconnect @count-disconnect
        c-connect    @count-connect]

    (let [p (promise)]
      (doto f/*mqtt-client*
        (mqtt/subscribe sub-topic (fn [t d] (deliver p [t d])))
        (mqtt/publish send-topic msg))
      (t/is (= [send-topic msg]
               (let [[t d] (deref p 100 nil)]
                 [t (String. (:payload d))]))))

    (let [p (promise)]
      (doto f/*mqtt-client*
        (mqtt/subscribe sub-topic (fn [t d] (deliver p [t d])))
        (mqtt/disconnect sub-topic))
      ;; on-disconnect cb is called asynchrously, wait for it to complete
      (while (mqtt/connected? f/*mqtt-client*) (Thread/sleep 10))
      (t/is (= (+ 1 c-disconnect) @count-disconnect) "check on-disconnect cb")
      (t/is (= (+ 0 c-connect) @count-connect))

      (t/is (not (mqtt/connected? f/*mqtt-client*)))
      (t/is (nil? (deref p 100 nil)))

      (doto f/*mqtt-client*
        mqtt/connect
        (mqtt/publish send-topic msg))
      (t/is (= (+ 1 c-disconnect) @count-disconnect))
      (t/is (= (+ 1 c-connect) @count-connect) "check connect complete cb")

      (t/is (mqtt/connected? f/*mqtt-client*))
      (t/is (= [send-topic msg]
               (let [[t d] (deref p 100 nil)]
                 [t (String. (:payload d))]))
            "automatically resubscribe on reconnect"))

    (let [p0 (promise)
          p1 (promise)]
      (doto f/*mqtt-client*
        (mqtt/subscribe sub-topic (fn [_t _d] (deliver p0 "from first")))
        (mqtt/subscribe sub-topic (fn [_t _d] (deliver p1 "from second")))
        (mqtt/publish send-topic "hello"))

      (t/is (= "from second" (deref p1 100 nil)))
      (t/is (nil? (deref p0 10 nil)) "sub of same topic overwrites"))))
