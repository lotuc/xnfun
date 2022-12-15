(ns lotuc.xnfun.mqtt.pubsub-test
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [lotuc.xnfun.mqtt.client-fixture :as f]
            [lotuc.xnfun.mqtt.pubsub :as ps]))

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
      (fn [_c] (log/infof "disconnected"))
      :connection-lost
      (fn [_c cause]
        (log/infof "connection lost on %s" cause))
      :connect-complete
      (fn [_c reconnect uri]
        (log/infof
         "%s to %s"
         (if reconnect "reconnected" "connected") uri))}}))

(t/deftest pub-sub
  (let [r (promise)]
    (doto f/*mqtt-client*
      (ps/sub-edn "/hello/#" (fn [topic data] (deliver r [topic data])))
      (ps/pub-edn "/hello/world" "world"))
    (t/is (= ["/hello/world" "world"] (deref r 100 :timeout)) "pub & sub works"))

  (let [r0 (promise)
        r1 (promise)]
    (doto f/*mqtt-client*
      (ps/sub-edn "/hello/#" (fn [topic data] (deliver r0 [topic data])))
      (ps/unsub "/hello/#")
      (ps/pub-edn "/hello/world" "world"))
    (let [u (ps/sub-edn f/*mqtt-client* "/hello/#"
                        (fn [topic data] (deliver r1 [topic data])))]
      (u)
      (ps/pub-edn f/*mqtt-client* "/hello/world" "world"))

    (t/is (= :timeout (deref r0 100 :timeout)) "unsub by topic works")
    ;; already waited, so we shrink the waiting time
    (t/is (= :timeout (deref r1 1 :timeout)) "unsub by sub response works")))
