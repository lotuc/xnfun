(ns lotuc.xnfun.mqtt.client-fixture
  (:require [clojure.test :as t]
            [lotuc.xnfun.mqtt.client :as mqtt]
            [clojure.tools.logging :as log]))

(def ^:dynamic *mqtt-client*)

(defn with-mqtt-client-for-test
  [name options]
  (fn [f]
    (binding [*mqtt-client*
              (try (mqtt/make-client options) (catch Exception _))]
      (when (nil? *mqtt-client*)
        (log/warnf
         (str "Skip testing %s. "
              "Start a mqtt broker for %s")
         name (:broker options))
        (t/is true (str "skip testing " name)))
      (when *mqtt-client*
        (try (f) (finally (mqtt/disconnect *mqtt-client*)))))))
