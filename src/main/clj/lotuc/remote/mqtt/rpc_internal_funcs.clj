(ns lotuc.remote.mqtt.rpc-internal-funcs)

(defn func-delayed-add [[a b]]
  (Thread/sleep 1000)
  (+ a b))

(defn func-delayed-sum [args]
  (Thread/sleep 1000)
  (apply + args))
