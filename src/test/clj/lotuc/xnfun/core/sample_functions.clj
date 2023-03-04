(ns lotuc.xnfun.core.sample-functions
  (:require
   [clojure.core.async :refer [<! >! >!! go-loop]]))

(defn echo-server [_ {:keys [req-meta in-c out-c]}]
  (let [result (promise)
        hb? (promise)
        hb-interval-ms (get req-meta :hb-interval-ms 30000)]

    ;; do heartbeat when both `result` and `hb?` are **not** fullfilled.
    (go-loop []
      (when (and (= :timeout (deref result (/ hb-interval-ms 2.0) :timeout))
                 (= :timeout (deref hb? (/ hb-interval-ms 2.0) :timeout)))
        (>! out-c {:typ :xnfun/hb :data :hb})
        (recur)))

    (go-loop []
      (when-let [{:as d :keys [typ data]} (<! in-c)]
        (when (= data :stop-heartbeat)
          (deliver hb? :stop-heartbeat))

        (cond
          (= typ :xnfun/cancel)
          (deliver result data)

          (= typ :xnfun/to-callee)
          (>! out-c {:typ :xnfun/to-caller :data data})

          :else
          (deliver result {:reason :unkown-data :data d}))
        (recur)))

    @result))

(defn echo [{:as echo-server-call-result :keys [in-c out-c]} msg]
  (>!! in-c {:typ :xnfun/to-callee :data msg})
  (let [r (promise)]
    (go-loop []
      (when-let [{:keys [typ data]} (<! out-c)]
        (if (= typ :xnfun/to-caller)
          (deliver r data)
          (recur))))
    (deref r 2000 :timeout)))
