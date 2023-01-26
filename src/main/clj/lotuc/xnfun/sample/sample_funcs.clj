(ns lotuc.xnfun.sample.sample-funcs
  (:require
   [clojure.core.async :refer [<! >!! go-loop put!]]
   [clojure.tools.logging :as log]))

(defn f-delayed-add [[a b delay-ms]]
  (Thread/sleep delay-ms)
  (+ a b))

(defn f-delayed-sum [args delay-ms]
  (Thread/sleep delay-ms)
  (apply + args))

(defn f-ping [_] "pong")

(defn f-echo [args] args)

(defn f-add [[a b]] (+ a b))

(defn f-sub [[a b]] (- a b))

(defn f-throw [e] (throw e))

(defn f-delayed-sum-with-hb
  "Argument list
   - `0.add-delay-ms`: waiting ms for every add action.
   - `0.hb-with-msg`: determines what message to send as heartbeat
     truthy value: partial message
     falsy value: heartbeat message
  "
  [[nums {:keys [add-delay-ms hb-with-msg]}]
   {:keys [in-c out-c]
    {:keys [req-id]} :req-meta}]
  (let [stop (atom false)
        hb-num (atom 0)]
    (go-loop [{:keys [typ] :as d} (<! in-c)]
      (when d
        (when (= typ :xnfun/cancel) (swap! stop (fn [_] true)))
        (recur (<! in-c))))
    (loop [nums nums r 0]
      (if (empty? nums)
        r
        (let [v (first nums)]
          (log/debugf "[%s] sleep for: %sms" req-id add-delay-ms)
          (Thread/sleep add-delay-ms)
          (if @stop
            :stopped
            (let [msg (if hb-with-msg
                        {:typ :msg :data (str "add " v)}
                        {:typ :xnfun/hb :data (swap! hb-num inc)})]
              (log/debugf "[%s] heartbeat with: %s" req-id msg)
              (when-not (>!! out-c msg)
                (log/warnf "[%s] out-c closed" req-id))
              (log/debugf "[%s] message sent: %s" req-id msg)
              (recur (rest nums) (+ v r)))))))))

(defn f-calculator
  "Got input from caller via `in-c` channel. Send result to caller via `out-c`
  channel."
  [_ {:keys [in-c out-c]
      {:as req-meta
       :keys [req-id hb-interval-ms]} :req-meta}]
  (log/infof "start calculator: %s" req-meta)
  (let [p (promise)]
    (letfn [(calc [exp]
              (if (number? exp) exp
                  (let [op   (case (keyword (first exp))
                               :+ + :- - :* * :/ / nil)
                        args (map calc (rest exp))]
                    (if (nil? op) nil (apply op args)))))]

      ;; Keep heartbeat.
      (future
        (loop []
          (put! out-c {:typ :xnfun/hb})
          (log/debugf "[%s] heartbeat" req-id)
          (when (= :timeout (deref p hb-interval-ms :timeout)) (recur))))

      ;; Wait for message, do calculation, and return result.
      (go-loop []
        (when-let [{:keys [typ] :as d} (<! in-c)]
          (log/debugf "[%s] calculator - handling: %s" req-id d)
          (case typ
            :xnfun/cancel
            (do (log/infof "[%s] cancelled" req-id)
                (deliver p :cancel))    ; quit

            :xnfun/to-callee
            (let [r (calc (:data d))]   ; calc
              (put! out-c {:typ :xnfun/to-caller :data {:exp (:data d) :res r}})
              (recur))

            (deliver p {:unkown d}))))
      @p)))
