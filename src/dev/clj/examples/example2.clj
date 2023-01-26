(ns examples.example2
  (:require [lotuc.xnfun.api :as xn]
            [lotuc.xnfun.sample.sample-funcs :as sample-funcs]
            [clojure.core.async :refer [go-loop put! <!]]))

;;; Start two nodes for demostration
(def n0 (xn/start-node {}))
(def n1 (xn/start-node {}))

;;; Register calculator function to node n1.
(xn/add-function n1 "calculator" sample-funcs/f-calculator)

;;; Do a long running call (from n0 to n1)
(def c (xn/call-function n0 "calculator" nil
                         {:req-meta {:timeout-ms 3600000
                                     :hb-interval-ms 3000
                                     :hb-lost-ratio 2}}))

;;; Channel for receiving message from callee
(def out-c (get-in c [:request :out-c]))
;;; Channel for sending message to callee
(def in-c (get-in c [:request :in-c]))

;;; Here we setup a go loop for recv calc-res
(go-loop [{:as v :keys [typ data]} (<! out-c)]
  (if v (do (when (= typ :xnfun/to-caller)
              (let [{:keys [exp res]} data]
                (println "> " exp)
                (println res)))
            (recur (<! out-c)))
      (println "quit")))

;;; Send some expresstion to the callee for calculation; obeserve calculated
;;; result.
(put! in-c {:typ :xnfun/to-callee :data '(+ 1 2)})
(put! in-c {:typ :xnfun/to-callee :data '(+ 1 (* 3 2) (+ 3 4))})

;;; Stop the callee
(put! in-c {:typ :xnfun/cancel})

;;; Stop nodes.
(xn/stop-node n0)
(xn/stop-node n1)
