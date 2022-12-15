(ns lotuc.xnfun.api
  (:require [lotuc.xnfun.rpc.node :as n]
            [lotuc.xnfun.rpc.impl :as impl]))

(defn start-node
  [{:as node :keys [node-id node-options]
    {:keys [heartbeat-interval-ms
            heartbeat-lost-ratio]} :node-options}]
  (impl/start-node node))

(defn stop-node [node]
  (impl/stop-node node))

(defn add-function [node fun-name fun & opt]
  (n/add-function node fun-name fun opt))

(defn call-function
  "
  Optional Arguments
  - `options.out-c`
  - `req-meta`
    - `:node-id`: call to given node.
    - `:with-stacktrace`
    - `:timeout-ms`
    - `:hb-interval-ms`
    - `:hb-lost-ratio`

  Returns {:keys [in-c out-c res-future]}
  "
  [node fun-name fun-params
   & {:as options
      :keys [req-meta out-c]
      {:keys [node-id]} :req-meta}]
  (impl/call-function node fun-name fun-params options))

(defn call
  [node fun-name fun-params & {:as options}]
  (let [r (impl/call-function node fun-name fun-params options)
        f (:res-future r)]
    (future (:res @f))))

(comment
  (do
    (def n0 (start-node {:node-options {:hb-interval-ms 3000}}))
    (def n1 (start-node {:node-options {:hb-interval-ms 3000}})))

  (do (require '[lotuc.xnfun.rpc.sample-funcs :as s])
      (require '[clojure.core.async :refer [go-loop put! <!]])

      (add-function n1 "calculator" s/f-calculator))
  (def c (call-function n0 "calculator" nil
                        {:req-meta {:timeout-ms 3600000
                                    :hb-interval-ms 3000
                                    :hb-lost-ratio 2}}))

  (go-loop [{:as v :keys [type data]} (<! (:out-c c))]
    (if v (do (when (= type :calc-res)
                (let [{:keys [exp res]} data]
                  (println "> " exp)
                  (println res)))
              (recur (<! (:out-c c))))
        (println "quit")))
  (put! (:in-c c) {:type :calc :data '(+ 1 2)})
  (put! (:in-c c) {:type :calc :data '(+ 1 (* 3 2) (+ 3 4))})
  (put! (:in-c c) {:type :xnfun/cancel})

  (do (stop-node n0) (stop-node n1))

  (do
    (add-function n0 "add" (fn [[a b]] (+ a b)))
    (add-function n1 "sub" (fn [[a b]] (- a b))))

  [(-> n0 :node-state :running-requests deref)
   (-> n0 :node-state :waiting-promises deref)
   (-> n1 :node-state :running-requests deref)
   (-> n1 :node-state :waiting-promises deref)]

  [(-> n0 :node-state :worker-nodes deref)
   (-> n1 :node-state :worker-nodes deref)]

  @(call n1 "add" [1 2])
  @(call n0 "sub" [1 2])

  ;;
  )
