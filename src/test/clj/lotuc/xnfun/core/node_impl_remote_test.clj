(ns lotuc.xnfun.core.node-impl-remote-test
  (:require
   [clojure.test :as t]
   [lotuc.xnfun.core.node-impl :as impl]
   [lotuc.xnfun.protocols :as xnp]))

(defn- cleanup
  [n]
  (try (xnp/stop-node-transport! n) (catch Exception _))
  (try (xnp/stop-heartbeat! n) (catch Exception _))
  (try (xnp/stop-heartbeat-listener! n) (catch Exception _)))

;; ensure mqtt broker tcp://127.0.0.1 (anonymous) is available.

(t/deftest remote-node-heartbeat
  (let [n-42 (impl/make-node
              {:node-id "node-42"
               :node-options
               {:hb-options {:hb-interval-ms 20
                             :hb-lost-ratio 2}}})
        n-43 (impl/make-node
              {:node-id "node-43"
               :node-options
               {:hb-options {:hb-interval-ms 20
                             :hb-lost-ratio 2}}})]
    (try
      (xnp/start-node-transport! n-42)
      (xnp/start-node-transport! n-43)

      (Thread/sleep 100)
      (let [ns @(xnp/node-remote-nodes n-42)]
        (t/is (empty? ns) (str ns)))
      (let [ns @(xnp/node-remote-nodes n-43)]
        (t/is (empty? ns) (str ns)))

      (xnp/start-heartbeat! n-43)
      (Thread/sleep 100)
      (let [ns @(xnp/node-remote-nodes n-42)]
        (t/is (empty? ns) (str ns)))
      (let [ns @(xnp/node-remote-nodes n-43)]
        (t/is (empty? ns) (str ns)))

      ;; n-42 listen, n-43 heartbeat
      (xnp/start-heartbeat-listener! n-42)
      (Thread/sleep 100)
      (xnp/start-heartbeat! n-43)
      (Thread/sleep 100)
      (let [ns @(xnp/node-remote-nodes n-42)]
        (t/is (some? (get ns (:node-id n-43)))))
      (let [ns @(xnp/node-remote-nodes n-43)]
        (t/is (empty? ns) (str ns)))

      ;; add function triggers heartbeat
      (xnp/add-function! n-43 :add (fn [[x y]] (+ x y)))
      (Thread/sleep 100)
      (let [ns @(xnp/node-remote-nodes n-42)]
        (t/is (some? (get-in ns [(:node-id n-43) :functions :add])) (str ns)))

      ;; stop n-43's heartbeat
      (xnp/stop-heartbeat! n-43)
      (Thread/sleep 60)
      ;; now n-42 should be heartbeat lost
      (let [ns @(xnp/node-remote-nodes n-42)]
        (t/is (empty? ns) (str ns)))

      (finally
        (cleanup n-42)
        (cleanup n-43)))))
