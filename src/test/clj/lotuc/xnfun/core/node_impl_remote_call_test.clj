(ns lotuc.xnfun.core.node-impl-remote-call-test
  (:require
   [clojure.core.async :refer [>!!]]
   [clojure.test :as t]
   [lotuc.xnfun.core.node-impl :as impl]
   [lotuc.xnfun.core.sample-functions :as f]
   [lotuc.xnfun.protocols :as xnp]))

(defn- cleanup
  [n]
  (try (xnp/stop-node-transport! n) (catch Exception _))
  (try (xnp/stop-heartbeat! n) (catch Exception _))
  (try (xnp/stop-heartbeat-listener! n) (catch Exception _)))

;; ensure mqtt broker tcp://127.0.0.1 (anonymous) is available.

(t/deftest remote-node-call-unary
  (let [n-42 (impl/make-node {:node-id "node-42"})
        n-43 (impl/make-node {:node-id "node-43"})]
    (xnp/start-remote! n-42)
    (xnp/start-remote! n-43)
    (xnp/start-serve-remote-call! n-42)
    (xnp/start-serve-remote-call! n-43)
    (xnp/add-function! n-42 :add (fn [[x y]] (+ x y)))

    (Thread/sleep 200)

    (let [{:as r :keys [res-promise]}
          (xnp/submit-remote-call! n-43 :add [2 3])]
      (t/is (= {:status :ok :data 5}
               (deref res-promise 1000 :timeout))
            (str r)))

    (try
      (finally
        (cleanup n-42)
        (cleanup n-43)))))

(t/deftest remote-node-call-bidirectional-call
  (let [n-42 (impl/make-node {:node-id "node-42"})
        n-43 (impl/make-node {:node-id "node-43"})]
    (xnp/start-remote! n-42)
    (xnp/start-remote! n-43)
    (xnp/start-serve-remote-call! n-42)
    (xnp/start-serve-remote-call! n-43)
    (xnp/add-function! n-42 :echo-server f/echo-server)

    (Thread/sleep 200)

    (let [{:as r :keys [req]}
          (xnp/submit-remote-call!
           n-43 :echo-server nil
           {:req-meta {:hb-interval-ms 20
                       :hb-lost-ratio 2
                       :timeout-ms 200}})]

      (try
        (t/is (= "hello world"
                 (f/echo req "hello world")))
        (finally
          (>!! (:in-c req) {:typ :xnfun/cancel :data :anything}))))

    (try
      (finally
        (cleanup n-42)
        (cleanup n-43)))))
