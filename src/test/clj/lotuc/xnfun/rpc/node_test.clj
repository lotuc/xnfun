(ns lotuc.xnfun.rpc.node-test
  (:require
   [clojure.core.async :refer [<! <!! >!! chan go-loop put!]]
   [clojure.test :as t]
   [clojure.tools.logging :as log]
   [lotuc.xnfun.rpc.node :as n]
   [lotuc.xnfun.rpc.sample-funcs :as s])
  (:import
   (java.util.concurrent CancellationException ExecutionException)))

(t/deftest heartbeat
  (let [n (n/make-node {:node-options {:hb-interval-ms 1000
                                       :hb-lost-ratio 2}})
        n1 "node-1"
        f0 {:f0 {:arity 2}}
        f0f1 {:f0 {:arity 2} :f1 {:arity 1}}
        get-worker-nodes   #(-> n :node-state :worker-nodes deref)
        get-worker-node    #(-> (get-worker-nodes) (get %))
        get-worker-node-hb #(-> (get-worker-node %) :hb-at)
        get-worker-node-functions #(-> (get-worker-node %) :functions)]
    (t/is (empty? (get-worker-nodes)))

    (binding [n/*now-ms* #(do 1000)]
      (n/on-heartbeat! n {:node-id n1 :functions f0}))
    (t/is (= 1000 (get-worker-node-hb n1)))
    (t/is (= f0 (get-worker-node-functions n1)))

    (binding [n/*now-ms* #(do 3000)]
      (n/on-heartbeat! n {:node-id n1 :functions f0f1}))
    (t/is (= 3000 (get-worker-node-hb n1)))
    (t/is (= f0f1 (get-worker-node-functions n1)))

    ;; heartbeat lost
    (binding [n/*now-ms* #(do 4000)]
      (t/is (not (n/heartbeat-lost? n (get-worker-node n1))))

      (let [prev (get-worker-nodes)]
        (n/remove-dead-workers! n)
        (t/is (= prev (get-worker-nodes)))))
    (binding [n/*now-ms* #(do 5000)]
      (t/is (not (n/heartbeat-lost? n (get-worker-node n1)))))
    (binding [n/*now-ms* #(do 5001)]
      (t/is (n/heartbeat-lost? n (get-worker-node n1)))
      (let [prev (get-worker-nodes)]
        (n/remove-dead-workers! n)
        (t/is (not= prev (get-worker-nodes)))
        (t/is (nil? (get-worker-node n1)))))))

(t/deftest add-function
  (let [n (n/make-node {})
        get-node-functions      #(-> n :node-state :functions deref)
        get-node-function-arity #(-> (get-node-functions) (get %) :arity)
        f-add (fn [[a b]] (+ a b))
        f-add-1 (fn [[a b] opt] [(+ a b) opt])]
    (n/add-function n "add" f-add)
    (n/add-function n "add-1" f-add-1)
    (t/is (= 1 (get-node-function-arity "add")))
    (t/is (= 2 (get-node-function-arity "add-1")))

    (t/is (= 3 (n/call-function n "add" [1 2])))
    (let [[r _opt] (n/call-function n "add-1" [1 2])]
      (t/is (= r 3)))

    (let [[r {:keys [in-c out-c]}] (n/call-function n "add-1" [1 2])]
      (t/is (= r 3))
      (t/is (some? in-c))
      (t/is (some? out-c)))

    (let [in-c0 (chan 1) out-c0 (chan 1) req-meta0 {:hb-interval-ms 10}
          [r {:keys [in-c out-c req-meta]}]
          (n/call-function n "add-1" [1 2] {:in-c in-c0 :out-c out-c0
                                            :req-meta req-meta0})]
      (t/is (= r 3))
      (t/is (= in-c in-c0))
      (t/is (= out-c out-c0))
      (t/is (= req-meta req-meta0)))))

(t/deftest calc-heartbeat-msg-&-find-worker-nodes
  (let [n (n/make-node {})
        node-ids ["n1" "n2" "n3" "n4"]
        nodes (->> node-ids
                   (map (fn [i] [i (n/make-node {:node-id i})]))
                   (into {}))
        f-add (fn [[a b]] (+ a b))
        f-sub (fn [[a b]] (- a b))
        f-add-1 (fn [[a b] opt] [(+ a b) opt])]
    ;; n1: add
    (n/add-function (nodes "n1") "add" f-add)
    ;; n2: add-1
    (n/add-function (nodes "n2") "add-1" f-add-1)
    ;; n3: add, add-1
    (n/add-function (nodes "n3") "add" f-add)
    (n/add-function (nodes "n3") "add-1" f-add-1)
    ;; n4: sub
    (n/add-function (nodes "n4") "sub" f-sub)

    (doseq [i node-ids]
      (n/on-heartbeat! n (assoc (n/calc-heartbeat-msg (nodes i)) :node-id i)))

    (doseq [i node-ids]
      (t/is (not (nil? (n/get-worker-node n i)))))

    (t/is (= "n4" (n/select-worker-node n {:function-name "sub"})))

    (t/is (->> (range 100)
               (every? (fn [_] (#{"n1" "n3"} (n/select-worker-node n {:function-name "add"}))))))

    (t/is (->> (range 100)
               (every? (fn [_] (#{"n2" "n3"} (n/select-worker-node n {:function-name "add-1"}))))))

    (t/is (nil? (n/select-worker-node n {:function-name "fake-42"})))))

(t/deftest call-function
  (let [n (n/make-node {})
        get-node-functions      #(-> n :node-state :functions deref)
        get-node-function-arity #(-> (get-node-functions) (get %) :arity)
        f-add (fn [[a b]] (+ a b))
        f-add-1 (fn [[a b] opt] [(+ a b) opt])]
    (n/add-function n "add" f-add)
    (n/add-function n "add-1" f-add-1)
    (t/is (= 1 (get-node-function-arity "add")))
    (t/is (= 2 (get-node-function-arity "add-1")))

    (t/is (= 3 (n/call-function n "add" [1 2])))
    (let [[r _opt] (n/call-function n "add-1" [1 2])]
      (t/is (= r 3)))

    (let [[r _opt] (n/call-function n "add-1" [1 2] :send (fn [_] "customized"))]
      (t/is (= r 3)))))

(defn- check-node-running-requests-empty [n & {:keys [waiting-ms]}]
  ;; cleanup will run after the timeout cancellation
  (Thread/sleep (or waiting-ms 50))
  (t/is (empty? @(-> n :node-state :running-requests))
        "should cleanup running-requests"))

(t/deftest submit-function-call-simple
  (let [n (n/make-node {})]
    (n/add-function n "add" s/f-add)
    (n/add-function n "sub" s/f-sub)
    (n/add-function n "throw" s/f-throw)
    (n/add-function n "long-running" s/f-delayed-sum-with-hb)

    (doseq [_ (range 20)]
      (let [r (promise)]
        (check-node-running-requests-empty n :waiting-ms 0)
        (t/is (= 3 @(n/submit-function-call
                     n "add" [1 2] {:callbacks {:on-res #(deliver r %)}})))
        (t/is (= 3 (deref r 100 :timeout)) "handles result callback")
        (check-node-running-requests-empty n :waiting-ms 0))

      (let [r (promise) e (ex-info "" {})]
        (t/is (empty? @(-> n :node-state :running-requests)))
        (t/is (thrown? ExecutionException
                       @(n/submit-function-call
                         n "throw" e {:callbacks {:on-exception #(deliver r %)}})))
        (t/is (= e (deref r 100 :timeout)) "handles exception callback")
        (check-node-running-requests-empty n :waiting-ms 0)))))

(t/deftest submit-function-call-calculator
  (let [n (n/make-node {})]
    (n/add-function n "calculator" s/f-calculator)

    (let [{:keys [in-c running-future]}
          (n/submit-function-call*
           n "calculator" nil
           {:req-meta {:hb-interval-ms 3000
                       :hb-lost-ratio 2.5
                       :timeout-ms 3600000}})]
      (>!! in-c {:type :xnfun/cancel})
      (t/is (thrown? CancellationException @running-future))
      (check-node-running-requests-empty n :waiting-ms 0))

    (let [out-c0 (chan 1)
          {:keys [out-c in-c running-future]}
          (n/submit-function-call*
           n "calculator" nil
           {:out-c out-c0
            :req-meta {:hb-interval-ms 3000
                       :hb-lost-ratio 2.5
                       :timeout-ms 3600000}})

          check-calc (fn [exp r]
                       (>!! in-c {:type :msg :data exp})
                       (t/is (= {:type :msg :data r}
                                (deref (let [p (promise)]
                                         (go-loop [d (<!! out-c)]
                                           (when d
                                             (if (not= :msg (:type d))
                                               (recur (<!! out-c))
                                               (deliver p d))))
                                         p)
                                       100 :timeout))))]
      (check-calc '(+ 1 2) 3)
      (check-calc '(* 2 4) 8)
      (check-calc '(* 7 (+ 3 3)) 42)

      (>!! in-c {:type :xnfun/cancel})
      (t/is (thrown? CancellationException @running-future))
      (check-node-running-requests-empty n :waiting-ms 0))))

(t/deftest submit-function-call*
  (let [n (n/make-node {})]
    (n/add-function
     n "echo"
     (fn [params {:keys [in-c out-c]
                  {:keys [hb-interval-ms]} :req-meta}]
       (let [finish (promise)]
         (future
           (loop []
             (when (= :timeout (deref finish hb-interval-ms :timeout)) (recur))))
         (go-loop [{:as d :keys [type]} (<! in-c)]
           (when d
             (cond
               (= type :xnfun/cancel)
               (log/infof "quit on cancel")

               (= type :resp)
               (deliver finish params)

               :else
               (do (put! out-c d)
                   (recur (<! in-c))))))
         @finish)))

    (let [params "hello world"

          {:keys [running-future in-c out-c]}
          (n/submit-function-call*
           n "echo" params {:req-meta {:hb-interval-ms 20
                                       :hb-lost-ratio 10}})

          d {:type :msg :data "hello"}]
      (>!! in-c d)
      (t/is (= d (deref (future (<!! out-c)) 100 :timeout)))

      (>!! in-c {:type :resp})
      (t/is (= params (deref running-future 100 :timeout))))))

(t/deftest submit-function-call-timeout
  (let [n (n/make-node {})]
    (n/add-function n "delayed-add" s/f-delayed-add)

    (t/is (= 3 @(n/submit-function-call
                 n "delayed-add" [1 2 90] {:req-meta {:timeout-ms 100}}))
          "should not timeout: 90 < 100")
    (t/is (empty? @(-> n :node-state :running-requests))
          "should cleanup running-requests")

    (t/is (thrown? CancellationException
                   @(n/submit-function-call
                     n "delayed-add" [1 2 100] {:req-meta {:timeout-ms 90}}))
          "should timeout: 100 > 90")

    (check-node-running-requests-empty n)))

(t/deftest submit-function-call-hb-lost
  (let [n (n/make-node {})]
    (n/add-function n "delayed-sum-with-hb" s/f-delayed-sum-with-hb)

    (check-node-running-requests-empty n :waiting-ms 0)
    (doseq [_ (range 10)]
      (let [nums (range 10)
            opts {:add-delay-ms 20 :hb-with-msg false}
            ;; 10 * 1.5 < 20: hb-lost
            req-meta {:hb-interval-ms 10 :hb-lost-ratio 1.5}]
        (t/is (thrown? CancellationException
                       @(n/submit-function-call
                         n "delayed-sum-with-hb" [nums opts]
                         {:req-meta req-meta}))
              "should timeout with :hb-lost")))

    (doseq [_ (range 1)]
      (let [nums (range 10)
            opts {:add-delay-ms 20 :hb-with-msg false}
            ;; 10 * 1.5 < 20: hb-lost
            req-meta {:hb-interval-ms 10 :hb-lost-ratio 1.5}]
        (t/is (thrown? CancellationException
                       @(n/submit-function-call
                         n "delayed-sum-with-hb" [nums opts]
                         {:req-meta req-meta}))
              "should timeout with :timeout")))
    (check-node-running-requests-empty n)

    (doseq [_ (range 1)]
      (let [nums (range 10)
            opts {:add-delay-ms 30 :hb-with-msg false}
            ;; hb-lost: 20 * 2.5 > 30
            ;; timeout: 500    > 30 * 10
            req-meta {:hb-interval-ms 40 :hb-lost-ratio 2.5 :timeout-ms 500}]
        (t/is (= 45 @(n/submit-function-call
                      n "delayed-sum-with-hb" [nums opts]
                      {:req-meta req-meta}))
              "should not timeout")))
    (check-node-running-requests-empty n)

    (let [nums (range 10)
          opts {:add-delay-ms 30 :hb-with-msg false}
            ;; hb-lost: 20 * 2.5 > 30
            ;; timeout: 500      > 30 * 10
          req-meta {:hb-interval-ms 40 :hb-lost-ratio 2.5 :timeout-ms 500}]
      (t/is (= 45 @(n/submit-function-call
                    n "delayed-sum-with-hb" [nums opts]
                    {:req-meta req-meta}))
            "should not timeout"))
    (check-node-running-requests-empty n)))

(t/deftest submit-wait-for-function-call
  (let [n (n/make-node {})]
    (let [req-id (str (random-uuid))
          {:keys [res-promise]}
          (n/submit-wait-for-function-call n {:req-id req-id})]
      (n/fullfill-wait-function-call n req-id 10)
      (t/is 10 (deref res-promise 100 :timeout)))
    (t/is (empty? @(-> n :node-state :waiting-promises)))

    (let [req-id (str (random-uuid))
          timeout-ms 20

          {:keys [res-promise _in-c _out-c]}
          (n/submit-wait-for-function-call n {:req-id req-id
                                              :req-meta {:timeout-ms timeout-ms}})
          r (deref res-promise 100 :timeout)]
      (t/is (= :err-rpc (:status r))))
    (t/is (empty? @(-> n :node-state :waiting-promises)))

    (let [req-id (str (random-uuid))
          timeout-ms 20

          {:keys [res-promise]}
          (n/submit-wait-for-function-call n {:req-id req-id
                                              :req-meta {:timeout-ms timeout-ms
                                                         :hb-interval-ms 20
                                                         :hb-lost-ratio 2.5}})
          r (deref res-promise 100 :timeout)]
      (t/is (= :err-rpc (:status r))))))

(t/deftest submit-wait-for-function-call-heartbeat-hb-lost
  (let [n (n/make-node {})]
    (doseq [_ (range 10)]
      (let [req-id (str (random-uuid))
            timeout-ms 2000

            hb-interval-ms 20
            hb-lost-ratio 2.5

            {:keys [res-promise]}
            (->> {:req-id req-id
                  :req-meta {:timeout-ms timeout-ms
                             :hb-interval-ms hb-interval-ms
                             :hb-lost-ratio hb-lost-ratio}}
                 (n/submit-wait-for-function-call n))]
        (Thread/sleep (+ 20 (* hb-lost-ratio hb-interval-ms)))
        (n/fullfill-wait-function-call n req-id 10)
        (let [r (deref res-promise 100 :timeout)]
          (t/is (= [:err-rpc :hb-lost] [(:status r) (:reason r)])
                "Should be timeouted because of heartbeat lost."))))))

(t/deftest submit-wait-for-function-call-heartbeat-hb-ok
  (let [n (n/make-node {})]
    (doseq [_ (range 10)]
      (let [req-id (str (random-uuid))
            timeout-ms 2000
            expected-hb-interval-ms 10
            expected-hb-lost-ratio 2.5
            deliver-delay-ms 30
            hb-interval-ms 10   ; < (* 2.5 10), so it should not trigger hb lost

            {:keys [res-promise hb]}
            (->> {:req-id req-id
                  :req-meta {:timeout-ms timeout-ms
                             :hb-interval-ms expected-hb-interval-ms
                             :hb-lost-ratio expected-hb-lost-ratio}}
                 (n/submit-wait-for-function-call n))]
        (future (doseq [_ (range 10)] (Thread/sleep hb-interval-ms) (hb)))
        (Thread/sleep deliver-delay-ms)
        (n/fullfill-wait-function-call n req-id 42)
        (t/is (= 42 (deref res-promise 100 :timeout)))))))
