(ns lotuc.remote.node.registry-test
  (:require [clojure.test :refer [deftest is testing]]
            [lotuc.remote.node.registry :as sut]))

(defn create-hb []
  (let [v (atom 0)]
    (fn [] (swap! v inc))))

(deftest heartbeat
  (testing "heartbeat time"
    (is (binding [sut/*current-time-millis* (create-hb)]
          (-> {}
              (sut/on-heartbeat "node-0" {})
              :heartbeat
              (get "node-0")
              ;; Assume execution time is less than 100ms
              (partial = 1))))

    (is (binding [sut/*current-time-millis* (create-hb)]
          (= {:methods {"node-0" #{} "node-1" #{"f0" "f1"}},
              :heartbeat {"node-0" 1 "node-1" 2}}
             (-> {}
                 (sut/on-heartbeat "node-0")
                 (sut/on-heartbeat "node-1" {:methods ["f0" "f1"]}))))))

  (testing "methods"
    (testing "do not empty methods when no methods given"
      (is (= #{"f0"}
             (-> {}
                 (sut/on-heartbeat "node-0" {:methods ["f0"]})
                 (sut/on-heartbeat "node-0")
                 :methods (get "node-0")))))

    (testing "overwrite methods when given"
      (is (= #{"f0" "f1"}
             (-> {}
                 (sut/on-heartbeat "node-0" {:methods ["f0"]})
                 (sut/on-heartbeat "node-0" {:methods ["f0" "f1"]})
                 :methods (get "node-0"))))

      ;; empty the methods.
      (is (= #{}
             (-> {}
                 (sut/on-heartbeat "node-0" {:heartbeat-at 42 :methods ["f0"]})
                 (sut/on-heartbeat "node-0" {:heartbeat-at 43 :methods []})
                 :methods (get "node-0"))))))

  (let [start-at 1
        hb-lost-ratio 2
        hb-interval 1
        ;; heartbeat will lost after this interval
        hb-lost-cost (* hb-lost-ratio hb-interval)]
    (binding [sut/*current-time-millis* #(identity start-at)
              sut/*heartbeat-lost-ratio* hb-lost-ratio
              sut/*heartbeat-interval-in-millis* hb-interval]
      (let [nodes (-> {} (sut/on-heartbeat "node-0"))]
        (doseq [i (range (+ 2 hb-lost-cost))]
          (let [t (+ start-at i)
                lost (> (- t start-at) hb-lost-cost)]
            (binding [sut/*current-time-millis* #(identity t)]

              (testing "check heartbeat"
                (is (= lost (sut/check-is-heartbeat-lost nodes "node-0"))
                    (str "heartbeat at:" start-at "; "
                         "current time:" t)))

              (testing "remove dead nodes"
                (let [nodes (sut/remove-dead-nodes nodes)
                      hb-node-ids (-> nodes :heartbeat keys set)
                      method-node-ids (-> nodes :method keys set)]
                  (if lost
                    (is (and (nil? (get hb-node-ids "node-0"))
                             (nil? (get method-node-ids "node-0"))))
                    (is (get hb-node-ids "node-0"))))))))))))

(deftest find-node
  (let [nodes
        (-> {}
            (sut/on-heartbeat "node-0" {:heartbeat-at 42 :methods []})
            (sut/on-heartbeat "node-1" {:heartbeat-at 43 :methods ["f0"]})
            (sut/on-heartbeat "node-2" {:heartbeat-at 44 :methods ["f1"]})
            (sut/on-heartbeat "node-3" {:heartbeat-at 44 :methods ["f2"]})
            (sut/on-heartbeat "node-4" {:heartbeat-at 45 :methods ["f0" "f1"]}))

        node-ids (-> nodes :heartbeat keys set)
        every-not-nil? (partial every? (complement nil?))
        find-node-count 10]
    (testing "find with no condition"
      (is (->> (range find-node-count)
               (map (fn [_] (sut/find-node nodes)))
               (map node-ids)
               every-not-nil?)))

    (testing "find with given node-id"
      (is (->> node-ids
               (map #(= % (sut/find-node nodes {:node-id %})))
               every-not-nil?)))

    (testing "find with given method"
      (is (->> (range find-node-count)
               (map (fn [_] (= "node-3" (sut/find-node nodes {:method "f2"}))))
               every-not-nil?)))

    (testing "find with given method - random node selection"
      (is (->> (range find-node-count)
               (map (fn [_] (#{"node-2" "node-4"} (sut/find-node nodes {:method "f1"}))))
               every-not-nil?)))

    (testing "find with given node-id and method"
      (is (->> (range find-node-count)
               (map (fn [_] (= "node-2" (sut/find-node nodes {:node-id "node-2" :method "f1"}))))
               every-not-nil?))

      (is (->> (range find-node-count)
               (map (fn [_] (= "node-1" (sut/find-node nodes {:node-id "node-2" :method "f1"}))))
               (every? false?))))))

(deftest method
  (let [add (fn [[a b]] (+ a b))
        methods (-> {}
                    (sut/add-method "add" add)
                    (sut/add-method "add2" add))]
    (testing (is (= 5 (sut/call-method methods "add" [2 3]))))
    (testing (is (= 11 (sut/call-method methods "add2" [2 9]))))))
