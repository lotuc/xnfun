(ns lotuc.xnfun.mqtt.topics-test
  (:require
   [clojure.test :as t]
   [lotuc.xnfun.rpc.mqtt-topics :as sut]))

(t/deftest registry-topic
  (binding [sut/*topic-prefix* "/hello/world"]
    (let [hb-topic {:action :heartbeat :node-id "node-42"}
          hb-topic1 {}

          topics [hb-topic hb-topic1]]
      (t/is (= "/hello/world/v0/registry/hb/node-42" (sut/registry-topic-create hb-topic)))
      (t/is (= "/hello/world/v0/registry/#" (sut/registry-topic-create hb-topic1)))

      (doseq [t topics]
        (t/is (= t (-> t sut/registry-topic-create sut/registry-topic-parse))
              (str t))))))

(t/deftest rpc-topic
  (binding [sut/*topic-prefix* "/hello/world"]
    (let [req-topic0 {:action :req}
          req-topic1 {:action :req :node-id "node-42"}
          req-topic2 {:action :req :node-id "node-42" :req-id "req-42"}

          resp-topic0 {:action :resp}
          resp-topic1 {:action :resp :node-id "node-42"}
          resp-topic2 {:action :resp :node-id "node-42" :req-id "req-42"}

          topics [req-topic0 req-topic1 req-topic2
                  resp-topic0 resp-topic1 resp-topic2]]
      (t/is (= "/hello/world/v0/rpc/req/#" (sut/rpc-topic-create req-topic0)))
      (t/is (= "/hello/world/v0/rpc/req/node-42/1/#" (sut/rpc-topic-create req-topic1)))
      (t/is (= "/hello/world/v0/rpc/req/node-42/1/req-42" (sut/rpc-topic-create req-topic2)))

      (t/is (= "/hello/world/v0/rpc/resp/#" (sut/rpc-topic-create resp-topic0)))
      (t/is (= "/hello/world/v0/rpc/resp/node-42/1/#" (sut/rpc-topic-create resp-topic1)))
      (t/is (= "/hello/world/v0/rpc/resp/node-42/1/req-42" (sut/rpc-topic-create resp-topic2)))

      (doseq [t topics]
        (t/is (= t (-> t sut/rpc-topic-create sut/rpc-topic-parse))
              (str t))))))
