(ns lotuc.xnfun.rpc.mqtt-link
  (:require [lotuc.xnfun.rpc.link :refer [XNFunLink]]
            [lotuc.xnfun.mqtt.client :as mqtt]
            [lotuc.xnfun.mqtt.pubsub :refer [pub-edn sub-edn unsub]]
            [lotuc.xnfun.rpc.mqtt-topics :as topics]
            [clojure.tools.logging :as log]))

(defn- add-subscription [topic handler client subscriptions]
  (let [sub-fn*
        (delay
          (fn [topic data]
            (let [fs (map #(future (% topic data)) (:handlers @subscriptions))]
              (doseq [f fs]
                (try
                  @f
                  (catch Exception e
                    (log/infof e "error handling %s" topic)))))))

        sub-fn
        (-> (swap!
             subscriptions
             (fn [m]
               (if-let [{:keys [handlers sub-fn]} (m topic)]
                 (assoc m topic {:handlers (conj handlers handler)
                                 :sub-fn sub-fn})
                 (assoc m topic {:handlers #{handler}
                                 :sub-fn sub-fn*}))))
            (get topic))]

    (when-not (realized? sub-fn)
      (sub-edn client topic @sub-fn))))

(defn- new-mqtt-link* [{:keys [topic-prefix client subscriptions
                               create-send-data
                               create-sub-data]}]
  (reify XNFunLink
    (send-msg [{:as node :keys [node-id]} {:as msg :keys [data]}]
      (let [[topic data] (create-send-data msg)]
        (pub-edn client topic data)))

    (sub-msg [{:as node} {:as handler :keys [types handle-fn]}]
      ;; create subscription to
      (let [topic-handlers
            (->> (set types)
                 (map (fn [typ] (create-sub-data handler typ))))

            topics (map first topic-handlers)

            ;; if hanlders of given topic are empty, do unsubscribe to mqtt
            do-unsub
            (fn []
              (let [[_ new]
                    (swap-vals!
                     subscriptions
                     #(reduce
                       (fn [m topic]
                         (let [{:keys [handlers sub-fn]} (m topic)
                               handlers (disj handlers handler)]
                           (if (empty? handlers)
                             (dissoc m topic)
                             (assoc m topic {:sub-fn sub-fn :handlers handlers}))))
                       %
                       topics))]
                (doseq [topic topics]
                  (when (nil? (get new topic))
                    (try (unsub client topic)
                         (catch Exception _))))))]

        (try
          (doseq [[topic handler] topic-handlers]
            (add-subscription topic handler client subscriptions))
          do-unsub
          (catch Exception e
            (do-unsub)
            (throw e)))))))

(defn new-mqtt-link [node]
  (let [{:keys [node-id node-options]} node

        {:keys [mqtt-topic-prefix mqtt-config]}
        (:link node-options)

        hb-topic
        (-> {:node-id node-id :action :heartbeat}
            topics/registry-topic-create)

        hb-sub-topic
        (-> {:action :heartbeat}
            topics/registry-topic-create)

        create-req-topic
        (fn [req-id]
          (-> {:action :req :node-id node-id :req-id req-id}
              topics/rpc-topic-create))

        req-sub-topic
        (-> {:action :req :node-id node-id}
            topics/rpc-topic-create)

        create-resp-topic
        (fn [{:as data :keys [req-id]}]
          (-> {:action :resp :node-id node-id :req-id req-id}
              topics/rpc-topic-create))

        create-resp-sub-topic
        (fn [{:as handler}]
          (let [req-id (:req-id (meta handler))]
            (when-not req-id
              (->> {:handler handler}
                   (ex-info "error register subscription handler: no req-id specified")
                   throw))
            (-> {:action :resp :node-id node-id :req-id req-id}
                topics/rpc-topic-create)))

        ;; topic to handler functions
        subscriptions (atom {})]
    (-> {:topic-prefix
         mqtt-topic-prefix

         :subscriptions
         subscriptions

         :create-send-data
         (fn [{:as msg :keys [typ data]}]
           (case typ
             :hb   [hb-topic data]
             :req  [(create-req-topic (:req-id data))
                    (dissoc data :req-id)]
             :resp [(create-resp-topic (:req-id data))
                    (dissoc data :req-id)]))

         :create-sub-data
         (fn [{:as handler} typ]
           (case typ
             :hb
             [hb-sub-topic
              (fn [topic data]
                (let [node-id (:node-id (topics/rpc-topic-parse topic))]
                  {:node-id node-id :data data}))]

             :req
             [req-sub-topic
              (fn [topic data]
                (let [{:keys [node-id req-id]}
                      (topics/rpc-topic-parse topic)]
                  {:node-id node-id :req-id req-id :data data}))]

             :resp
             [(create-resp-sub-topic handler)
              (fn [topic data]
                (let [{:keys [node-id req-id]}
                      (topics/rpc-topic-parse topic)]
                  {:node-id node-id :req-id req-id :data data}))]))

         :client
         (mqtt/make-client mqtt-config)}

        new-mqtt-link*)))
