(ns lotuc.remote.mqtt.topics
  (:require [clojure.string :as str]))

(def ^:dynamic *topic-prefix* "")
(def ^:private registry-topic-prefix "/v0/registry")

(defn registry-topic-create
  "the register action requires"
  [{:keys [action node-id] :as params}]
  (let [thr #(throw (ex-info %1 {:params params}))]
    (case action
      nil
      (str *topic-prefix* registry-topic-prefix "/#")

      :heartbeat
      (do
        (when-not node-id (thr "node-id not given"))
        (str *topic-prefix* registry-topic-prefix "/hb/" node-id))

      (thr "action not supported"))))

(defn registry-topic-parse
  "only parses the concrete topic"
  [topic]
  (let [prefix (str *topic-prefix* registry-topic-prefix "/")
        thr #(throw (ex-info %1 {:topic topic}))]
    (when-not (str/starts-with? topic prefix) (thr "illegal topic"))

    (let [ss (str/split (subs topic (count prefix)) #"/")
          ss-len (count ss)]
      (case ss-len
        2
        ;; /<action>/<node-id>
        (let [[action-name node-id] ss
              action (case action-name
                       "hb" :heartbeat
                       (thr (str "illegal action - " action-name)))]
          (cond-> {}
            action
            (assoc :action action)

            (and action (not= node-id "#"))
            (assoc :node-id node-id)))

        1
        (let [[v] ss]
          (when-not (= v "#") (thr "illegal topic"))
          {})

        (thr "illegal topic")))))

(def ^:private rpc-topic-prefix "/v0/rpc")
;; <rpc-topic-prefix>/<action>/<node-id>/<reserved:1>/<req-id>

(defn rpc-topic-create
  "action: :req :resp"
  [{:keys [action node-id req-id] :as params}]
  (let [thr #(throw (ex-info %1 {:params params}))
        action-name (case action
                      :req "req"
                      :resp "resp"
                      (thr "illegal action"))]
    (->> (cond
           (and (not (nil? node-id)) (not (nil? req-id)))
           [action-name node-id "1" req-id]

           (not (nil? node-id))
           [action-name node-id "1" "#"]

           :else
           [action-name "#"])
         (str/join "/")
         (str *topic-prefix* rpc-topic-prefix "/"))))

(defn rpc-topic-parse
  "only parses the concrete topic"
  [topic]
  (let [prefix (str *topic-prefix* rpc-topic-prefix)
        thr #(throw (ex-info (str "invalid topic: " %1) {:topic topic}))]
    (when-not (str/starts-with? topic prefix)
      (thr (str "prefix not match " prefix)))
    (let [ss (str/split (subs topic (inc (count prefix))) #"/")
          ss-len (count ss)
          action (case (get ss 0)
                   "req" :req
                   "resp" :resp
                   (thr "illegal topic - action not found"))]
      (case ss-len
        4
        (let [[_ node-id reserved req-id] ss]
          (when-not (= reserved "1") (thr "illegal topic - unkown reserved field"))
          (when (= node-id "#") (thr "illegal topic - node-id"))
          (cond-> {:action action}
            (not= node-id "+")        (assoc :node-id node-id)
            (not (#{"+" "#"} req-id)) (assoc :req-id req-id)))

        3
        (let [[_ node-id v] ss]
          (when-not (= v "#") (thr (str "illegal topic")))
          (when (= node-id "#") (thr "illegal topic - node-id"))
          (cond-> {:action action}
            (not= node-id "+")  (assoc :node-id node-id)))

        2
        (let [[_ v] ss]
          (when-not (= v "#") (thr (str "illegal topic")))
          {:action action})

        ;; default
        (thr "illegal topic")))))
