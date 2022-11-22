(ns lotuc.remote.node.registry
  (:require [clojure.tools.logging :as log]
            [lotuc.remote.utils :refer [max-arity]])
  (:import (java.time Instant)))

(def ^:dynamic *heartbeat-interval-in-millis* 3000)
(def ^:dynamic *heartbeat-lost-ratio* 2.0)
(def ^:dynamic *current-time-millis* #(System/currentTimeMillis))

(defn on-heartbeat
  [nodes node-id & {:keys [methods]}]
  (let [hb-at     (*current-time-millis*)
        prev-hb   (-> nodes :heartbeat (get node-id))
        node-hb   {node-id hb-at}
        node-mths (if methods
                    ;; if methods given, update it
                    {node-id (set methods)}
                    ;; if it's an old node, keep the methods
                    ;; else initialize it with empty method set
                    (if (nil? prev-hb) {node-id #{}} {}))]
    (when (nil? prev-hb)
      (log/info "new node heartbeat" node-id))
    (-> nodes
        (update :methods (fn [mths] (merge (or mths {}) node-mths)))
        (update :heartbeat (fn [hbs] (merge (or hbs {}) node-hb))))))

(defn check-is-heartbeat-lost [nodes node-id]
  (if-let [heartbeat-at (-> nodes :heartbeat (get node-id))]
    (let [cur-millis (*current-time-millis*)
          since-last (- cur-millis heartbeat-at)
          heartbeat-lost-time (* *heartbeat-lost-ratio* *heartbeat-interval-in-millis*)
          lost-heartbeat (> since-last heartbeat-lost-time)]
      (when lost-heartbeat
        (log/warn "node" node-id
                  "lost heartbeat ("
                  cur-millis "-" heartbeat-at ">" heartbeat-lost-time
                  ") last heartbeat at"
                  (str (Instant/ofEpochMilli heartbeat-at))))
      lost-heartbeat)
    (do (log/warn "node" node-id "had no heartbeat") true)))

(defn remove-dead-nodes
  ([nodes]
   (let [dead-nodes
         (->> (:heartbeat nodes) keys
              (filter (partial check-is-heartbeat-lost nodes)))]
     (remove-dead-nodes dead-nodes nodes)))

  ([dead-nodes nodes]
   (let [dead-nodes (set dead-nodes)
         is-alive (complement dead-nodes)]
     (if (empty? dead-nodes)
       nodes
       (let [methods (:methods nodes)
             heartbeats (:heartbeat nodes)
             is-alive-by-key (partial filter (fn [[k _]] (is-alive k)))]
         (log/warn "remove dead nodes" dead-nodes)
         (-> nodes
             (assoc :methods
                    (->> methods is-alive-by-key (into {})))
             (assoc :heartbeat
                    (->> heartbeats is-alive-by-key (into {})))))))))

(defn find-node [nodes & {:keys [node-id method]}]
  (let [heartbeats (nodes :heartbeat)
        methods (nodes :methods)]
    (if node-id
      (let [found (heartbeats node-id)
            match-method (or (nil? method) (methods method))]
        (when found
          (when-not match-method
            (log/warn "node" node-id "does not support"
                      method " (" methods ")"))
          (when match-method node-id)))
      (let [node-ids (keys heartbeats)
            node-ids (if (nil? method)
                       node-ids
                       (->> node-ids
                            (filter #(get (methods %1) method))))
            found (not-empty node-ids)]
        (when-not found
          (log/warn "node with method" method "not found"))
        (when found
          ;; TODO: more strategies?
          ;; random strategy for node selection by method.
          (rand-nth node-ids))))))

(defn add-method [methods method func]
  (let [func-arity (max-arity func)
        prev (get methods method)]
    (when-not (#{1 2} func-arity)
      (log/warn "invalid rpc method" method)
      (throw (ex-info "invalid rpc method (arity not match)"
                      {:method method :func func})))
    (when-not (nil? prev)
      (log/warn "overwrite method" method "with" func ", prev is" prev))

    (assoc methods method {:func func :arity func-arity})))

(defn call-method [methods method params & {:keys [send]}]
  (let [{:keys [func arity]} (get methods method)
        _ (when-not func (throw (ex-info "method not found"
                                         {:method method
                                          :params params
                                          :methods methods})))]
    (if (= arity 2)
      (apply func [params {:send (or send identity)}])
      (apply func [params]))))

(defmacro ^:private run-ignore-error [func]
  (let [e (gensym)]
    `(try (~func) (catch Exception ~e (log/warn ~e "error cleanup")))))

(defn run-funcs-ignore-error [funcs]
  (doseq [f funcs] (run-ignore-error f)))
