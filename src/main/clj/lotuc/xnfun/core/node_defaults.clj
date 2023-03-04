(ns lotuc.xnfun.core.node-defaults)

(defn make-req-meta [req-meta]
  (let [req-id         (or (:req-id req-meta) (str (random-uuid)))
        hb-interval-ms (:hb-interval-ms req-meta)
        hb-lost-ratio  (if hb-interval-ms
                         (or (:hb-lost-ratio req-meta) 2.5)
                         nil)]
    (cond-> (or req-meta {})
      req-id         (assoc :req-id req-id)
      hb-interval-ms (assoc :hb-interval-ms hb-interval-ms)
      hb-lost-ratio  (assoc :hb-lost-ratio hb-lost-ratio))))

(defn- make-node-hb-options
  [options]
  (-> (or options {})
      (update :hb-interval-ms #(or % 30000))
      (update :hb-lost-ratio  #(or % 2.5))))

(defn- make-node-transport-options
  "We use mqtt as the default communication transport."
  [node-id transport]
  (or transport
      {:xnfun/module 'xnfun.mqtt
       :mqtt-topic-prefix ""
       :mqtt-config
       {:broker "tcp://127.0.0.1:1883"
        :client-id node-id
        :connect-options {:max-in-flight 1000
                          :auto-reconnect true}}}))

(defn make-node-options
  "Options of the node.

  Options contains:

  - `hb-options`:
      - `:hb-interval-ms`: Heartbeat interval for this node.
      - `:hb-lost-ratio`: Consider this node to be heartbeat lost in
        (* hb-lost-ratio hb-interval-ms) when no heartbeat occurs.
  - `:transport`: The transport this node would make to connect to other nodes.

  Arguments:

  - `node-options`: default options."
  [node-id node-options]
  (-> (or node-options {})
      (update :hb-options #(make-node-hb-options %))
      (update :transport #(make-node-transport-options node-id %))))
