(ns lotuc.xnfun.core.node
  "Node abstraction for xnfun.

  - You can start a transport (ex. MQTT transport) for connecting with other nodes.
  - You can register (capability) functions to current node.
  - You can submit function call to current node (call you registered functions).
  - You can register a promise to current node and maybe fullfill it later.
  - You can do call to other node through current node via your established
    transport

  When caller call from one node to other node's function (callee), this will

  - Submit a promise in current node waiting for result.
  - Submit a future (representing the remote computation, the callee) in remote
    node.
  - Create a bi-directional communication channel between caller and callee.

  **Node**

  - [[make-node]]
  - [[node-info]]
  - [[node-transport]] [[node-futures]] [[node-promises]] [[node-remote-nodes]]
  - [[select-remote-node]]

  - [[start-node-transport!]] [[stop-node-transport!]]
  - [[on-heartbeat!]]  [[remove-dead-workers!]]
  - [[start-heartbeat!]] [[stop-heartbeat!]]
  - [[start-heartbeat-listener!]] [[stop-heartbeat-listener!]]

  **Capabilities**

  - [[add-function!]] [[call-function!]]

  **Promises**

  - [[fullfill-promise!]] [[submit-promise!]]

  **Function call**

  - [[submit-call!]]

  **Remote function call**

  - [[submit-remote-call!]]
  - [[serve-remote-call!]] [[stop-serve-remote-call!]]
  "
  (:require
   [clojure.core.async :refer [<! <!! >!! chan close! dropping-buffer go-loop]]
   [clojure.tools.logging :as log]
   [lotuc.xnfun.core.utils :refer [max-arity swap!-swap-in-delayed!
                                   *now-ms* *run-at* *periodic-run*]]
   [lotuc.xnfun.core.transport-mqtt :refer [make-mqtt-transport]]
   [lotuc.xnfun.core.transport :as l]))

(defmacro ^:private ensure-node-transport [node]
  (let [transport (gensym)]
    `(let [~transport @(-> ~node :node-state :local :transport)
           ~transport (and ~transport @~transport)]
       (when (nil? ~transport) (throw (ex-info "transport not created" {:node ~node})))
       (when (.closed? ~transport) (throw (ex-info "transport closed" {:node ~node})))
       ~transport)))

(defmacro ^:private send-msg
  ([node msg]
   `(l/send-msg (ensure-node-transport ~node) ~msg))
  ([node msg msg-meta]
   `(l/send-msg (ensure-node-transport ~node) (with-meta ~msg ~msg-meta))))

(defmacro ^:private add-sub
  ([node typ handle-fn]
   `(l/add-subscription
     (ensure-node-transport ~node)
     {:types [~typ] :handle-fn ~handle-fn}))
  ([node typ handle-fn subscription-meta]
   `(l/add-subscription
     (ensure-node-transport ~node)
     (with-meta
       {:types [~typ] :handle-fn ~handle-fn}
       ~subscription-meta))))

(defn- make-req-meta [req-meta]
  (let [req-id         (or (:req-id req-meta) (str (random-uuid)))
        timeout-ms     (or (:timeout-ms req-meta) 60000)
        hb-interval-ms (:hb-interval-ms req-meta)
        hb-lost-ratio  (if hb-interval-ms (or (:hb-lost-ratio req-meta) 2.5) nil)]
    (cond-> (or req-meta {})
      req-id         (assoc :req-id req-id)
      timeout-ms     (assoc :timeout-ms timeout-ms)
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

(defn- make-node-options
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

(defn- make-node-state
  "State of the node.

  State contains:
  - `:local`: All loccal states.
    - `:hb-timer`: a delayed computation for doing heartbeat.
    - `:functions`: Functions we locally support.
    - `:futures`: Locally running function instances.
    - `:transport`: Node's transport.
  - `:remote`: What we know about remote nodes and interactions with remote nodes.
    - `:hb-listener`:  a delayed computation for doing heartbeat listening.
    - `:nodes`: Knowleges about remote nodes. It's driven by *message*, so we
      choose agent for it's message's async handling.
    - `:promises`: Promises waiting for remote responses."
  []
  {:local
   {:hb-timer     (atom nil)
    :functions    (atom {})
    :futures      (atom {})
    :transport         (atom nil)
    :rpc-listener (atom nil)}

   :remote
   {:hb-listener (atom nil)
    :nodes       (agent {})
    :promises    (atom {})}})

(defn make-node
  "Create a node.

  A node contains:

  - `:node-id`: The global identifier for given connection transport.
  - `:node-options`: The options specified for given node. Check [[make-node-options]]
     for details.
  - `:node-state`: The mutable state for this node. Check [[make-node-state]] for
    details."
  [& {:keys [node-id node-options]}]
  (let [node-id (or node-id (str (random-uuid)))]
    {:node-id      node-id
     :node-options (make-node-options node-id node-options)
     :node-state   (make-node-state)}))

(defn clean-node! [node]
  (let [{:keys [hb-timer transport]} (-> node :node-state :local)]
    (when-let [{:keys [cancel]} (and hb-timer @hb-timer @@hb-timer)]
      (cancel))
    (when-let [transport (and transport @transport @@transport)]
      (.close! transport))))

(defn node-futures [node] (-> node :node-state :local :futures))
(defn node-transport [node] (-> node :node-state :local :transport))
(defn node-hb-timer [node] (-> node :node-state :local :hb-timer))
(defn node-functions [node] (-> node :node-state :local :functions))
(defn node-rpc-listener [node] (-> node :node-state :local :rpc-listener))

(defn node-remote-nodes [node] (-> node :node-state :remote :nodes))
(defn node-hb-listener [node] (-> node :node-state :remote :hb-listener))
(defn node-promises [node] (-> node :node-state :remote :promises))

(defn node-info
  "Retrieve node's supported functions as:
  `function-name -> {:keys [arity]}`"
  [{:as node :keys [node-id node-state node-options]}]
  (let [local-state (-> node-state :local)]
    {:node-id node-id

     :node-options
     (select-keys node-options [:hb-options])

     :functions
     (->> @(:functions local-state)
          (map (fn [[k v]] [k (select-keys v [:arity])]))
          (into {}))}))

(defn- on-heartbeat*
  "Update top-level non-nil fields, keep the top-level nil one what it previous
  is."
  [remote-nodes {:as msg :keys [node-id node-options functions]} now-ms]
  (->>
   (fn [{:keys [hb-at] old-functions :functions old-node-options :node-options}]
     {:hb-at      now-ms
      :node-options (if node-options node-options (or old-node-options {}))
      :functions  (if functions functions (or old-functions {}))})
   (update remote-nodes node-id)))

(defn on-heartbeat!
  "Handle heartbeat message from `node-id`.

  Heartbeat message derived from [[node-info]], the structure is the same.

  The node's hb message may contains the functions it now supports."
  ([node msg]
   (on-heartbeat! node msg (*now-ms*)))
  ([node hb-msg now-ms]
   (let [nodes (node-remote-nodes node)]
     (send nodes on-heartbeat* hb-msg now-ms))))

(defn- stop-node-transport* [node-id transport]
  (when (and transport @transport (not (l/closed? @transport)))
    (log/debugf "[%s] closing previous transport" node-id)
    (l/close! @transport))
  nil)

(defn stop-node-transport! [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (node-transport node)
   {:ignores-nil? true}
   (partial stop-node-transport* node-id))
  node)

(defn start-node-transport!
  "Start node transport."
  [{:as node :keys [node-id] {:keys [transport]} :node-state}]
  (let [transport-options (-> node :node-options :transport)
        transport-module  (:xnfun/module transport-options)]
    (if (= transport-module 'xnfun.mqtt)
      (swap!-swap-in-delayed!
       (node-transport node)
       {:ignores-nil? false}
       #(do
          (stop-node-transport* node-id %)
          (log/debugf "[%s] start transport" node-id)
          (make-mqtt-transport node-id transport-options)))
      (->> {:node node}
           (ex-info "unkown transport")
           throw))
    node))

(defn- do-heartbeat*
  ([node] (do-heartbeat* node (*now-ms*)))
  ([node time-in-ms]
   (let [data (assoc (node-info node) :trigger-at time-in-ms)]
     (send-msg node {:typ :hb :data data}))))

(defn- watch-heartbeat-state*
  "Watch node state and trigger heartbeat as state change."
  [node]
  (add-watch
   (node-functions node) :hb
   (fn [_k _r _old _new]
     (do-heartbeat* node))))

(defn- unwatch-hearbteat-state*
  [node]
  (remove-watch (node-functions node) :hb))

(defn- remove-dead-workers*
  "If the remote node itself does not report it's hb options, we'll use current
  node's hb options as the default."
  [remote-nodes now-ms {:keys [hb-interval-ms hb-lost-ratio]}]
  (->> remote-nodes
       (filter
        (fn [[_ {:keys [hb-at node-options]}]]
          (< (- now-ms hb-at)
             (* (get-in node-options [:hb-options :hb-interval-ms] hb-interval-ms)
                (get-in node-options [:hb-options :hb-lost-ratio] hb-lost-ratio)))))
       (into {})))

(defn remove-dead-workers!
  "Try remove all node that losts their heartbeat."
  ([node]
   (remove-dead-workers! node (*now-ms*)))
  ([{:as node :keys [node-options]} now-ms]
   (let [nodes (node-remote-nodes node)
         hb-options (get-in node [:node-options :hb-options])]
     (send nodes remove-dead-workers* now-ms hb-options))))

(defn stop-heartbeat* [node-id hb-timer]
  (when-let [{:keys [cancel]} (and hb-timer @hb-timer)]
    (when cancel
      (log/debugf "[%s] stop previous heartbeat" node-id)
      (cancel)))
  nil)

(defn stop-heartbeat!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (node-hb-timer node)
   {:ignores-nil? true}
   (partial stop-heartbeat* node-id))
  node)

(defn start-heartbeat!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (node-hb-timer node)
   {:ignores-nil? false}
   (fn [v]
     (stop-heartbeat* node-id v)

     (log/debugf "[%s] start heartbeat" node-id)
     (let [hb-timer
           (*periodic-run*
            (*now-ms*)
            (get-in node [:node-options :hb-options :hb-interval-ms])
            (partial do-heartbeat* node))

           cancel-hb-timer
           #(do (log/debugf "[%s] cancel heartbeat" node-id)
                ((:cancel hb-timer)))

           unwatch-hb-state
           #(do (log/debugf "[%s] unwatch heartbeat related state" node-id)
                (unwatch-hearbteat-state* node))]

       (log/debugf "[%s] start watch heartbeat related state" node-id)
       (watch-heartbeat-state* node)

       {:cancel #(do (unwatch-hb-state) (cancel-hb-timer))
        :hb-timer hb-timer
        :hb-state-watcher {:cancel unwatch-hb-state}})))
  node)

(defn stop-heartbeat-listener* [node-id hb-listener]
  (when-let [{:keys [cancel]} (and hb-listener @hb-listener)]
    (when cancel
      (log/debugf "[%s] stop heartbeat listener" node-id)
      (cancel)))
  nil)

(defn stop-heartbeat-listener!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (node-hb-listener node)
   {:ignores-nil? true}
   (partial stop-heartbeat-listener* node-id))
  node)

(defn start-heartbeat-listener!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (node-hb-listener node)
   {:ignores-nil? false}
   (fn [v]
     (stop-heartbeat-listener* node-id v)
     (log/infof "[%s] start heartbeat listener" node-id)

     (log/debugf "[%s] start remote node heartbeat checker" node-id)
     (let [hb-check-timer
           (*periodic-run*
            (*now-ms*)
            (let [{:keys [hb-interval-ms hb-lost-ratio]}
                  (get-in node [:node-options :hb-options])]
              (* hb-interval-ms hb-lost-ratio))
            (partial remove-dead-workers! node))

           cancel-hb-check
           (fn []
             (log/debugf "[%s] stop remote node heartbeat checker" node-id)
             ((:cancel hb-check-timer)))

           unsub-hb-msg
           (do (log/debugf "[%s] start subscribe to remote node heartbeat" node-id)
               (->> (fn [{:keys [data]}] (on-heartbeat! node data))
                    (add-sub node :hb)))

           unsub-hb-msg
           (fn []
             (log/debugf "[%s] unsub remote node heartbeat" node-id)
             (unsub-hb-msg))]
       {:cancel #(do (unsub-hb-msg) (cancel-hb-check))
        :hb-check-timer hb-check-timer
        :unsub-hb-msg unsub-hb-msg}))))

(defn select-remote-node
  "Filter out node that satisties the condition and randomly selects one.
  - `match-fn`: matches if (match-fn node)"
  [{:as node} {:keys [match-fn]}]
  (let [node-ids
        (->> @(node-remote-nodes node)
             (filter #(if match-fn (match-fn (second %)) true))
             (map first))]
    (if (empty? node-ids) nil (rand-nth node-ids))))

(defn- add-function*
  [local-functions fun-name fun &
   {:keys [overwrite] :or {overwrite true}}]
  (let [func-arity (max-arity fun)]
    (when-not (#{1 2} func-arity)
      (throw (ex-info "invalid rpc function (arity not match)"
                      {:function-name fun-name :function fun})))
    (->>
     (fn [prev-fun]
       (when (and prev-fun (not overwrite))
         (->> {:function-name fun-name :function fun}
              (ex-info "function already exists")
              throw))
       {:function fun :arity func-arity})
     (update local-functions fun-name))))

(defn add-function!
  "Register a supported function to current node.

  With `overwrite` being false, throw if the same name already exists.
  "
  [{:as node} fun-name fun &
   {:as opts :keys [overwrite]}]
  (-> (node-functions node)
      (swap! add-function* fun-name fun opts)))

(defn call-function!
  "Call registered function with name.

  All data should be format of `{:keys [typ data]}`

  Arguments:

  - `out-c` (optional): function can send message to caller via the channel
      - `:typ=:xnfun/hb`: Long-running function heartbeat
      - `:typ=:xnfun/to-caller`: Message to caller
  - `in-c` (optional): caller send some signal to callee
      - `:typ=:xnfun/cancel`: Cancellation message.
      - `:typ=:xnfun/to-callee`: Message to callee."
  [node fun-name params
   & {:as options :keys [out-c in-c req-meta]}]
  (let [{:keys [function arity]}
        (-> node
            node-functions
            deref
            (get fun-name))]
    (when-not function
      (->> {:function-name fun-name :params params}
           (ex-info "function not found")
           throw))
    (if (= arity 2)
      (apply function [params {:out-c (or out-c (chan (dropping-buffer 1)))
                               :in-c  (or in-c  (chan (dropping-buffer 1)))
                               :req-meta req-meta}])
      (apply function [params]))))

(defn fullfill-promise!
  "Handling data from callee.

  Arguments:

  - `:status`: `:ok` or `:xnfun/err` or `:xnfun/remote-err` or `:err`
      - When error is assured triggered by the user's function code, no matter it
        runs locally or remotely, the status should be `:err`
      - When error is not assured triggered by user's function code:
          - `:xnfun/err`: Error occurs locally
          - `:xnfun/remote-err`: Error occurs remotely
  - `:meta`: the call metadata.
  - `:data`: If `status=:ok`, means the fullfilled data; else the data
    describing the error.
  "
  [{:as node :keys [node-id]} req-id {:as r :keys [status data]}]
  (let [[old _] (swap-vals! (node-promises node) (fn [m] (dissoc m req-id)))]
    (if-let [p (get old req-id)]
      (let [{:keys [hb-lost-timer timeout-timer res-promise]} @p]
        (log/debugf "[%s] fullfilled [%s]: %s" node-id req-id data)
        (deliver res-promise r)
        (when-let [{:keys [cancel]} hb-lost-timer] (cancel))
        (when-let [{:keys [cancel]} timeout-timer] (cancel)))
      (log/debugf "[%s] request not found [%s]: %s" node-id req-id r))))

(defn get-promise
  "Get the submitted promsie by [[submit-promise!]]."
  [node req-id]
  (when-let [p (-> @(node-promises node) (get req-id))]
    @p))

(defn- promise-hb
  [{:as node :keys [node-id]}
   {:keys [req-id req-meta]
    {:keys [hb-lost-ratio hb-interval-ms]} :req-meta}]
  (when (or hb-lost-ratio hb-interval-ms)
    (when (or (not hb-lost-ratio) (not hb-interval-ms))
      (throw (ex-info "illegal promise-hb" {:req-id req-id :req-meta req-meta})))

    (swap!-swap-in-delayed!
     (node-promises node)
     {:ignores-nil? true :ks [req-id]}
     (fn [p]
       (let [{:keys [hb-lost-timer]} @p]
         (when-let [{:keys [cancel]} hb-lost-timer] (cancel))

         (let [hb-lost-at
               (+ (*now-ms*) (* hb-lost-ratio hb-interval-ms))

               on-hb-lost
               (fn [at]
                 (->> {:status :xnfun/err
                       :data {:typ :timeout :reason "hb-lost" :timeout-at at}}
                      (fullfill-promise! node req-id)))]
           (log/debugf "[%s][%s] heartbeat will lost in (* %s %s)ms"
                       node-id req-id hb-lost-ratio hb-interval-ms)
           (-> @p
               (assoc :hb-lost-at hb-lost-at)
               (assoc :hb-lost-timer (*run-at* hb-lost-at on-hb-lost)))))))))

(defn- promise-submit-timeout-handler
  [node req-id timeout-ms]
  (->> #(->> {:status :xnfun/err
              :data {:typ :timeout :reason "timeout" :timeout-at %}}
             (fullfill-promise! node req-id))
       (*run-at* (+ (*now-ms*) timeout-ms))))

(defn submit-promise!
  "Submit promise to node.

  The submitted promise may be fullfilled on timeout. Or can be fullfilled
  manually with: [[fullfill-promise!]]

  Returns: `{:keys [res-promise request timeout-timer hb-lost-timer]}`

  - `request`: {:keys [`req-meta`]}"
  [{:as node :keys [node-id]}
   {:as request :keys [req-meta]}]
  (let [req-meta (make-req-meta req-meta)
        request  (assoc request :req-meta req-meta)

        {:keys [req-id timeout-ms hb-lost-ratio hb-interval-ms]}
        req-meta

        create-promise
        #(let [p {:res-promise   (promise)
                  :request       request
                  :timeout-timer (promise-submit-timeout-handler node req-id timeout-ms)}]
           (if (and hb-lost-ratio hb-interval-ms)
             (let [hb (fn [] (promise-hb node req-meta))] (hb) (assoc p :hb hb))
             p))

        r
        @(-> (swap!-swap-in-delayed!
              (node-promises node)
              {:ignores-nil? false :ks [req-id]}
              #(if % @% (create-promise)))
             (get req-id))]
    r))

(defn- call-request-cleanup
  [{:as node :keys [node-id]} req-id]
  (let [[old _] (swap-vals! (node-futures node) #(dissoc % req-id))]
    (when-let [req (get old req-id)]
      (let [{:keys [in-c out-c in-c-internal out-c-internal hb-lost-timer timeout-timer]} @req]
        (future
          (Thread/sleep 100)
          (close! in-c-internal)
          (close! out-c-internal)
          (close! out-c)
          (close! in-c)
          (when-let [{:keys [cancel]} hb-lost-timer] (cancel))
          (when-let [{:keys [cancel]} timeout-timer] (cancel)))))))

(defn- get-call
  "Get the successfully submitted call."
  [node req-id]
  (when-let [f (-> @(node-futures node) (get req-id))]
    @f))

(defn- call-request-cancel
  "Cancel the call.

  Arguments:

  - `:reason`: {:keys [typ data]}, `typ` may be:
      - `:timeout`
      - `:xnfun/caller-cancel`
  "
  [node req-id reason]
  (when-let [{:keys [in-c-internal running-future]} (get-call node req-id)]
    (>!! in-c-internal {:typ :xnfun/cancel :data reason})
    (future (Thread/sleep 100) (future-cancel running-future))))

(defn- call-request-hb [{:as node :keys [node-id]} req-id]
  (swap!-swap-in-delayed!
   (node-futures node)
   {:ignores-nil? true :ks [req-id]}
   (fn [p]
     (when-let [hb-lost-timer (and p (:hb-lost-timer @p))]
       (when-let [{:keys [cancel]} hb-lost-timer] (cancel)))

     (let [{:keys [hb-lost-ratio hb-interval-ms]} (get-in @p [:request :req-meta])
           hb-lost-at (+ (*now-ms*) (* hb-lost-ratio hb-interval-ms))

           on-hb-lost
           (fn [at]
             (->> {:typ :timeout :data {:reason "hb-lost" :at at}}
                  (call-request-cancel node req-id)))]
       (log/debugf "[%s][%s] heartbeat will lost in (* %s %s)ms"
                   node-id req-id hb-lost-ratio hb-interval-ms)
       (-> @p
           (assoc :hb-lost-at hb-lost-at)
           (assoc :hb-lost-timer (*run-at* hb-lost-at on-hb-lost)))))))

(defn- submit-call*
  [{:as node :keys [node-id]}
   fun-name params
   {:as options :keys [req-meta in-c out-c]
    {:keys [req-id timeout-ms hb-interval-ms hb-lost-ratio]} :req-meta}]
  (let [request {:fun-name fun-name
                 :params params
                 :req-meta req-meta}

        out-c-internal (chan 1)
        in-c-internal (chan 1)

        call-option
        {:in-c in-c-internal :out-c out-c-internal :req-meta req-meta}

        hb
        (when (and hb-interval-ms hb-lost-ratio) (and hb-interval-ms hb-lost-ratio)
              #(call-request-hb node req-id))

        r {:req-id req-id
           :submit-at (*now-ms*)
           :request request
           :in-c in-c
           :out-c out-c
           :in-c-internal in-c-internal
           :out-c-internal out-c-internal}]

    (log/debugf "[%s][%s] waiting for callee message" node-id req-id)
    (go-loop [d (<! out-c-internal)]
      (when-let [{:keys [typ]} d]
        (try
          (log/debugf "[%s][%s][%s] callee message: %s" node-id req-id typ d)
          ;; all message counts as heartbeat message.
          (when hb (hb))

          ;; Forward message, and always blocking when forwarding.
          (if (#{:xnfun/hb :xnfun/to-caller} typ)
            (>!! out-c d)
            (log/warnf "unkown message type from callee: %s" typ))
          (catch Exception e
            (log/warnf e "[%s][%s] error handling msg from callee: %s" node-id req-id d)))
        (recur (<! out-c-internal))))

    (log/debugf "[%s][%s] waiting for caller message" node-id req-id)
    (go-loop [d (<! in-c)]
      (when-let [{:keys [typ data]} d]
        (try
          (cond
            (= typ :xnfun/cancel)
            (->> {:typ :xnfun/caller-cancel :data data}
                 (call-request-cancel node req-id))

            (= typ :xnfun/to-callee)
            (>!! in-c-internal d)

            :else
            (log/warnf "[%s] unkown message from caller: %s" node-id d))
          (catch Exception e
            (log/warnf e "[%s][%s] error handling msg from caller: %s" node-id req-id d)))
        (recur (<! in-c))))

    (cond-> r
      true
      (assoc :timeout-timer
             (*run-at*
              (+ (*now-ms*) timeout-ms)
              (fn [at]
                (->> {:typ :timeout :data {:reason "timeout" :at at}}
                     (call-request-cancel node req-id)))))

      true
      (assoc :running-future
             (future
               (try (call-function! node fun-name params call-option)
                    (finally (call-request-cleanup node req-id)))))

      (and hb-interval-ms hb-lost-ratio)
      (assoc :hb #(call-request-hb node req-id)))))

(defn submit-call!
  "Submit function to node and run it asynchronously.

  Arguments

  - `out-c`: callee will send message (and heartbeat) throught this
     channel.
      - If not given, will create a dropping buffer channel
      - Notice that the out-c would block callee, so you should handle the message
        as soon as possible.
  - `req-meta`: Check [[make-req-meta]] for details.

  Returns `{:keys [req-id submit-at request running-future in-c out-c
                   timeout-timer hb-lost-timer]}`

  `in-c` accepts:

  - {`:typ` `:xnfun/cancel` `:data` ...}
  - {`:typ` `:xnfun/to-callee` `:data` ...}

  `out-c` returns:

  - {`:typ` `:xnfun/hb` `:data` ...}
  - {`:typ` `:xnfun/to-caller` `:data` ...}

  Check [[call-function!]] for `in-c` and `out-c`.
  "
  [{:as node :keys [node-id]}
   fun-name params
   & {:as options :keys [req-meta out-c]}]
  (let [req-meta (make-req-meta req-meta)
        req-id   (:req-id req-meta)
        options  (-> options
                     (assoc :req-meta req-meta)
                     (assoc :in-c (chan 1))
                     (assoc :out-c (or out-c (chan (dropping-buffer 1)))))]
    (log/debugf "[%s] submitted request %s" node-id req-id)
    @(-> (swap!-swap-in-delayed!
          (node-futures node)
          {:ignores-nil? false :ks [req-id]}
          #(if % @% (submit-call* node fun-name params options)))
         (get req-id))))

;; RPC related data
;; :req
;;   :typ :xnfun/call | :xnfun/to-callee | :xnfun/cancel
;; :resp
;;   :typ :xnfun/to-caller | :xnfun/hb | :xnfun/resp

(defn- rpc-client--handle-caller-msg
  "Message will be forward to [[rpc-server--on-req-msg]]"
  [{:keys [req-id in-c send-req]}]
  (go-loop []
    (let [d (<! in-c)]
      (when-let [{:keys [typ]} d]
        (try
          (if (or (= typ :xnfun/to-callee)
                  (= typ :xnfun/cancel))
            (send-req d)
            (log/warnf "rpc client unkown message from caller: %s" d))
          (catch Exception e
            (log/warnf e "[%s] error handle caller msg: %s" req-id d)))
        (recur)))))

(defn- rpc-client--handle-resp-from-callee
  "Message from [[rpc-server--handle-callee-msg]]"
  [{:keys [node req-id out-c]} {:as msg :keys [typ data]}]
  (when-not (= typ :resp)
    (log/warnf "[%s] should be :resp, but got %s" (:node-id node) msg)
    (->> {:msg msg} (ex-info (str "should be :resp, but got " typ)) throw))

  (if-let [{:as res-promise :keys [hb]} (get-promise node req-id)]
    (let [{:as msg-data :keys [typ data]} data]
      (cond
        (= typ :xnfun/resp)
        (let [{:keys [status data]} data]
          (->>
           (cond
             (= status :ok)        {:status :ok :data data}
             (= status :err)       {:status :err :data data}
             (= status :xnfun/err) {:status :xnfun/remote-err :data data}

             :else {:status :xnfun/remote-err
                    :data {:typ :xnfun/err-invalid-resp :data data}})
           (fullfill-promise! node req-id)))

        (#{:xnfun/to-caller :xnfun/hb} typ)
        (do (when hb (hb)) (>!! out-c msg-data))

        :else
        (log/warnf "[%s] recv unkown :resp message from %s - %s"
                   (:node-id node) req-id typ)))
    (log/warnf "the response promise is already fullfilled: %s" req-id)))

(defn- rpc-server--handle-req--call
  "Handle the callee message/result to caller.

  1. waiting on callee's `out-c`, and forward message to caller.

  2. waiting on callee's result, send back to caller.

  Notice that the message is forward to caller and handle
  by [[rpc-client--handle-resp-data-from-callee]]
  "
  [{:keys [out-c send-resp running-future]}]
  (go-loop [d (<!! out-c)]
    (when-let [{:keys [typ]} d]
      (if (#{:xnfun/hb :xnfun/to-caller} typ)
        (send-resp d)
        (log/warnf "illegal message from callee: %s" typ))
      (recur (<!! out-c))))

  (future
    (send-resp
     (try
       (let [r @running-future]
         {:typ :xnfun/resp
          :data {:status :ok :data r}})
       (catch Exception e
         {:typ :xnfun/resp
          :data {:status :err
                 :data {:exception-class (str (class e))}}})))))

(defn- rpc-server--handle-req-from-caller
  "Handle the `:req` message from remote caller to current `node`.

  1. For new call, initiate the call by [[submit-call!]] to current node. and
  waiting message via [[remote-call-server-handle-callee-msg]].

  2. For initiated call, forward message to callee.
  "
  [{:as node :keys [node-id]}
   {:as msg
    {:as msg-typ}                   :typ
    {:as msg-data :keys [typ data]} :data}]
  (when-not (= msg-typ :req)
    (throw (ex-info "should be :req message" {:msg msg})))

  (let [{:as msg-meta :keys [req-id caller-node-id]} (meta msg)]
    (log/debugf "[%s] recv req msg %s - %s" node-id msg msg-meta)
    (cond
      (= typ :xnfun/call)
      (let [[[fun-name params] req-meta] data
            _ (log/debugf "[%s] recv call %s %s %s"
                          node-id fun-name params req-meta)

            ;; handles to the callee for its interactive message
            out-c (chan 1)
            options {:out-c out-c :req-meta req-meta}

            send-resp
            (fn [d] (send-msg node {:typ :resp :data d} msg-meta))

            {:keys [running-future]}
            (submit-call! node fun-name params options)]

        (log/infof "[%s] submit call %s: %s" node-id fun-name params)
        (rpc-server--handle-req--call
         {:out-c out-c :send-resp send-resp :running-future running-future}))

      (#{:xnfun/to-callee :xnfun/cancel} typ)
      (if-let [{:keys [in-c]} (get-call node req-id)]
        (>!! in-c msg-data)
        (log/warnf "[%s] request [%s] not found from [%s]"
                   node-id req-id caller-node-id))

      :else
      (log/warnf "[%s] illegal :req message type: %s[%s] from %s"
                 node-id typ req-id caller-node-id))))

(defn- rpc-client--submit-call-to-remote-node!
  "Here we do the submit.

  1. Send request to remote callee
  2. Register remote callee message handler
  3. Register caller message handler

  And this function assumes that `req-meta` is already initialized
  via [[make-req-meta]].
  "
  [{:as node :keys [node-id]}
   fun-name params callee-node-id
   & {:as options :keys [req-meta out-c]}]

  (let [in-c  (chan 1)
        out-c (or out-c (chan (dropping-buffer 1)))

        p (->> {:req-meta req-meta
                :fun-name fun-name
                :params params
                :callee-node-id callee-node-id
                :in-c in-c
                :out-c out-c}
               (submit-promise! node))

        req-id (:req-id req-meta)
        res-promise (:res-promise p)
        msg-meta {:callee-node-id callee-node-id :req-id req-id}

        send-req (fn [msg]
                   (log/debugf "send req msg: %s - %s" msg msg-meta)
                   (send-msg node {:typ :req :data msg} msg-meta))]

    (log/debugf "[%s] [-> %s %s] start handle caller message"
                node-id callee-node-id req-id)
    (rpc-client--handle-caller-msg
     {:req-id req-id :in-c in-c :send-req send-req})

    (log/debugf "[%s] [-> %s %s] start handle callee message"
                node-id callee-node-id req-id)
    (let [handle-fn (->> {:node node :req-id req-id :out-c out-c}
                         (partial rpc-client--handle-resp-from-callee))
          unsub (add-sub node :resp handle-fn msg-meta)]
      (future (try @res-promise (finally (unsub)))))

    (future (try @res-promise (finally (close! in-c) (close! out-c))))

    (log/debugf "[%s] [-> %s %s] initiate request :req :xnfun/call"
                node-id callee-node-id req-id)
    (send-req {:typ :xnfun/call :data [[fun-name params] req-meta]})
    p))

(defn submit-remote-call!
  "Submit call to remote nodes.

  Node is selected by the optional `match-node-fn` and the `fun-name`."
  [node fun-name params
   & {:as options :keys [req-meta out-c match-node-fn]}]
  (let [options
        (assoc options :req-meta (make-req-meta req-meta))

        callee-node-id
        (->> {:match-fn
              (if match-node-fn
                (fn [n] (and (get-in n [:functions fun-name])
                             (match-node-fn n)))
                (fn [n] (get-in n [:functions fun-name])))}
             (select-remote-node node))]
    (when-not callee-node-id
      (throw (ex-info "no matching node" {:fun-name fun-name})))
    (rpc-client--submit-call-to-remote-node!
     node fun-name params callee-node-id options)))

(defn stop-serve-remote-call!
  [node]
  (swap!-swap-in-delayed!
   (node-rpc-listener node)
   {:ignores-nil? false}
   (fn [v]
     (when-let [{:keys [stop]} (and v @v)] (stop))
     nil)))

(defn serve-remote-call!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (node-rpc-listener node)
   {:ignores-nil? false}
   (fn [v]
     (when-let [{:keys [stop]} (and v @v)] (stop))

     (log/infof "[%s] start serving remote call" node-id)
     (let [handle-fn (partial rpc-server--handle-req-from-caller node)
           unsub (add-sub node :req handle-fn)]
       {:stop #(do (log/infof "[%s] stop serving remote call" node-id)
                   (unsub))}))))

(comment

  (defn add [[x y]] (+ x y))

  (defn echo [_ {:as opt :keys [in-c out-c]
                 {:keys [hb-interval-ms hb-lost-ratio]} :req-meta}]
    (let [hb (future (while true
                       (Thread/sleep hb-interval-ms)
                       (>!! out-c {:typ :xnfun/hb :data nil})))]
      (go-loop [d (<!! in-c)]
        (when-let [{:keys [typ data]} d]
          (case typ
            :xnfun/cancel (future-cancel hb)
            :xnfun/to-callee (>!! out-c {:typ :xnfun/to-caller :data data})
            (>!! out-c {:typ :xnfun/to-caller :data {:unkown d}}))
          (recur (<!! in-c))))
      @hb))

  (defn n [v]
    (when v (try (clean-node! v) (catch Exception _)))
    (-> {:node-id "node-0"
         :node-options {:hb-options
                        {:hb-interval-ms 30000}}}
        make-node))

  (defonce n0 (atom nil))

  (defn restart! []
    (when-let [n0 @n0] (clean-node! n0))
    (swap! n0 n))

  (defn stop-n0! []
    (doto @n0
      (stop-heartbeat-listener!)
      (stop-heartbeat!)
      (stop-node-transport!)
      (stop-serve-remote-call!)))

  (defn restart-n0! []
    (stop-n0!)
    (doto @n0
      (add-function! "add" add)
      (add-function! "echo" echo)
      (start-node-transport!)
      (start-heartbeat!)
      (start-heartbeat-listener!)
      (serve-remote-call!)))

  (restart!)

  (start-node-transport! @n0)
  (stop-node-transport! @n0)

  (start-heartbeat! @n0)
  (stop-heartbeat! @n0)

  (start-heartbeat-listener! @n0)
  (stop-heartbeat-listener! @n0)

  (def p0 (submit-promise! @n0 {:req-meta {:timeout-ms 3000}}))
  (def p1 (submit-promise! @n0 {:req-meta {:timeout-ms 30000
                                           :hb-interval-ms 3000}}))
  ((:hb p1))

  (= p1 (get-promise @n0 (get-in p1 [:request :req-meta :req-id])))
  (fullfill-promise! @n0 (get-in p1 [:request :req-meta :req-id]) {:status :ok :data 10})

  (add-function! @n0 "add" (fn [[x y]] (+ x y)))
  (call-function! @n0 "add" [1 2])
  (def c0 (submit-call! @n0 "add" [1 2]))

  (add-function! @n0 "echo" echo)

  (def c1 (->> {:req-meta {:timeout-ms 60000
                           :hb-interval-ms 3000
                           :hb-lost-ratio 2.5}}
               (submit-call! @n0 "echo" nil)))
  (go-loop [d (<!! (:out-c c1))]
    (when d
      (println "recv: " d)
      (recur (<!! (:out-c c1)))))

  (future-cancel (:running-future c1))
  (>!! (:in-c c1) {:typ :xnfun/to-callee :data "hello world"})
  (>!! (:in-c c1) {:typ :xnfun/cancel :data nil})

  (serve-remote-call! @n0)
  (stop-serve-remote-call! @n0)

  (def p2 (submit-remote-call! @n0 "add" [[1 2]]))
  (def p3 (->> {:req-meta {:timeout-ms 60000
                           :hb-interval-ms 3000
                           :hb-lost-ratio 2.5}}
               (submit-remote-call! @n0 "echo" nil)))
  (go-loop [d (<!! (:out-c (:request p3)))]
    (when d
      (println "recv: " d)
      (recur (<!! (:out-c (:request p3))))))

  (>!! (:in-c (:request p3)) {:typ :xnfun/to-callee :data "hello world"})
  (>!! (:in-c (:request p3)) {:typ :xnfun/cancel :data "cancel"})
  ;
  )
