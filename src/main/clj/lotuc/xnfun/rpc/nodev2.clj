(ns lotuc.xnfun.rpc.nodev2
  "Node abstraction for xnfun.

  - You can start a link (ex. MQTT link) for connecting with other nodes.
  - You can register (capability) functions to current node.
  - You can submit function call to current node (call you registered functions).
  - You can register a promise to current node and maybe fullfill it later.
  - You can do call to other node through current node via your established
    link

  When caller call from one node to other node's function (callee), this will
  - Submit a promise in current node waiting for result.
  - Submit a future (representing the remote computation, the callee) in remote
    node.
  - Create a bi-directional communication channel between caller and callee.

  **Node**
  - [[make-node]]
  - [[node-info]]
  - [[node-link]] [[node-futures]] [[node-promises]] [[node-remote-nodes]]
  - [[select-remote-node]]

  - [[start-node-link!]] [[stop-node-link!]]
  - [[on-heartbeat!]]  [[remove-dead-workers!]]
  - [[start-heartbeat!]] [[stop-heartbeat!]]
  - [[start-heartbeat-listener!]] [[stop-heartbeat-listener!]]

  **Capabilities**
  - [[add-function!]] [[call-function!]]

  **Promises**
  - [[fullfill-promise!]] [[submit-promise!]]

  **Function**
  - [[submit-call!]]

  **Remote function**
  - [[submit-remote-call!]] [[serve-remote-call!]]"
  (:require
   [clojure.core.async :refer [<! <!! >!! chan close! dropping-buffer go-loop]]
   [clojure.tools.logging :as log]
   [lotuc.xnfun.utils :refer [max-arity
                              swap!-swap-in-delayed!
                              *now-ms* *run-at* *periodic-run*]]
   [lotuc.xnfun.rpc.mqtt-link :refer [new-mqtt-link]]
   [lotuc.xnfun.rpc.link :as l]))

(defmacro ^:private ensure-node-link [node]
  (let [link (gensym)]
    `(let [~link @(-> ~node :node-state :local :link)
           ~link (and ~link @~link)]
       (when (nil? ~link) (throw (ex-info "link not created" {:node ~node})))
       (when (.closed? ~link) (throw (ex-info "link closed" {:node ~node})))
       ~link)))

(defmacro ^:private send-msg
  ([node msg]
   `(l/send-msg (ensure-node-link ~node) ~msg))
  ([node msg msg-meta]
   `(l/send-msg (ensure-node-link ~node) (with-meta ~msg ~msg-meta))))

(defmacro ^:private add-sub
  ([node typ handle-fn]
   `(l/add-subscription
     (ensure-node-link ~node)
     {:types [~typ] :handle-fn ~handle-fn}))
  ([node typ handle-fn subscription-meta]
   `(l/add-subscription
     (ensure-node-link ~node)
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

(defn- make-node-link-options
  "We use mqtt as the default communication link."
  [node-id link]
  (or link
      {:xnfun/module 'xnfun.mqtt
       :mqtt-topic-prefix ""
       :mqtt-config
       {:broker "tcp://127.0.0.1:1883"
        :client-id node-id
        :connect-options {:max-in-flight 1000
                          :automatic-reconnect true}}}))

(defn- make-node-options
  "Options of the node.

  Options contains:
  - `hb-options`:
    - `:hb-interval-ms`: Heartbeat interval for this node.
    - `:hb-lost-ratio`: Consider this node to be heartbeat lost in
      (* hb-lost-ratio hb-interval-ms) when no heartbeat occurs.
  - `:link`: The link this node would make to connect to other nodes.

  Arguments:
  - `node-options`: default options."
  [node-id node-options]
  (-> (or node-options {})
      (update :hb-options #(make-node-hb-options %))
      (update :link #(make-node-link-options node-id %))))

(defn- make-node-state
  "State of the node.

  State contains:
  - `:local`: All loccal states.
    - `:hb-timer`: a delayed computation for doing heartbeat.
    - `:functions`: Functions we locally support.
    - `:futures`: Locally running function instances.
    - `:link`: Node's link.
  - `:remote`: What we know about remote nodes and interactions with remote nodes.
    - `:hb-listener`:  a delayed computation for doing heartbeat listening.
    - `:nodes`: Knowleges about remote nodes. It's driven by *message*, so we
      choose agent for it's message's async handling.
    - `:promises`: Promises waiting for remote responses."
  []
  {:local  {:hb-timer  (atom nil)
            :functions (atom {})
            :futures   (atom {})
            :link      (atom nil)}
   :remote {:hb-listener (atom nil)
            :nodes       (agent {})
            :promises    (atom {})}})

(defn make-node
  "Create a node.

  A node contains:
  - `:node-id`: The global identifier for given connection link.
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
  (let [{:keys [hb-timer link]} (-> node :node-state :local)]
    (when-let [{:keys [cancel]}
               (and hb-timer @hb-timer (realized? @hb-timer) @@hb-timer)]
      (cancel))
    (when-let [link (and link @link (realized? @link) @@link)]
      (when (realized? link) (.close! @link)))))

(defn node-promises [node] (-> node :node-state :remote :promises))
(defn node-futures [node] (-> node :node-state :local :futures))
(defn node-link [node] (-> node :node-state :local :link))
(defn node-remote-nodes [node] (-> node :node-state :remote :nodes))

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

(defn- stop-node-link* [node-id link]
  (when (and link (realized? link) @link (not (l/closed? @link)))
    (log/debugf "[%s] closing previous link" node-id)
    (l/close! @link))
  nil)

(defn stop-node-link! [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (node-link node)
   {:ignores-nil? true}
   (partial stop-node-link* node-id))
  node)

(defn start-node-link!
  "Start node link."
  [{:as node :keys [node-id] {:keys [link]} :node-state}]
  (let [link-options (-> node :node-options :link)
        link-module  (:xnfun/module link-options)]
    (if (= link-module 'xnfun.mqtt)
      (swap!-swap-in-delayed!
       (node-link node)
       {:ignores-nil? false}
       #(do
          (stop-node-link* node-id %)
          (log/debugf "[%s] start link" node-id)
          (new-mqtt-link node-id link-options)))
      (->> {:node node}
           (ex-info "unkown link")
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
  (let [functions (-> node :node-state :local :functions)]
    (add-watch
     functions :hb
     (fn [_k _r _old _new]
       (do-heartbeat* node)))))

(defn- unwatch-hearbteat-state*
  [node]
  (let [functions (-> node :node-state :local :functions)]
    (remove-watch functions :hb)))

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
  (when-let [{:keys [cancel]} (and hb-timer (realized? hb-timer) @hb-timer)]
    (when cancel
      (log/debugf "[%s] stop previous heartbeat" node-id)
      (cancel)))
  nil)

(defn stop-heartbeat!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (get-in node [:node-state :local :hb-timer])
   {:ignores-nil? true}
   (partial stop-heartbeat* node-id))
  node)

(defn start-heartbeat!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (get-in node [:node-state :local :hb-timer])
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
  (when-let [{:keys [cancel]} (and hb-listener (realized? hb-listener) @hb-listener)]
    (when cancel
      (log/debugf "[%s] stop heartbeat listener" node-id)
      (cancel)))
  nil)

(defn stop-heartbeat-listener!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (get-in node [:node-state :remote :hb-listener])
   {:ignores-nil? true}
   (partial stop-heartbeat-listener* node-id))
  node)

(defn start-heartbeat-listener!
  [{:as node :keys [node-id]}]
  (swap!-swap-in-delayed!
   (get-in node [:node-state :remote :hb-listener])
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

  With `overwrite` be false, we through if the same name already exists.
  "
  [{:as node} fun-name fun &
   {:as opts :keys [overwrite]}]
  (-> (-> node :node-state :local :functions)
      (swap! add-function* fun-name fun opts)))

(defn call-function!
  "Call registered function with name.

  All data should be format of {:keys [typ data]}

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
            (get-in [:node-state :local :functions])
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
    (when (realized? p) @p)))

(defn- promise-hb
  [{:as node :keys [node-id]} req-id]
  (when-let [{:as req-meta :keys [hb-lost-ratio hb-interval-ms]}
             (get-in (get-promise node req-id) [:request :req-meta])]
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

  Returns: {:keys [res-promise request timeout-timer hb-lost-timer]}
  - `request`: {:keys [`req-meta`]}"
  [{:as node :keys [node-id]}
   {:as request :keys [req-meta]}]
  (let [req-meta (make-req-meta req-meta)
        request  (assoc request :req-meta req-meta)

        {:keys [req-id timeout-ms hb-lost-ratio hb-interval-ms]}
        req-meta

        _ (log/debugf "[%s] submit promise [%s]" node-id req-id)

        create-promise
        (fn []
          (let [p {:res-promise   (promise)
                   :request       request
                   :timeout-timer (promise-submit-timeout-handler node req-id timeout-ms)}]
            (if (and hb-lost-ratio hb-interval-ms)
              (let [hb (fn [] (promise-hb node req-id))]
                (hb)
                (assoc p :hb hb))
              p)))

        r
        @(-> (swap!-swap-in-delayed!
              (node-promises node)
              {:ignores-nil? false :ks [req-id]}
              #(or % (create-promise)))
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
    (when (realized? f) @f)))

(defn- call-request-cancel
  "Cancel the call.

  Arguments:
  `:reason`: {:keys [typ data]}, `typ` may be:
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
            (log/warnf "unkown message from caller: %s" typ))
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

  Returns {:keys [req-id submit-at request running-future in-c out-c
                  timeout-timer hb-lost-timer]}

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
    @(-> (swap!-swap-in-delayed!
          (node-futures node)
          {:ignores-nil? false :ks [req-id]}
          #(or % (submit-call* node fun-name params options)))
         (get req-id))))

;; RPC related data
;; :req
;;   :typ :xnfun/call | :xnfun/to-callee | :xnfun/cancel
;; :resp
;;   :typ :xnfun/to-caller | :xnfun/hb | :xnfun/resp

(defn- remote-call-client-handle-caller-msg
  "Message to [[remote-call-server-handle-caller-msg]]"
  [{:keys [req-id in-c send-req]}]
  (go-loop []
    (let [d (<! in-c)]
      (when-let [{:keys [typ]} d]
        (try
          (if (or (= typ :xnfun/to-callee)
                  (= typ :xnfun/cancel))
            (send-req d)
            (log/warnf "unkown message from caller: %s" d))
          (catch Exception e
            (log/warnf e "[%s] error handle caller msg: %s" req-id d)))
        (recur)))))

(defn- remote-call-client-handle-callee-msg
  "Message from [[remote-call-server-handle-callee-msg]]"
  [{:keys [node req-id out-c]} {:as msg :keys [typ data]}]
  (if-let [{:as res-promise :keys [hb]} (get-promise node req-id)]
    (case typ
      :xnfun/resp
      (let [{:keys [status data]} data]
        (->>
         (cond
           (= status :ok)        {:status :ok :data data}
           (= status :err)       {:status :err :data data}
           (= status :xnfun/err) {:status :xnfun/remote-err :data data}

           :else {:status :xnfun/remote-err
                  :data {:typ :xnfun/err-invalid-resp :data data}})
         (fullfill-promise! node req-id)))

      :xnfun/to-caller
      (do (when hb (hb)) (>!! out-c data))

      :xnfun/hb
      (when hb (hb)))
    (log/warnf "the response promise is already fullfilled: %s" req-id)))

(defn- remote-call-server-handle-callee-msg
  "Handle the callee message/result to caller.

  1. waiting on callee's `out-c`, and forward message to caller.

  2. waiting on callee's result, send back to caller.

  Notice that the message is forward to caller and handle
  by [[remote-call-client-handle-callee-msg]]
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

(defn- remote-call-server-handle-caller-msg
  "Handle the message from remote caller.

  1. For new call, initiate the call by [[submit-call!]] to current node. and
  waiting message via [[remote-call-server-handle-callee-msg]].

  2. For initiated call, forward message to callee.
  "
  [node {:as msg :keys [typ data]}]
  (let [{:as m :keys [req-id caller-node-id]} (meta msg)]
    (cond
      (= typ :xnfun/call)
      (let [[[fun-name params] req-meta m] data

            ;; handles to the callee for its interactive message
            out-c (chan 1)
            options {:out-c out-c :req-meta req-meta}

            send-resp
            (fn [d] (send-msg node {:typ :resp :data d} m))

            {:keys [out-c running-future]}
            (submit-call! node fun-name params options)]
        (future (try @running-future (finally (close! out-c))))
        (remote-call-server-handle-callee-msg
         {:out-c out-c :send-resp send-resp :running-future running-future}))

      (#{:xnfun/to-callee :xnfun/cancel} typ)
      (if-let [{:keys [in-c]} (get-call node req-id)]
        (>!! in-c msg)
        (log/warnf "request [%s] not found from [%s]" req-id caller-node-id)))))

(defn- submit-remote-call-to-node!
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

        req-id   (-> p :request :req-meta :req-id)
        msg-meta {:callee-node-id callee-node-id :req-id req-id}

        send-req (fn [msg] (send-msg node {:typ :req :data msg} msg-meta))]

    (log/debugf "[%s] [-> %s %s] start handle caller message"
                node-id callee-node-id req-id)
    (remote-call-client-handle-caller-msg
     {:req-id req-id :in-c in-c :send-req send-req})

    (log/debugf "[%s] [-> %s %s] start handle callee message"
                node-id callee-node-id req-id)
    (let [handle-fn (->> {:node node :req-id req-id :out-c out-c}
                         (partial remote-call-client-handle-callee-msg))]
      (add-sub node :resp handle-fn msg-meta))

    (log/debugf "[%s] [-> %s %s] initiate request :req :xnfun/call"
                node-id callee-node-id req-id)
    (send-req {:typ :xnfun/call :data [[fun-name params] req-meta]})
    p))

(defn submit-remote-call!
  [node fun-name params
   & {:as options :keys [req-meta out-c match-node-fn]}]
  (let [callee-node-id (select-remote-node node {:match-fn match-node-fn})]
    (submit-remote-call-to-node! node fun-name params callee-node-id options)))

(defn serve-remote-call!
  [{:as node :keys [node-id]}]
  (let [handle-fn (partial remote-call-server-handle-caller-msg node)]
    (add-sub node :req handle-fn)))

(comment

  (defn n [v]
    (when v (try (clean-node! v) (catch Exception _)))
    (-> {:node-id "node-0"
         :node-options {:hb-options
                        {:hb-interval-ms 3000}}}
        make-node))

  (defonce n0 (atom nil))

  (defn restart! []
    (when-let [n0 @n0] (clean-node! n0))
    (swap! n0 n))

  (defn start-n0! []
    (doto @n0
      (start-node-link!)
      (start-heartbeat!)
      (start-heartbeat-listener!)))

  (defn stop-n0! []
    (doto @n0
      (stop-heartbeat-listener!)
      (stop-heartbeat!)
      (stop-node-link!)))

  (restart!)

  (start-node-link! @n0)
  (stop-node-link! @n0)

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

  (add-function! @n0 "echo"
                 (fn [_ {:as opt :keys [in-c out-c]
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
                     @hb)))

  (def c1 (submit-call! @n0 "echo" nil {:req-meta {:timeout-ms 60000
                                                   :hb-interval-ms 3000
                                                   :hb-lost-ratio 2.5}}))
  (go-loop [d (<!! (:out-c c1))]
    (when d
      (println "recv: " d)
      (recur (<!! (:out-c c1)))))
  (future-cancel (:running-future c1))
  (>!! (:in-c c1) {:typ :xnfun/to-callee :data "hello world"})
  (>!! (:in-c c1) {:typ :xnfun/cancel :data nil})

  (serve-remote-call! @n0)

  (submit-remote-call! @n0 "add" [[1 2]])

  (clean-node! @n0)

  (agent-error (-> @n0 :node-state :remote :nodes))
  ;
  )
