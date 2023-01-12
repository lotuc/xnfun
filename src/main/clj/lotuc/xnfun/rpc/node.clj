(ns lotuc.xnfun.rpc.node
  (:require
   [chime.core :as chime]
   [clojure.core.async :refer [<! >!! chan close! dropping-buffer go-loop]]
   [clojure.tools.logging :as log]
   [lotuc.xnfun.utils :refer [max-arity]])
  (:import
   (java.time Instant)))

(def ^:dynamic *now-ms* #(System/currentTimeMillis))

(def ^:private merge-ignores-nil
  (partial merge-with (fn [a b] (if (nil? b) a b))))

(def ^:private default-hb-options
  {:hb-interval-ms 300000
   :hb-lost-ratio 2.5})

(def ^:private default-mqtt-options
  {:broker "tcp://127.0.0.1:1883"
   :connect-options {:max-in-flight 1000 :automatic-reconnect true}})

(defn make-node
  "Create a node."
  [& {:keys [node-id node-options]}]
  (let [node-id (or node-id (str (random-uuid)))]
    {:node-id      node-id

     :node-options
     (-> (or node-options {})
         (merge-ignores-nil default-hb-options)
         (update :xnfun/link
                 #(or % {:xnfun/module 'xnfun.mqtt
                         :mqtt-config (update default-mqtt-options :client-id node-id)})))

     :node-state
     {:cleanup          (atom #{})

      ;; as caller, the registed nodes
      ;; node-id -> hb-at, functions
      :worker-nodes     (atom {})
      :waiting-promises (atom {})

      ;; as worker
      ;; supported functions
      :functions        (atom {})
      ;; handling requests
      :running-requests (atom {})}}))

(defn- on-heartbeat- [nodes node-id & {:keys [functions]}]
  (let [hb-at     (*now-ms*)
        prev-hb   (-> nodes :heartbeat (get node-id))
        node-funs (if functions
                    ;; if functions given, update it
                    functions
                    ;; if it's an old node, keep the functions
                    ;; else initialize it with empty function set
                    (if (nil? prev-hb) {} (get nodes :functions {})))]
    (assoc nodes node-id {:hb-at hb-at :functions node-funs})))

(defn calc-heartbeat-msg [{:as node
                           {:keys [functions] :as stat} :node-state}]
  {:functions (->> @functions
                   (map (fn [[k v]] [k (select-keys v [:arity])]))
                   (into {}))})

(defn heartbeat-lost? [{:as                    node
                        {:keys [hb-interval-ms hb-lost-ratio]} :node-options}
                       {:as worker-node
                        :keys [hb-at]}]
  (> (*now-ms*) (+ hb-at (* hb-interval-ms hb-lost-ratio))))

(defn on-heartbeat!
  [{:as                    node
    {:keys [worker-nodes]} :node-state}
   {:keys [node-id functions] :as stat}]
  (swap! worker-nodes #(on-heartbeat- % node-id stat)))

(defn remove-dead-workers!
  ([{:as                    node
     {:keys [hb-interval-ms hb-lost-ratio]} :node-options
     {:keys [worker-nodes]} :node-state}]
   (swap!
    worker-nodes
    #(->> %
          (filter (fn [[_ worker-node]] (not (heartbeat-lost? node worker-node))))
          (into {})))))

(defn find-worker-nodes [node]
  (-> node :node-state :worker-nodes deref))

(defn get-worker-node [node node-id]
  (-> (find-worker-nodes node) (get node-id)))

(defn select-worker-node [{:as                    node
                           {:keys [worker-nodes]} :node-state}
                          & {:keys [node-id function-name]}]
  (if node-id
    (let [{:keys [functions]} (@worker-nodes node-id)]
      (when (or (nil? function-name) (functions function-name))
        node-id))

    (let [node-ids
          (cond->> @worker-nodes
            function-name (filter (fn [[_ {:keys [functions]}]] (functions function-name)))
            true   (map (fn [[node-id _]] node-id)))]
      (if (empty? node-ids) nil (rand-nth node-ids)))))

(defn add-function [{{:keys [functions]} :node-state} fun-name fun
                    & {:keys [overwrite]
                       :or   {overwrite true}}]
  (swap!
   functions
   (fn [m]
     (let [func-arity (max-arity fun)]
       (when-not (#{1 2} func-arity)
         (throw (ex-info "invalid rpc function (arity not match)"
                         {:function-name fun-name :function fun})))
       (when-not overwrite
         (when (m fun-name)
           (->> {:function-name fun-name :function fun}
                (ex-info "function already exists")
                throw)))
       (-> m (assoc fun-name {:function fun :arity func-arity}))))))

(defn call-function
  "Call registered function with name.

  Arguments:
  - optional out-c: function reporting some data to caller
    - {:type :xnfun/hb}: long-running function heartbeat
    - {:type :msg :data ..}: message, also triggers heartbeat
  - optional in-c: caller send some signal to running function
    - {:type :xnfun/cancel}: softlly tells the function we'll cancel
  "
  [{{:keys [functions]} :node-state} fun-name params
   & {:keys [out-c in-c req-meta]}]
  (let [{:keys [function arity]} (-> @functions (get fun-name))]
    (when-not function (throw (ex-info "function not found"
                                       {:function-name fun-name
                                        :params params})))
    (if (= arity 2)
      (apply function [params {:out-c (or out-c (chan (dropping-buffer 1)))
                               :in-c  (or in-c  (chan (dropping-buffer 1)))
                               :req-meta req-meta}])
      (apply function [params]))))

(defn- swap-request-state-status!
  "state-key: `:waiting-promises`, `:running-requests`"
  [{:as node :keys [node-id node-state]}
   state-key req-id expected-status status]
  (let [state (node-state state-key)
        status-k [req-id :status]
        [prev-val new-val]
        (swap-vals! state #(cond-> %
                             (= expected-status (get-in % status-k))
                             (assoc-in status-k status)))]
    [prev-val new-val
     (and (= (get-in prev-val status-k) expected-status)
          (= (get-in new-val status-k)  status))]))

(defn fullfill-wait-function-call
  "Handling data from callee.

  Arguments:
  - `val.status`: `:ok`, `:err`, `:err-rpc`
  - `val.data`
  "
  [{:as node :keys [node-id] {:keys [waiting-promises]} :node-state}
   req-id val]
  (swap!
   waiting-promises
   (fn [m]
     (let [d (get m req-id)
           {:keys [cancel-timeout stop-on-hb-lost res-promise]} d]
       (when d
         (log/debugf "[%s] fullfilled [%s]: %s" node-id req-id val)
         (deliver res-promise val)
         (when cancel-timeout (.close cancel-timeout))
         (when stop-on-hb-lost (.close stop-on-hb-lost)))
       (when-not d
         (log/debugf "[%s] request not found [%s]: %s" node-id req-id val))
       (dissoc m req-id)))))

(defn- try-start-wait-for-function-call
  [{:as node :keys [node-id]
    {:keys [waiting-promises]} :node-state} req-id]
  (let [[_ new-waiting-promises updated]
        (swap-request-state-status!
         node :waiting-promises req-id :submitted :started)

        {{:as request
          {:keys [timeout-ms hb-interval-ms hb-lost-ratio]} :req-meta} :request}
        (get new-waiting-promises req-id)]
    (if-not updated
      new-waiting-promises
      (let [timeout-at
            (fn [time-ms reason]
              (chime/chime-at
               [(Instant/ofEpochMilli time-ms)]
               (fn [_]
                 (->> {:status :err-rpc :msg "timeout" :reason reason}
                      (fullfill-wait-function-call node req-id)))
               {:error-handler
                (fn [e] (log/warnf e "[%s][%s] error cancel run: %s" node-id req-id reason))}))

            cancel-timeout (timeout-at (+ (*now-ms*) timeout-ms) :timeout)

            do-hb
            (fn []
              (let [hb-lost-at (+ (*now-ms*) (* hb-lost-ratio hb-interval-ms))]
                {:hb-lost-at hb-lost-at
                 :stop-on-hb-lost
                 (do
                   (log/debugf "[%s][%s] heartbeat will lost in (* %s %s)ms"
                               node-id req-id hb-lost-ratio hb-interval-ms)
                   (timeout-at hb-lost-at :hb-lost))}))

            hb
            (fn []
              (when hb-interval-ms
                (let [d (do-hb)

                      [prev-waitings _]
                      (swap-vals!
                       waiting-promises
                       (fn [waitings]
                         (if (= (get-in waitings [req-id :status]) :started)
                           (update waitings req-id #(merge % d))
                           waitings)))]
                  (when-let [v (get-in prev-waitings [req-id :stop-on-hb-lost])]
                    (.close v)))))

            d (cond-> {:cancel-timeout cancel-timeout :hb hb}
                hb-interval-ms (merge (do-hb)))]

        (-> waiting-promises
            (swap-vals!
             (fn [waitings]
               (if-let [p (get waitings req-id)]
                 (assoc waitings req-id (merge p d))
                 waitings)))
            second)))))

(defn submit-wait-for-function-call
  [{:as node {:keys [waiting-promises]} :node-state}
   {:as request
    :keys [req-id]
    {:keys [timeout-ms]} :req-meta}]
  (let [timeout-ms (or timeout-ms 60000)
        request (-> request
                    (assoc-in [:req-meta :timeout-ms] timeout-ms))]
    (-> waiting-promises
        (swap!
         (fn [m]
           (if (get m req-id)
             m
             (->> {:res-promise (promise)
                   :status :submitted
                   :request request}
                  (assoc m req-id))))))
    (-> (try-start-wait-for-function-call node req-id)
        (get req-id)
        (select-keys [:res-promise :hb]))))

(defn- cleanup-function-call
  [{:as node :keys [node-id] {:keys [running-requests]} :node-state}
   req-id channels]
  (let [[_ vs updated] (swap-request-state-status!
                        node :running-requests req-id :started :cleaned)]
    (when updated
      (log/debugf "[%s][%s] cleanup function call" node-id req-id)
      (let [{:keys [in-c out-c stop-on-timeout stop-on-hb-lost] :as req}
            (get vs req-id)]
        (future
          (Thread/sleep 100)
          (close! out-c)
          (close! in-c)
          (doseq [c channels] (close! c))
          (when stop-on-timeout (.close stop-on-timeout))
          (when stop-on-hb-lost (.close stop-on-hb-lost))))
      (swap! running-requests #(dissoc % req-id)))))

(defn- try-start-function-call
  [{:as node :keys [node-id] {:keys [running-requests]} :node-state}
   req-id]
  (let [[_ new-running-requests updated]
        (swap-request-state-status!
         node :running-requests req-id :submitted :started)

        {:keys [running-future fun-name params in-c out-c req-meta]
         {:keys [hb-interval-ms hb-lost-ratio timeout-ms]} :req-meta
         {:keys [on-res on-exception]} :callbacks}
        (get new-running-requests req-id)]
    (if-not updated
      {:req-id req-id :running-future running-future :in-c in-c :out-c out-c}
      (let [in-c-internal (chan 1)
            out-c-internal (chan 1)

            ;; actually start run here
            running-future
            (future
              (try
                (let [r (->> {:in-c in-c-internal :out-c out-c-internal :req-meta req-meta}
                             (call-function node fun-name params))]
                  (when on-res (future (on-res r)))
                  r)
                (catch Exception e
                  (when on-exception (future (on-exception e)))
                  (throw e))
                (finally
                  (cleanup-function-call node req-id [in-c-internal out-c-internal]))))

            _
            (future
              (try
                @running-future
                (finally
                  (cleanup-function-call node req-id [in-c-internal out-c-internal]))))

            cancel-run
            (fn [reason]
              (when-not (future-done? running-future)
                (log/debugf "[%s][%s] cancelled on %s" node-id req-id reason)
                ;; for cancel, we forward msg first, wishing the function
                ;; will shutdown itself gracefullly.
                (let [f (future (->> {:type :xnfun/cancel :data {:reason reason}}
                                     (>!! in-c-internal)))]
                  (future-cancel running-future)
                  @f)))

            timeout-at
            (fn [time-ms reason]
              (chime/chime-at
               [(Instant/ofEpochMilli time-ms)]
               (fn [_] (cancel-run reason))
               {:error-handler
                (fn [e] (log/warnf e "[%s][%s] error cancel run: %s" node-id req-id reason))}))

            do-hb
            (fn []
              (let [hb-lost-at (+ (*now-ms*) (* hb-lost-ratio hb-interval-ms))]
                {:hb-lost-at hb-lost-at
                 :stop-on-hb-lost
                 (do (log/debugf "[%s][%s] heartbeat will lost in (* %s %s)ms"
                                 node-id req-id hb-lost-ratio hb-interval-ms)
                     (timeout-at hb-lost-at :hb-lost))}))

            hb
            (fn []
              (log/debugf "[%s][%s] on hb %s" node-id req-id hb-interval-ms)
              (when hb-interval-ms
                (let [d (do-hb)
                      [prev-requests _]
                      (swap-vals!
                       running-requests
                       (fn [reqs1]
                         (if (= (get-in reqs1 [req-id :status]) :started)
                           (update reqs1 req-id #(merge % d))
                           reqs1)))]
                  (when-let [v (get-in prev-requests [req-id :stop-on-hb-lost])]
                    (.close v)))))

            handle-caller-msg
            (fn [{:as d :keys [type]}]
              ;; Always blocking when forwarding message to callee
              (cond
                (= type :xnfun/cancel) (cancel-run (:reason d))
                :else (>!! in-c-internal d)))

            handle-callee-msg
            (fn [{:as d :keys [type]}]
              (log/debugf "[%s][%s] callee message: %s" node-id req-id d)
              ;; all message counts as heartbeat message.
              (future (hb))

              ;; Forward message, and always blocking when forwarding.
              (>!! out-c d))

            stop-on-timeout
            (do
              (log/debugf "[%s][%s] will timeout in %sms" req-id node-id timeout-ms)
              (timeout-at (+ (*now-ms*) timeout-ms) :timeout))

            d (cond-> {:stop-on-timeout stop-on-timeout
                       :running-future running-future}
                hb-interval-ms (merge (do-hb)))]

        (log/debugf "[%s][%s] waiting for callee message" node-id req-id)
        (go-loop [d (<! out-c-internal)]
          (when (some? d)
            (try
              (handle-callee-msg d)
              (catch Exception e
                (log/warnf e "[%s][%s] error handling msg from callee: %s" node-id req-id d)))
            (recur (<! out-c-internal))))

        (log/debugf "[%s][%s] waiting for caller message" node-id req-id)
        (go-loop [d (<! in-c)]
          (when (some? d)
            (try
              (handle-caller-msg d)
              (catch Exception e
                (log/warnf e "[%s][%s] error handling msg from caller: %s" node-id req-id d)))
            (recur (<! in-c))))

        (swap!
         running-requests
         (fn [reqs]
           (let [req (get reqs req-id)]
             (cond-> reqs
               req (assoc req-id (merge req d))))))

        {:req-id req-id :running-future running-future :in-c in-c :out-c out-c}))))

(defn submit-function-call*
  "Submit function to async run.

  Arguments
  - `options.out-c`: callee will send message (and heartbeat) throught this
     channel.
     - If not given, will create a dropping buffer channel
     - Notice that the out-c would block callee, so you should handle the message
       as soon as possible.
  - `options.timeout-ms`: Defaults to be `60_000`. Max timeout for function call
  - `options.hb-interval-ms`: Suggested hb interval for function if given
  - `options.hb-lost-ratio`: Defaults to be `2.5`
    If hb-interval-ms given, function call will be timeouted when no heartbeat
    recv for every (* `hb-interval-ms` `hb-lost-ratio`)

  Returns {:keys [running-future in-c out-c]}
  - `in-c`: send message to function call, {:keys [type, data]}
    - `type`
      - `cancel`: the function can do some gracefully shutdown operations.
        Notice the return value of function will not be available for caller
        after the function recvs cancel, because the caller already abandons.
  - `out-c`: message from the callee, {:keys [type, data]}
    - `type`
      - `hb`: heartbeat message
      - `msg`: message to caller
  "
  [{:as node :keys [node-id]
    {:keys [running-requests]} :node-state}
   fun-name params
   & {:as options
      :keys [req-meta callbacks out-c]
      {:keys [timeout-ms hb-interval-ms hb-lost-ratio req-id]} :req-meta
      {:keys [on-res on-exception]} :callbacks}]
  (let [req-id        (or req-id (str (random-uuid)))
        timeout-ms    (or timeout-ms 60000)
        hb-lost-ratio (or hb-lost-ratio 2.5)
        req-meta      (-> req-meta
                          (assoc :req-id req-id)
                          (assoc :timeout-ms timeout-ms)
                          (assoc :hb-lost-ratio hb-lost-ratio))]
    (swap!
     running-requests
     (fn [reqs]
       (let [req (get reqs req-id)]
         (if req
           reqs
           (->> {:status :submitted
                 :submit-at (*now-ms*)
                 :fun-name fun-name
                 :params params
                 :req-meta req-meta
                 :callbacks callbacks
                 :in-c  (chan 1)
                 :out-c (or out-c (chan (dropping-buffer 1)))}
                (assoc reqs req-id))))))

    (try-start-function-call node req-id)))

(defn submit-function-call
  [node fun-name params
   & {:as options
      :keys [req-meta callbacks out-c]
      {:keys [timeout-ms hb-interval-ms hb-lost-ratio req-id]} :req-meta
      {:keys [on-res on-exception]} :callbacks}]
  (-> (submit-function-call* node fun-name params options)
      :running-future))
