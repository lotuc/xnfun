(ns lotuc.xnfun.core.node-impl
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

  - [[make-node]] creates `XNNode`
  "

  (:require
   [clojure.core.async :refer [<!! <! >! >!! go-loop]]
   [clojure.pprint :as pp]
   [lotuc.xnfun.core.node-defaults :as defaults]
   [lotuc.xnfun.core.node-impl-local :as impl-local]
   [lotuc.xnfun.core.node-impl-remote :as impl-remote]
   [lotuc.xnfun.core.node-impl-remote-call :as impl-remote-call]
   [lotuc.xnfun.protocols :as xnp])
  (:import
   [java.io Writer]))

(defrecord XNNode [node-id node-options node-state])

(defmethod print-method XNNode [it ^Writer w]
  (.write w (format "#<XNNode %s>" (:node-id it))))

(defmethod pp/simple-dispatch XNNode [it]
  (print-method it *out*))

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
    :transport    (atom nil)
    :rpc-listener (atom nil)}

   :remote
   {:hb-listener (atom nil)
    :nodes       (agent {})
    :promises    (atom {})}})

(defn make-node
  "Create a node.

  A node contains:

  - `:node-id`: The global identifier for given connection transport.
  - `:node-options`: The options specified for given node. Check
    [[make-node-options]] for details.
  - `:node-state`: The mutable state for this node. Check [[make-node-state]]
    for details."
  [& {:keys [node-id node-options]}]
  (->XNNode
   (or node-id (str (random-uuid)))
   (defaults/make-node-options node-id node-options)
   (make-node-state)))

(extend-type XNNode
  xnp/XNNodeLocalState
  (node-futures [node] (get-in node [:node-state :local :futures]))
  (node-transport [node] (get-in node [:node-state :local :transport]))
  (node-hb-timer [node] (get-in node [:node-state :local :hb-timer]))
  (node-functions [node] (get-in node [:node-state :local :functions]))
  (node-rpc-listener [node] (get-in node [:node-state :local :rpc-listener])))

(extend-type XNNode
  xnp/XNNodeLocal
  (add-function!
    ([node fun-name fun]
     (impl-local/add-function! node fun-name fun))
    ([node fun-name fun {:as opts :keys [overwrite]}]
     (impl-local/add-function! node fun-name fun opts)))
  (call-function!
    ([node fun-name params]
     (impl-local/call-function! node fun-name params))
    ([node fun-name params {:as opts :keys [out-c in-c req-meta]}]
     (impl-local/call-function! node fun-name params opts)))

  (submit-call!
    ([node fun-name params]
     (impl-local/submit-call! node fun-name params))
    ([node fun-name params {:as opts :keys [req-meta out-c]}]
     (impl-local/submit-call! node fun-name params opts)))
  (get-call [node req-id]
    (impl-local/get-call node req-id))

  (submit-promise!
    [node {:as req :keys [req-meta]}]
    (impl-local/submit-promise! node req))
  (fullfill-promise!
    [node req-id {:as r :keys [status data]}]
    (impl-local/fullfill-promise! node req-id r))
  (get-promise
    [node req-id]
    (impl-local/get-promise node req-id)))

(comment
  (def n (make-node))
  (xnp/node-functions n)
  (xnp/add-function! n :add (fn [[x y]] (+ x y)))
  (xnp/add-function! n :add' (fn [[x y]] (Thread/sleep 5000) (+ x y)))
  (xnp/call-function! n :add [2 3])
  (def r (xnp/submit-call! n :add' [2 3]))
  (xnp/get-call n (get-in r [:req :req-meta :req-id]))
  @(:fut r)

  (def p0 (xnp/submit-promise! n {:req-meta {}}))
  (xnp/fullfill-promise!
   n (get-in p0 [:req :req-meta :req-id]) {:status :ok :data 2})
  (xnp/fullfill-promise!
   n (get-in p0 [:req :req-meta :req-id]) {:status :err :data {:reason "err"}})
  (xnp/get-promise n (get-in p0 [:req :req-meta :req-id]))

  (def p1 (xnp/submit-promise! n {:req-meta {:timeout-ms 1000}}))
  (def p2 (xnp/submit-promise! n {:req-meta {:hb-interval-ms 1000}}))
  ((:hb p2))
  (xnp/get-promise n (get-in p2 [:req :req-meta :req-id])))

(extend-type XNNode
  xnp/XNNodeRemoteState
  (node-remote-nodes [node] (get-in node [:node-state :remote :nodes]))
  (node-hb-listener [node] (get-in node [:node-state :remote :hb-listener]))
  (node-promises [node] (get-in node [:node-state :remote :promises])))

(extend-type XNNode
  xnp/XNNodeRemote
  (start-remote! [node]
    (try
      (doto node
        xnp/start-node-transport!
        xnp/start-heartbeat-listener!
        xnp/start-heartbeat!)
      (catch Exception e
        (xnp/stop-remote! node)
        (throw e))))
  (stop-remote! [node]
    (doto node
      xnp/stop-heartbeat!
      xnp/stop-heartbeat-listener!
      xnp/stop-node-transport!))

  (start-node-transport! [node]
    (impl-remote/start-node-transport! node) node)
  (stop-node-transport! [node]
    (impl-remote/stop-node-transport! node) node)

  (start-heartbeat! [node]
    (impl-remote/start-heartbeat! node) node)
  (stop-heartbeat! [node]
    (impl-remote/stop-heartbeat! node) node)

  (start-heartbeat-listener! [node]
    (impl-remote/start-heartbeat-listener! node) node)
  (stop-heartbeat-listener! [node]
    (impl-remote/stop-heartbeat-listener! node) node))

(comment

  (def n (make-node {:node-options
                     {:hb-options
                      {:hb-interval-ms 1000}}}))
  (xnp/start-node-transport! n)
  (xnp/stop-node-transport! n)

  (xnp/start-heartbeat! n)
  (xnp/stop-heartbeat! n)

  (xnp/start-heartbeat-listener! n)
  (xnp/stop-heartbeat-listener! n)

  (xnp/start-remote! n)
  (xnp/stop-remote! n)

  ;;
  )

(extend-type XNNode
  xnp/XNNodeRemoteCall
  (start-serve-remote-call! [node]
    (impl-remote-call/start-serve-remote-call! node))
  (stop-serve-remote-call! [node]
    (impl-remote-call/stop-serve-remote-call! node))
  (submit-remote-call!
    ([node fun-name params]
     (impl-remote-call/submit-remote-call! node fun-name params))
    ([node fun-name params {:as options :keys [req-meta out-c match-node-fn]}]
     (impl-remote-call/submit-remote-call! node fun-name params options))))

(comment
  (def n (make-node {:node-options
                     {:hb-options
                      {:hb-interval-ms 30000}}}))

  (xnp/stop-remote! n)

  (defn add [[x y]] (+ x y))
  (defn echo [_ {:as opt :keys [in-c out-c]
                 {:keys [hb-interval-ms hb-lost-ratio]} :req-meta}]
    (let [r (promise)]
      (go-loop []
        (when (= :timeout (deref r hb-interval-ms :timeout))
          (>! out-c {:typ :xnfun/hb :data nil})
          (recur)))
      (go-loop []
        (when-let [{:as d :keys [typ data]} (<! in-c)]
          (println "!!! recv" d)
          (case typ
            :xnfun/cancel (deliver r :stopped)
            :xnfun/to-callee (>! out-c {:typ :xnfun/to-caller :data data})
            (>! out-c {:typ :xnfun/to-caller :data {:unkown d}}))
          (recur)))
      @r))

  (xnp/add-function! n "add" add)
  (xnp/call-function! n "add" [1 2])

  (def c0 (xnp/submit-call! n "add" [1 2]))

  (xnp/add-function! n "echo" echo)

  (def c1 (->> {:req-meta {:timeout-ms 60000
                           :hb-interval-ms 3000
                           :hb-lost-ratio 2.5}}
               (xnp/submit-call! n "echo" nil)))
  (go-loop []
    (when-let [d (<! (:out-c c1))]
      (println "recv: " d)
      (recur)))

  (future-cancel (:fut c1))
  (>!! (:in-c c1) {:typ :xnfun/to-callee :data "hello world"})
  (>!! (:in-c c1) {:typ :xnfun/cancel :data nil})

  (xnp/start-serve-remote-call! @n0)
  (xnp/stop-serve-remote-call! @n0)

  (def p2 (xnp/submit-remote-call! @n0 "add" [[1 2]]))
  (def p3 (->> {:req-meta {:timeout-ms 60000
                           :hb-interval-ms 3000
                           :hb-lost-ratio 2.5}}
               (xnp/submit-remote-call! @n0 "echo" nil)))
  (go-loop [d (<!! (:out-c (:request p3)))]
    (when d
      (println "recv: " d)
      (recur (<!! (:out-c (:request p3))))))

  (>!! (:in-c (:request p3)) {:typ :xnfun/to-callee :data "hello world"})
  (>!! (:in-c (:request p3)) {:typ :xnfun/cancel :data "cancel"})
  ;
  )
