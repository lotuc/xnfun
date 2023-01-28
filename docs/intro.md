# Introduction to xnfun (Cross Node Function)

`xnfun` is designed around the abstraction of node. Consider a node as the
container of a group of functions, it manages local & remote function execution
states; and nodes are inter-connected through transports.

Checkout the source code at [lotuc/xnfun](https://github.com/lotuc/xnfun) and
view api documentation at [xnfun](https://lotuc.org/xnfun).

Next we will go through two kinds of functions supported.

- Unary RPC
- Bidirectional RPC

## Unary RPC

At present, we only implemented the MQTT transport. So take it as an example.

First, start a MQTT broker.

```sh
cat > mosquitto.conf <<EOF
allow_anonymous true
listener 1883
EOF

docker run --rm -it -v `pwd`/mosquitto.conf:/mosquitto/config/mosquitto.conf \
    -p 1883:1883 eclipse-mosquitto

# Now you've started a broker at 1883 which can be connected anonymously.
```

Next start a Clojure repl which loads the dependency.

```sh
cat > deps.edn <<EOF
{:deps {lotuc.xnfun/xnfun {:git/sha "1d0b91d316260bce3bd5cc04504421b9c48eac3a"
                           :git/url "https://github.com/lotuc/xnfun.git"}}}
EOF

clojure
```

From the repl, we start two `xnfun` nodes and register some functions to each.

```clojure
(require '[lotuc.xnfun.api :refer [start-node add-function call]])

(def n0 (start-node {:transport
                     {:xnfun/module 'xnfun.mqtt
                      :mqtt-topic-prefix ""
                      :mqtt-config
                      {:broker "tcp://127.0.0.1:1883"
                       :connect-options {:max-in-flight 1000
                                         :auto-reconnect true}}}}))
;; Actually, the mqtt at 127.0.0.1:1883 is the default transport.
;; We start another node directly.
(def n1 (start-node {}))

;; Register function "add" to n0
(add-function n0 "add" (fn [[a b]] (+ a b)))

;; Register function "sub" to n1
(add-function n1 "sub" (fn [[a b]] (- a b)))
```

Since the node `n0` and `n1` are connected to the same MQTT transport, the tow
nodes are actually inter-connected. You can call to each other.

```clojure
@(call n0 "sub" [4 2])
@(call n1 "add" [4 2])

;; and sure you can call to the function registered to the node itself
@(call n0 "add" [4 2])
@(call n1 "sub" [4 2])
```

## Bidirectional RPC

This mode is like the [gRPC Bidirectional streaming RPC](https://grpc.io/docs/what-is-grpc/core-concepts/#bidirectional-streaming-rpc).

Create a function that supports bidirectional call, now the function got a
second argument, you can retieve two channels `in-c` and `out-c`. From `in-c`
you can listen for `caller`'s message, and via `out-c` you can send message to
caller. Check [lotuc.xnfun.api/add-function](https://lotuc.org/xnfun/lotuc.xnfun.api.html#var-add-function)
for details.

```clojure
(require '[clojure.core.async :refer [<! go-loop put!]])

(defn echo-server
  [arg {:keys [in-c out-c]
        {:as req-meta :keys [req-id hb-interval-ms]} :req-meta}]
  (let [p (promise)]
    ;; Keep heartbeat.
    (future (loop []
              (put! out-c {:typ :xnfun/hb})
              (when (= :timeout (deref p hb-interval-ms :timeout))
                (recur))))
    (go-loop []
      (when-let [{:keys [typ data] :as d} (<! in-c)]
        (case typ
          :xnfun/cancel
          (deliver p :cancel) ; quit

          :xnfun/to-callee
          (do (put! out-c {:typ :xnfun/to-caller :data data})
              (recur))

          (deliver p {:unkown d}))))
    @p
    arg))
```

Register the function to node, say `n1`

```clojure
(add-function n1 "echo-server" echo-server)
```

Now, try call it from node `n0`

```clojure
(require '[lotuc.xnfun.api :refer [call-function]])

(def r (call-function n0 "echo-server" "hello world"
                      {:req-meta {:timeout-ms 3600000
                                  :hb-interval-ms 3000
                                  :hb-lost-ratio 2}}))

;; retrieve channels from the response
(def in-c  (get-in r [:request :in-c]))
(def out-c (get-in r [:request :out-c]))
;; Get the promise waiting for function call result
(def resp (:res-promise r))

;; Setup a go-loop waiting for echo response
(go-loop [{:as v :keys [typ data]} (<! out-c)]
  (if v (do (when (= typ :xnfun/to-caller)
              (println "!!!" data))
            (recur (<! out-c)))
      (println "quit")))

;; Send message to callee
(put! in-c {:typ :xnfun/to-callee :data "hi!"})

;; Stop the function
(put! in-c {:typ :xnfun/cancel :data nil})

;; And check the function call result
@resp
```
