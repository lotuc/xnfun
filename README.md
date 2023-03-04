# XNFUN (Cross-Node Functions)

## Documentation

* [Introduction to xnfun](https://lotuc.org/xnfun/intro.html)
* [API Docs](https://lotuc.org/xnfun/lotuc.xnfun.api.html)

## Sample

All the samples assume a running MQTT broker at `tcp://localhost:1883` with no credentials.

- [example1.clj](./src/dev/clj/examples/example1.clj): Unary RPC call
- [example2.clj](./src/dev/clj/examples/example2.clj): Bidirectional RPC call

```clojure
(require '[lotuc.xnfun.api :refer [start-node add-function call]])

;; Start node and register function to node

(def n0 (start-node {:node-options {:hb-interval-ms 3000}}))
(add-function n0 "add" (fn [[a b]] (+ a b)))

(def n1 (start-node {:node-options {:hb-interval-ms 3000}}))
(add-function n1 "sub" (fn [[a b]] (- a b)))

;; Call functions

@(call n0 "add" [4 2])
@(call n0 "sub" [4 2])

@(call n1 "add" [4 2])
@(call n1 "sub" [4 2])
```
