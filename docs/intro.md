# xnfun (Cross Node Function)

Using xnfun, you just start a node, it will connects to others. Register
functions for other nodes' usage. Or just call other node's function from your
started node.

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

Checkout the source code at [lotuc/xnfun](https://github.com/lotuc/xnfun) and
view api documentation at [xnfun](https://lotuc.org/xnfun).
