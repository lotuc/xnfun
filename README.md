# XNFUN (Cross-Node Functions)

```clojure
(require '[lotuc.xnfun.api :refer [start-node add-function call]])

;; make sure mqtt is run at localhost:1883 with no password

;; Start a node, default connects to mqtt broker at tcp://localhost:1883.
(def n0 (start-node {:node-options {:hb-interval-ms 3000}}))
;; Add a supported function to node n0
(add-function n0 "add" (fn [[a b]] (+ a b)))

;; Start another node likewise, within same process or another process.
(def n1 (start-node {:node-options {:hb-interval-ms 3000}}))
(add-function n1 "sub" (fn [[a b]] (- a b)))

;; Now you can call for functions
@(call n0 "add" [1 2]) ;; call from n0 to n0
@(call n0 "sub" [1 2]) ;; call from n0 to n1

@(call n1 "add" [1 2]) ;; call from n1 to n0
@(call n1 "sub" [1 2]) ;; call from n1 to n1
```
