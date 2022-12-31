(ns examples.example1
  (:require [lotuc.xnfun.api :as xn]
            [lotuc.xnfun.rpc.sample-funcs :as sample-funcs]))

;;; Start node n0 and register add function to n0
(def n0 (xn/start-node {}))
(xn/add-function n0 "add" sample-funcs/f-add)

;;; Start node n1 and register sub function to n1
(def n1 (xn/start-node {}))
(xn/add-function n1 "sub" sample-funcs/f-sub)

;;; call n0 to n1 (deref the result future)
@(xn/call n0 "sub" [42 24])

;;; call n1 to n0
@(xn/call n1 "add" [42 24])

;;; Stop nodes
(xn/stop-node n0)
(xn/stop-node n1)
