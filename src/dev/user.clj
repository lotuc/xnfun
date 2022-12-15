(ns user
  (:require [lotuc.xnfun.api :as xnfun]
            [lotuc.xnfun.utils :refer [max-arity]]
            [lotuc.xnfun.rpc.internal-funcs :refer [fun-echo]]))

(def n (xnfun/start-node {}))

(xnfun/stop-node n)

(fun-echo "abc")
(xnfun/add-function n "add" fun-echo)

(max-arity fun-echo)
