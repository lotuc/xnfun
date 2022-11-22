(ns lotuc.remote.mqtt.rpc
  (:require [lotuc.remote.mqtt.rpc-internal :as ri]))

(defn make-node
  "create node
  node-id
  roles: set of :caller, :worker
  "
  [& opts]
  (ri/registry-make-node opts))

(defonce ^:dynamic *node* (make-node))

(defn node-start []
  (ri/registry-start *node*))

(defn node-stop []
  (ri/registry-stop *node*))

(defn node-add-worker-method [method func]
  (ri/registry-add-worker-method *node* method func))

(defn call [method params & {:as req-meta}]
  (ri/registry-rpc-call *node* method params req-meta))

#_(require '[lotuc.remote.mqtt.rpc-internal-funcs
             :refer [func-delayed-sum func-delayed-add]]
           '[lotuc.remote.node.registry :as r]
           '[clojure.tools.logging :as log])

#_(try (node-start)
       (node-add-worker-method "func-add" #'func-delayed-add)
       (node-add-worker-method "func-sum" #'func-delayed-sum)

       (Thread/sleep r/*heartbeat-interval-in-millis*)

       (print "func-add 1 2 ==>" (time @(call "func-add" [1 2])))
       (print "func-sum (range 10) ==>" (time @(call "func-sum" (range 10))))

       (print "func-add 4 2 ==>" (time @(call "func-add" [4 2] {:resp-meta true})))

       (catch Exception e (log/error e (str e)))
       (finally (node-stop)))
