(ns lotuc.xnfun.core.node-utils)

(defmacro ensure-node-transport [node]
  (let [transport (gensym)]
    `(let [~transport @(-> ~node :node-state :local :transport)
           ~transport (and ~transport @~transport)]
       (when (nil? ~transport)
         (throw (ex-info "transport not created" {:node ~node})))
       (when (.closed? ~transport)
         (throw (ex-info "transport closed" {:node ~node})))
       ~transport)))

(defmacro send-msg
  ([node msg]
   `(xnp/send-msg (ensure-node-transport ~node) ~msg))
  ([node msg msg-meta]
   `(xnp/send-msg (ensure-node-transport ~node) (with-meta ~msg ~msg-meta))))

(defmacro add-sub
  ([node typ handle-fn]
   `(xnp/add-subscription
     (ensure-node-transport ~node)
     {:types [~typ] :handle-fn ~handle-fn}))
  ([node typ handle-fn subscription-meta]
   `(xnp/add-subscription
     (ensure-node-transport ~node)
     (with-meta
       {:types [~typ] :handle-fn ~handle-fn}
       ~subscription-meta))))
