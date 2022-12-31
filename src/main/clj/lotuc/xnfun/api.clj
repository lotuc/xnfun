(ns lotuc.xnfun.api
  (:require [lotuc.xnfun.rpc.node :as n]
            [lotuc.xnfun.rpc.impl :as impl]))

(defn start-node
  [{:as node :keys [node-id node-options]
    {:keys [heartbeat-interval-ms
            heartbeat-lost-ratio]} :node-options}]
  (impl/start-node node))

(defn stop-node [node]
  (impl/stop-node node))

(defn add-function [node fun-name fun & opt]
  (n/add-function node fun-name fun opt))

(defn call-function
  "
  Optional Arguments
  - `options.out-c`
  - `req-meta`
    - `:node-id`: call to given node.
    - `:with-stacktrace`
    - `:timeout-ms`
    - `:hb-interval-ms`
    - `:hb-lost-ratio`

  Returns {:keys [in-c out-c res-future]}
  "
  [node fun-name fun-params
   & {:as options
      :keys [req-meta out-c]
      {:keys [node-id]} :req-meta}]
  (impl/call-function node fun-name fun-params options))

(defn call
  [node fun-name fun-params & {:as options}]
  (let [r (impl/call-function node fun-name fun-params options)
        f (:res-future r)]
    (future (:res @f))))
