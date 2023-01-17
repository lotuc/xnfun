(ns lotuc.xnfun.api
  (:require [lotuc.xnfun.rpc.nodev2 :as n]))

(defn start-node
  [{:as node :keys [node-id node-options]}]
  (doto (n/make-node node)
    (n/start-node-link!)
    (n/start-heartbeat!)
    (n/serve-remote-call!)
    (n/start-heartbeat-listener!)))

(defn stop-node [node]
  (doto node
    (n/stop-heartbeat-listener!)
    (n/stop-serve-remote-call!)
    (n/stop-heartbeat!)
    (n/stop-node-link!)))

(defn add-function [node fun-name fun & opt]
  (n/add-function! node fun-name fun opt))

(defn call-function
  [node fun-name fun-params
   & {:as options :keys [req-meta out-c]}]
  (n/submit-remote-call! node fun-name fun-params options))

(defn call
  [node fun-name fun-params & {:as options}]
  (let [r (call-function node fun-name fun-params options)
        p (:res-promise r)]
    (future
      (let [{:as r :keys [status data]} @p]
        (cond
          (= status :ok) data
          (= status :err) (throw (ex-info "execution error" r))
          (= status :xnfun/err) (throw (ex-info "local error" r))
          (= status :xnfun/remote-err) (throw (ex-info "remote error" r))
          :else (throw (ex-info "unkown response" r)))))))
