(ns lotuc.xnfun.api
  (:require [lotuc.xnfun.core.node :as n]))

(declare stop-node)

(defn start-node
  "Check [[lotuc.xnfun.core.node/make-node]] for `node-options`'s details.

  Returns the node to operate on.
  "
  [{:as node :keys [node-id node-options]}]
  (let [r (n/make-node node)]
    (try
      (doto r
        (n/start-node-transport!)
        (n/start-heartbeat!)
        (n/serve-remote-call!)
        (n/start-heartbeat-listener!))
      (catch Exception e
        (stop-node r)
        (throw e)))))

(defn stop-node
  "Stop the `node` started with [[start-node]]."
  [node]
  (doto node
    (n/stop-heartbeat-listener!)
    (n/stop-serve-remote-call!)
    (n/stop-heartbeat!)
    (n/stop-node-transport!)))

(defn add-function
  "Register a function to given `node`.

  Arguments:

  - `node`: The node started with [[start-node]]
  - `fun-name`: The function name
  - `fun`: Can be either a one argument or two arguments function
      - `(fn [arg] ...)`
      - `(fn [arg {:as call-options :keys [in-c out-c req-meta]}] ...)`
          - `:in-c`: A channel through which callee can receive message sent from
            caller. Check [[call-function]] for details.
          - `:out-c`: A channel through which callee can send message to caller.
            Check [[call-function]] for details.
          - `:req-meta`: `{:keys [req-id timeout-ms hb-interval-ms hb-lost-ratio]}`,
            all of these fields are optional
              - `:req-id`: The call request's id
              - `:timeout-ms`: The call requests' timeout (in ms)
              - `:hb-interval-ms`: The expected heartbeat interval (in ms)
              - `:hb-lost-ratio`: If the `:hb-interval-ms` given, the caller will
                consider the request dead when no heartbeat received in
                `(* hb-interval-ms hb-lost-ratio)`.
  - `opt`: `{:keys [overwrite]}`
      - `overwrite`: defaults to be `true`; if specified as `false` and the given
        name is already registered, the function will throw.
  "
  [node fun-name fun & opt]
  (n/add-function! node fun-name fun opt))

(defn call-function
  "Call the function through given node.

  Returns `{:keys [res-promise request]}`

  - `:res-promise`: A Clojure promise for the call result `{:keys [status data]}`.
      - `:data` is the value of function call return value if `:status` is `:ok`.
      - `:data` is the data representing a call failure if `:status` is not `:ok`.
        There are now three types of error `:status`:
          - `:err` The callee function's execution error (mean user code erro, no
            matter if it's executed locally or remotelly).
          - `:xnfun/err` The caller side XNFUN node error (ex, timeout)
          - `:xnfun/remote-err` The callee side XNFUN node error (ex, timeout)
  - `:request`: The call request `{:keys [fun-name params req-meta in-c out-c]}`.
      - `req-meta`: check [[add-function]]'s `fun` for details.
      - `in-c`: A channel through which caller can send message `{:keys [typ data}`
        to callee. Now there are two types (encoded in `:typ`) of data can be sent:
          - `:xnfun/to-callee`: The message is forward directly to callee.
          - `:xnfun/cancel`: The message is forward to callee as well, the callee
            can do a gracefully shutdown on it. XNFUN will also forcelly cancel the
            request.
      - `out-c`: A channel through which caller can receive message
        `{:keys [typ data]}` from callee. Now there are two types (encoded in
        `:typ`) of messages:
          - `:xnfun/to-caller`: The message is forward from callee directly
          - `:xnfun/hb`: The message is forward from callee as well, callee can send
            and type of `:data` as the heartbeat content. XNFUN will ignore data
            part, and just reset the internal heartbeat check timer.
  "
  [node fun-name fun-params
   & {:as options :keys [req-meta out-c]}]
  (n/submit-remote-call! node fun-name fun-params options))

(defn call
  "This is a wrapper of [[call-function]] for the simplest use case.

  Consider the same as `(future (apply fun fun-params))` in which `fun`
  represents the function associated with function name `fun-name`.
  "
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
