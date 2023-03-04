(ns lotuc.xnfun.protocols)

(defprotocol XNNodeLocalState
  (node-futures [_])
  (node-transport [_])
  (node-hb-timer [_])
  (node-functions [_]
    "atom of {fun-name: {function arity}}")
  (node-rpc-listener [_]))

(defprotocol XNNodeLocal
  (add-function!
    [_ fun-name fun]
    [_ fun-name fun {:as opts :keys [overwrite]}]
    "Function `fun` should be either

    - arity 1: call by `(fun params)`
    - arity 2: call by `(fun params opts)`")

  (call-function!
    [_ fun-name params]
    [_ fun-name params {:as opts :keys [in-c out-c req-meta]}]
    "Issue a direct call to function of name `fun-name` registered.

     Channel's data should be format of `{:keys [typ data]}`

     - `out-c` (optional): function can send message to caller via the channel
         - `:typ=:xnfun/hb`: function heartbeat
         - `:typ=:xnfun/to-caller`: message to caller
     - `in-c` (optional): caller send some signal to callee
         - `:typ=:xnfun/cancel`: cancellation message.
         - `:typ=:xnfun/to-callee`: message to callee.

     `req-meta` fields (all of them are optional):

     | req-id         |
     | timeout-ms     |
     | hb-interval-ms |
     | hb-lost-ratio  |
     | caller-node-id |
     | callee-node-id |
     ")

  (submit-call!
    [_ fun-name params]
    [_ fun-name params {:as opts :keys [out-c req-meta]}]
    "Submit a function call request.

     - `out-c`: callee will send message (and heartbeat) throught this channel.
         - If not given, will create a dropping buffer channel
         - Notice that the out-c may block callee, so you should handle the
           message as soon as possible.
     - `req-meta`: Check [make-req-meta]] for details.

     `req-meta`

     | req-id         | optional, will generate a random one if absent |
     | timeout-ms     | optional, will not be timeouted if absent      |
     | hb-interval-ms | optional                                       |
     | hb-lost-ratio  | optional                                       |
     | caller-node-id | optional                                       |
     | callee-node-id | current node's id                              |

    Returns:

    | req   |
    | in-c  |
    | out-c |
    | fut   |

    `req`

    | req-meta |
    | fun-name |
    | params   |
    ")
  (get-call [_ req-id]
    "Returns the same of `submit-call!`")

  (submit-promise!
    [_ {:as req :keys [req-meta]}]
    "Submit a promise to node.

    Returns:

    | req         |                    |
    | hb          | triggers heartbeat |
    | res-promise |                    |
    ")
  (get-promise [_ req-id]
    "Returns the same of `submit-promise!`")
  (fullfill-promise! [_ req-id {:as r :keys [status data]}]
    "fullfill promise of given request.

     Arguments:

     - `:status`: `:ok` or `:xnfun/err` or `:xnfun/remote-err` or `:err`
         - When error is assured triggered by the user's function code, no
           matter it runs locally or remotely, the status should be `:err`
         - When error is not assured to be triggered by user's function code:
             - `:xnfun/err`: Error occurs locally
             - `:xnfun/remote-err`: Error occurs remotely
     - `:meta`: the call metadata.
     - `:data`: If `status=:ok`, means the fullfilled data; else the data
       describing the error."))

(defprotocol XNNodeRemoteState
  (node-remote-nodes [_])
  (node-hb-listener [_])
  (node-promises [_]))

(defprotocol XNNodeRemote
  "Managing remote nodes."

  (start-remote! [_])
  (stop-remote! [_])

  (start-node-transport! [_])
  (stop-node-transport! [_])

  (start-heartbeat! [_])
  (stop-heartbeat! [_])

  (start-heartbeat-listener! [_])
  (stop-heartbeat-listener! [_]))

(defprotocol XNNodeRemoteCall
  (start-serve-remote-call! [_])

  (stop-serve-remote-call! [_])

  (submit-remote-call!
    [_ fun-name params]
    [_ fun-name params {:as options :keys [req-meta out-c match-node-fn]}]))

(defprotocol XNFunTransport
  "All messages sending and receiving are the shape of:

  ```Clojure
  {:keys [typ data}
  ```

  There are 3 types of data, their essential metadata are:

  - `typ=:hb`: `{:keys [node-id]}`
  - `typ=:req`: `{:keys [caller-node-id callee-node-id req-id}}`
  - `typ=:resp`: `{:keys [caller-node-id callee-node-id req-id}}`

  We expect the subscription's (created by [[add-subscription]]) `handle-fn`
  receives the exact message the [[send-msg]] sent, except for the message's
  metadata, for the metata, we **expect** only the uppon documented parts are
  reserved.
  "

  (send-msg
    [_ {:as msg :keys [typ data]}]
    "Send given message to transport base node's info.

    Additional info is encoded with metadata of `msg`.")

  (add-subscription
    [_ {:as subscription :keys [types handle-fn]}]
    "Subscribe for message and handle it with `handle-fn`.

    - `types`: the message types the `handle-fn` supports
    - `handle-fn`: `(fn [{:as msg :keys [typ data}] ...)`

    Additional info is encoded within metadata of `subscription`.

    If `types` contains `:req`, we exepct the following metadata:

    - `:caller-node-id` (optional): If given, only listen to this node's calling
      request
    - `:req-id` (optional): If given, only listen to this id's request

    If `types` contains `:resp`, we expect the following metadata:

    - `:req-id` (required): The response belong to this request.
    - `:callee-node-id` (optional): If given, means that we only handle the
      `:resp` from this node.

    Additional info is encoded within metadata of `msg`.

    Returns a zero argument function that unsubscribes the subscription.")

  (remove-subscription
    [_ {:as subscription :keys [types handle-fn]}]
    "Remove the `subscription` added by [[add-subscription]]")

  (closed?
    [_]
    "If the transport closed.")

  (close!
    [_]
    "Close the transport."))
