(ns lotuc.xnfun.core.transport)

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
