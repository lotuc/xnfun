(ns lotuc.xnfun.core.link)

(defprotocol XNFunLink
  "All messages sending and receiving are the shape of:

  {:keys [typ data}

  There are 3 types of data, their metadata are:
  - `typ=:hb`: {:keys [node-id]}
  - `typ=:req`: {:keys [caller-node-id callee-node-id req-id}}
  - `typ=:resp`: {:keys [caller-node-id callee-node-id req-id}}

  We expect `sub-msg--handle-fn` receives the exact message the `send-msg`
  sent, except for the message's metadata, for the metata, we expect the uppon
  documented part is reserved.
  "

  (send-msg
    [_ {:as msg :keys [typ data]}]
    "Send given message to link base node's info.

    - `typ`: `:req`
      - metadata `:callee-node-id` is required.

    Additional info is encoded with metadata of `msg`.")

  (add-subscription
    [_ {:as subscription :keys [types handle-fn]}]
    "Subscribe for message and handle it with `handle-fn`.

    - `typ`: the message type the `handle-fn` supports
    - `handle-fn`: (fn [{:as `msg` :keys [typ data}] ...)

    Additional info is encoded within metadata of `handler`

    For `typ=:req`, the metadata
    - `:caller-node-id` (optional): If given, only listen to this node's calling
      request
    - `:req-id` (optional): If given, only listen to this id's request

    For `typ=:resp`, the metadata:
    - `:req-id` (required): The response belong to this request.
    - `:callee-node-id` (optional): If given, means we only handle the `:resp`
      from this node.

    Additional info is encoded within metadata of `msg`.

    Return: unsubscribe the subscription.")

  (remove-subscription
    [_ {:as subscription :keys [types handle-fn]}]
    "Remove the `subscription` added by [[add-subscription]]")

  (closed?
    [_]
    "If the link closed.")

  (close!
    [_]
    "Close the link."))
