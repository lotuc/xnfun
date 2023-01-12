(ns lotuc.xnfun.rpc.link)

(defprotocol XNFunLink
  (send-msg
    [node {:as msg :keys [typ data]}]
    "Send given message to link base node's info.

    Additional info is encoded with metadata of `msg`.")

  (sub-msg
    [node {:as handler :keys [typ handle-fn]}]
    "Subscribe for message and handle it with `msg-handler`.

    - `typ`: the message type the `handle-fn` supports
    - `handle-fn`: (fn [{:as `msg` :keys [data}] ...)

    Additional info is encoded within metadata of `handler`:
    - TODO: document implementation agnostic metadata.
    - Implementation specific.

    Additional info is encoded within metadata of `msg`.

    Return: unsubscribe the subscription."))
