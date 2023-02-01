# Thoughts about xnfun

TODO:

- Clients
  - Java client
  - ClojureScript (and JavaScript)
  - HTTP client
- Error definitions
- Authentication (maybe something like the [gRPC authentication](https://grpc.io/docs/guides/auth/))
- Node scheduler policy (now callee node is selected randomly, maybe introduce
  something like the [Kubernetes Scheduler](https://kubernetes.io/docs/concepts/scheduling-eviction/kube-scheduler/))
- More transport support (now, we only got MQTT, maybe implement a [dapr pubsub](https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-overview/)
  transport first)
- spec/schema
- Proxying gRPC APIs
- State management
- Monitoring
- What's the rationale of this? There are so many RPC framework out there. You can always wrap them with some proxy.
