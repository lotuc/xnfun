# Introduction to xnfun

## Node Options

- `:node-id`: node id
- `:hb-interval-ms`: heartbeat interval
- `:hb-lost-ratio`: heartbeat will be considered lost in `(* hb-interval-ms hb-lost-raio)`
- `:xnfun/link`: Message link
  - Defaults to be mqtt:
    ```edn
    {:xnfun/module 'xnfun.mqtt
     :mqtt-config {...}}
    ```
