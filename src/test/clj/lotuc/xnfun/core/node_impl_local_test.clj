(ns lotuc.xnfun.core.node-impl-local-test
  (:require
   [clojure.core.async :refer [>!!]]
   [clojure.test :as t]
   [lotuc.xnfun.core.node-impl :as impl]
   [lotuc.xnfun.core.sample-functions :as f]
   [lotuc.xnfun.protocols :as xnp]))

(t/deftest xnnode-local-unary-call
  (let [n (impl/make-node)]
    (xnp/add-function! n :add (fn [[x y]] (+ x y)))
    (t/is (= 3 (xnp/call-function! n :add [2 1])))

    (let [fun-name :add params [1 2]

          {:as r :keys [req in-c out-c fut]}
          (xnp/submit-call! n fun-name params)]
      ;; result data shape
      (t/is (every? some? [req in-c out-c fut]))
      (t/is (and (= fun-name (:fun-name req))
                 (= params (:params req))))

      ;; response
      (t/is (= 3 @fut)))))

(t/deftest xnnode-local-bidirectional-call
  (let [n (impl/make-node)]
    (xnp/add-function! n :echo-server f/echo-server)

    (let [{:as r :keys [fut]}
          (xnp/submit-call!
           n :echo-server nil
           ;; heartbeat will be lost in 40ms if no message
           {:req-meta {:hb-interval-ms 20
                       :hb-lost-ratio 2
                       :timeout-ms 200}})]
      (Thread/sleep 100)
      (t/is (not (future-done? fut)))
      (Thread/sleep 70)
      (t/is (not (future-done? fut)))
      (Thread/sleep 50)                 ; (+ 100 70 50) > 200
      (t/is (future-done? fut)))

    (let [{:as r :keys [in-c fut]}
          (xnp/submit-call!
           n :echo-server nil
           ;; heartbeat will be lost in 40ms if no message
           {:req-meta {:hb-interval-ms 20
                       :hb-lost-ratio 2}})]

      ;; echo
      (let [msg "hello world"]
        (t/is (= msg (f/echo r msg))))

      ;; heartbeating, so we are ok
      (Thread/sleep 60)                 ; 60 > 40
      (t/is (not (future-done? fut)))

      ;; !!! stop heartbeat
      (>!! in-c {:typ :xnfun/to-callee :data :stop-heartbeat})
      (Thread/sleep 30)                 ; sleep 30
      (t/is (not (future-done? fut)))   ; ok

      (let [msg "hello world"]
        (t/is (= msg (f/echo r msg))))    ; some message
      (Thread/sleep 30)                 ; sleep 30
                                        ; (+ 30 30) > 40
      (t/is (not (future-done? fut)))   ; ok

      (Thread/sleep 60)                 ; sleep 60 > 40
      (t/is (future-done? fut))         ; not ok

      ;; after cancel, the result of @fut is undefined
      ;; the function may gracefully shutdown and return something, may not
      (>!! in-c {:typ :xnfun/cancel :data :anything}))))

(let [n (impl/make-node)]
  (t/deftest submit-promise--submit-and-get
    (let [req-id "id-42"
          {:as r :keys [req hb res-promise]}
          (xnp/submit-promise! n {:req-meta {:req-id req-id}})]

      (t/is (every? some? [req hb res-promise]))
      ;; given id
      (t/is (= req-id (some-> req :req-meta :req-id)))

      ;; get-promise
      (t/is (= r (xnp/get-promise n req-id)))))

  (t/deftest submit-promise--fullfill
    (let [{:as r :keys [req hb res-promise]}
          (xnp/submit-promise! n {:req-meta {}})]
      (t/is (every? some? [req hb res-promise]))
      ;; generated id
      (let [req-id (some-> req :req-meta :req-id)]
        (t/is (some? req-id))
        (xnp/fullfill-promise! n req-id 42)
        (t/is (= 42 (deref res-promise 1000 :timeout))))))

  (t/deftest submit-promise--timeout
    ;; timeout
    (let [{:as r :keys [res-promise]}
          (xnp/submit-promise! n {:req-meta {:timeout-ms 50}})]
      (Thread/sleep 60)
      (let [{:keys [status data]} (deref res-promise 1000 :timeout)]
        (t/is (= status :xnfun/err))
        (t/is (= {:typ :timeout} (select-keys data [:typ]))
              (str data)))))

  (t/deftest submit-promise-hb-lost
    ;; heartbeat lost
    (let [{:as r :keys [hb res-promise]}
          (xnp/submit-promise! n {:req-meta {:hb-interval-ms 20
                                             :hb-lost-ratio 2
                                             :timeout-ms 1000}})]
      ;; ok
      (t/is (= :timeout (deref res-promise 30 :timeout)))
      (hb)
      ;; ok
      (t/is (= :timeout (deref res-promise 30 :timeout)))

      ;; hb lost
      (Thread/sleep 60)
      (let [{:keys [status data]} (deref res-promise 1000 :timeout)]
        (t/is (= status :xnfun/err))
        (t/is (= {:typ :timeout} (select-keys data [:typ]))
              (str data))))))
