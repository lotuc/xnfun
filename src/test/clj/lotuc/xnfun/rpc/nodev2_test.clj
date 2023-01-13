(ns lotuc.xnfun.rpc.nodev2-test
  (:require
   [lotuc.xnfun.rpc.nodev2 :as n]))

(def n0 (n/make-node))

(n/start-node-link! n0)

@(-> n0 :node-state :local :link)
@@(-> n0 :node-state :local :link)

(.closed? @@(-> n0 :node-state :local :link))
(.close! @@(-> n0 :node-state :local :link))

(n/add-function! n0 "add" (fn [[a b]] (+ a b)))

(n/call-function! n0 "add" [1 2])

(def r0 (n/submit-call! n0 "add" [1 2]))

(def p0 (n/submit-promise! n0 {:req-meta {}}))
(n/fullfill-promise! n0 (-> p0 :request :req-meta :req-id) 1)

(n/start-heartbeat! n0)

(n/node-info n0)
