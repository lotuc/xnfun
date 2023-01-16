(ns lotuc.xnfun.utils
  (:require [chime.core :as chime])
  (:import (java.time Instant Duration)))

(defn max-arity
  "Returns the maximum arity of:
    - anonymous functions like `#()` and `(fn [])`.
    - defined functions like `map` or `+`.
    - macros, by passing a var like `#'->`.

  Returns `:variadic` if the function/macro is variadic.

  https://stackoverflow.com/questions/1696693/clojure-how-to-find-out-the-arity-of-function-at-runtime
  "
  [f]
  (let [func (if (var? f) @f f)
        methods (->> func class .getDeclaredMethods
                     (map #(vector (.getName %)
                                   (count (.getParameterTypes %)))))
        var-args? (some #(-> % first #{"getRequiredArity"})
                        methods)]
    (if var-args?
      :variadic
      (let [max-arity (->> methods
                           (filter (comp #{"invoke"} first))
                           (sort-by second) last second)]
        (if (and (var? f) (-> f meta :macro))
          (- max-arity 2) ;; substract implicit &form and &env arguments
          max-arity)))))

;; The current time in ms
(def ^:dynamic *now-ms* #(System/currentTimeMillis))

(defn create-cancellable [{:as m :keys [cancel]}]
  (let [cancelled-at (atom nil)]
    (-> m
        (assoc :cancel #(do (cancel) (reset! cancelled-at (*now-ms*))))
        (assoc :cancelled-at cancelled-at))))

(def ^:dynamic *run-at*
  "Return a closable."
  (fn [time-in-ms fun & handlers]
    (let [ran-at (atom nil)
          closable (chime/chime-at
                    [(Instant/ofEpochMilli time-in-ms)]
                    (fn [t]
                      (let [t (.toEpochMilli t)]
                        (reset! ran-at t)
                        (fun t)))
                    handlers)]
      (create-cancellable
       {:cancel #(.close closable)
        :run-at time-in-ms
        :ran-at ran-at}))))

(defn- create-delayed-update-fn
  [{:as options :keys [ignores-nil?]} f & args]
  (fn [prev]
    (if (and (nil? prev) ignores-nil?)
      prev
      (do
        (when (and prev
                   (instance? clojure.lang.IPending prev)
                   (not (realized? prev)))
          (throw (ex-info "another computation running" {})))
        (delay (apply f prev args))))))

(defn swap-vals!-swap-in-delayed!
  "Ensure the swap function is called only once by swapping in the delayed
  computation. And reset the old value back if the computation fails.

  We apply the function located by `ks`.

  And we ensure only swap in a new delayed computation only there is no
  previous pending computation in place."
  [atom {:as options :keys [ignores-nil? ks]} f & args]
  (let [update-fn (apply create-delayed-update-fn options f args)

        [old new]
        (if (empty? ks)
          (swap-vals! atom update-fn)
          (swap-vals! atom #(update-in % ks update-fn)))]
    (try
      (when-let [p (if (empty? ks) new (get-in new ks))]
        @p)

      [old new]
      (catch Exception e
        (swap! atom (fn [v] (if (= v new) old v)))
        (throw e)))))

(defn swap!-swap-in-delayed!
  [atom {:as options :keys [ignores-nil? ks]} f & args]
  (second (apply swap-vals!-swap-in-delayed! atom options f args)))

(def ^:dynamic *periodic-run*
  "Periodically run. Return a closable."
  (fn [first-run-at-in-ms interval-ms fun & handlers]
    (let [ran-at (atom nil)
          closable (-> (chime/periodic-seq
                        (Instant/ofEpochMilli first-run-at-in-ms)
                        (Duration/ofMillis interval-ms))
                       (chime/chime-at
                        (fn [t]
                          (let [t (.toEpochMilli t)]
                            (reset! ran-at t)
                            (fun t)))
                        handlers))]
      (create-cancellable
       {:cancel #(.close closable)
        :interval interval-ms
        :ran-at ran-at}))))
