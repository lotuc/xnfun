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

(def ^:dynamic *run-at*
  "Return a closable."
  (fn [time-in-ms fun & handlers]
    (chime/chime-at
     [(Instant/ofEpochMilli time-in-ms)]
     (fn [t] (fun (.toEpochMilli t)))
     handlers)))

(def ^:dynamic *periodic-run*
  "Periodically run. Return a closable."
  (fn [first-run-at-in-ms interval-ms fun & handlers]
    (-> (chime/periodic-seq (Instant/ofEpochMilli first-run-at-in-ms)
                            (Duration/ofMillis interval-ms))
        (chime/chime-at (fn [t] (fun (.toEpochMilli t))) handlers))))
