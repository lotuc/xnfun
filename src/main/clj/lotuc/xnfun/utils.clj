(ns lotuc.xnfun.utils)

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
