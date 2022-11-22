(defproject lotuc-remote "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main ^:skip-aot lotuc.remote.core

  :repositories [["clojars" {:url "https://mirrors.ustc.edu.cn/clojars/"}]]

  :repl {:middleware [cider.enrich-classpath/middleware]
         :enrich-classpath {:classifiers #{"sources" "javadoc"}}}
  :plugins [[cider/cider-nrepl "0.28.7"]
            [mx.cider/enrich-classpath "1.9.0"]
            [refactor-nrepl/refactor-nrepl "3.6.0"]
            [nrepl/nrepl "1.0.0"]]

  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.logging "1.2.4"]
                 [com.taoensso/nippy "3.2.0"]
                 [jarohen/chime "0.3.3"]
                 ;; Maven Dependencies
                 [org.eclipse.paho/org.eclipse.paho.client.mqttv3 "1.2.5"]]

  :source-paths ["src/main/clj"]
  :test-paths ["src/test/clj"]
  :resource-paths ["resources/main"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
