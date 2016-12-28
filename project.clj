(defproject semantic-csv "0.1.0"
  :description "A Clojure library with higher level CSV parsing functionality"
  :url "http://github.com/metasoarous/semantic-csv"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/clojurescript "1.7.228"]
                 [clojure-csv/clojure-csv "2.0.1"]]
  :plugins [[lein-cljsbuild "1.1.4"]]
  :profiles {:dev {:plugins [[lein-marginalia "0.9.0"]]
                   :source-paths ["test"]}
             :1.8 {:dependencies [[org.clojure/clojure       "1.8.0" :scope "provided"]
                                  [org.clojure/clojurescript "1.8.51" :scope "provided"]]}
             :1.9 {:dependencies [[org.clojure/clojure       "1.9.0-alpha12" :scope "provided"]
                                  [org.clojure/clojurescript "1.9.229" :scope "provided"]]
                   :global-vars  {*print-namespace-maps* false}}}

  ;; This was more or less ripped from datascript
  :cljsbuild {:builds [{:id "release"
                        :source-paths ["src"]
                        :assert false
                        :compiler {:output-to     "release-js/semantic-csv.bare.js"
                                   :optimizations :advanced
                                   :pretty-print  false
                                   :elide-asserts true
                                   :output-wrapper false
                                   :parallel-build true}}
                        ;:notify-command ["release-js/wrap_bare.sh"]}

                       {:id "advanced"
                        :source-paths ["src" "test"]
                        :compiler {:output-to     "target/semantic-csv.js"
                                   :optimizations :advanced
                                   :source-map    "target/semantic-csv.js.map"
                                   :pretty-print  true
                                   :recompile-dependents false
                                   :parallel-build true}}

                       {:id "none"
                        :source-paths ["src" "test"]
                        :compiler {;:main          semantic-csv.test
                                   :output-to     "target/semantic-csv.js"
                                   :output-dir    "target/none"
                                   :optimizations :none
                                   :source-map    true
                                   :recompile-dependents false
                                   :parallel-build true}}]})
