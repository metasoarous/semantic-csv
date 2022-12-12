(defproject semantic-csv "0.2.1-alpha1"
  :description "A Clojure library with higher level CSV parsing functionality"
  :url "http://github.com/metasoarous/semantic-csv"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [clojure-csv/clojure-csv "2.0.1"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                                  [com.cemerick/piggieback "0.2.1"]]
                   :plugins      [[lein-marginalia "0.9.0"]]
                   :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}}})
