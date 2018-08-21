(defproject loader "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :plugins [[cider/cider-nrepl "0.17.0"]]

  :dependencies [[org.clojure/clojure "1.9.0"]

                 [org.clojure/data.csv "0.1.4"]
                 [org.postgresql/postgresql "42.1.3"]
                 [org.clojure/java.jdbc "0.6.1"]]

  :main ^:skip-aot loader.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
