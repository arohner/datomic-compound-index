(defproject datomic-compound-index "0.2.0"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :profiles {:dev {:dependencies [[com.datomic/datomic-free "0.9.5130"]
                                  [org.clojure/data.fressian "0.2.0"]
                                  [org.clojure/test.check "0.5.9"]]}
             :test {:jvm-opts ["-Xmx2048m"]}}
  :global-vars  {*warn-on-reflection* true})
