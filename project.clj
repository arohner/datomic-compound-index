(defproject datomic-compound-index "0.3.2-SNAPSHOT"
  :description "A small library for adding compound indices to Datomic"
  :url "https://github.com/arohner/datomic-compound-index"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/data.fressian "0.2.0"]]
  :profiles {:dev {:dependencies [[com.datomic/datomic-free "0.9.5130"]
                                  [org.clojure/test.check "0.5.9"]]}
             :test {:jvm-opts ["-Xmx2048m"]}}
  :global-vars  {*warn-on-reflection* true}
  :deploy-repositories [["releases" {:url "https://clojars.org/repo"
                                     :creds :gpg}]])
