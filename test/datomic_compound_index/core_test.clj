(ns datomic-compound-index.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer (defspec)]
            [datomic.api :as d]
            [datomic-compound-index.core :refer :all])
  (:import java.util.Date))

(defn attribute
  "Install a new schema attribute"
  [ident type & {:as opts}]
  (merge {:db/id (d/tempid :db.part/db)
          :db/ident ident
          :db/valueType type
          :db.install/_attribute :db.part/db}
         {:db/cardinality :db.cardinality/one}
         opts))

(defn empty-database []
  (let [db-url (str "datomic:mem://" (gensym "db"))]
    (d/create-database db-url)
    (d/connect db-url)))

(deftest index-key-uses-to-datomic-for-dates
  (let [v (index-key [(Date.) 1234])]
    (is (string? v))
    (is (re-find #"^[0-9|]+$" v))))

(deftest index-key-works
  (is (= "1|:foo|" (index-key [1 :foo])))
  (is (= "1|:foo|" (index-key [1 :foo]))))

(deftest search-finds-things-correctly
  (let [schema [(attribute :user/foo-bar :db.type/string
                           :db/index true)]
        conn (empty-database)]
    @(d/transact conn schema)
    @(d/transact conn [{:db/id (d/tempid :db.part/user)
                        :user/foo-bar (index-key [1 "foo"])}
                       {:db/id (d/tempid :db.part/user)
                        :user/foo-bar (index-key [1 "bar"])}
                       {:db/id (d/tempid :db.part/user)
                        :user/foo-bar (index-key [11 "bbq"])}
                       {:db/id (d/tempid :db.part/user)
                        :user/foo-bar (index-key [2 "baz"])}])
    (is (= 2 (count (search (d/db conn) :user/foo-bar [1]))))
    (is (= 1 (count (search (d/db conn) :user/foo-bar [1 "bar"]))))
    (is (-> (search (d/db conn) :user/foo-bar [1 "bar"])
            (first)
            .v
            (= "1|bar|")))
    (is (= 0 (count (search (d/db conn) :user/foo-bar [1 "bogus"]))))))

(deftest search-range-finds-things-correctly
  (let [schema [(attribute :user/foo-bar :db.type/string
                           :db/index true)]
        conn (empty-database)]
    @(d/transact conn schema)
    @(d/transact conn [{:db/id (d/tempid :db.part/user)
                        :user/foo-bar (index-key [1 "foo"])}
                       {:db/id (d/tempid :db.part/user)
                        :user/foo-bar (index-key [2 "bar"])}
                       {:db/id (d/tempid :db.part/user)
                        :user/foo-bar (index-key [3 "bbq"])}
                       {:db/id (d/tempid :db.part/user)
                        :user/foo-bar (index-key [4 "baz"])}
                       {:db/id (d/tempid :db.part/user)
                        :user/foo-bar (index-key [5 "quux"])}])
    (is (= 3 (count (search-range (d/db conn) :user/foo-bar [2] [4]))))))

(defspec search-spec
  100
  (let [conn (empty-database)
        schema [(attribute :user/foo :db.type/long)
                (attribute :user/bar :db.type/long)
                (attribute :user/foo-bar :db.type/string
                           :db/index true)]]
    @(d/transact conn schema)
    (prop/for-all [values (gen/vector (gen/tuple gen/int gen/int))]
      @(d/transact conn (for [[foo bar] values]
                          {:db/id (d/tempid :db.part/user)
                           :user/foo foo
                           :user/bar bar
                           :user/foo-bar (index-key [foo bar])}))
      (let [db (d/db conn)]
        (every? (fn [[foo bar]]
                  (let [datomic (->> (d/q '[:find [?e ...] :in $ ?foo ?bar :where
                                            [?e :user/foo ?foo]
                                            [?e :user/bar ?bar]] db foo bar)
                                     (sort))
                        search-result (->> (search db :user/foo-bar [foo bar])
                                           (mapv (fn [datom]
                                                   (.e datom)))
                                           (sort))
                        eql? (= datomic search-result)]
                    ;; (when-not eql?
                    ;;   (inspect [values])
                    ;;   (inspect [foo bar (index-key [foo bar])])
                    ;;   (inspect (d/q '[:find [(pull ?e [*]) ...] :in $ ?foo ?bar :where
                    ;;                   [?e :user/foo ?foo]
                    ;;                   [?e :user/bar ?bar]] db foo bar))
                    ;;   (inspect (->>
                    ;;             (search db :user/foo-bar [foo bar])
                    ;;             (mapv (fn [datom]
                    ;;                     (d/pull db '[*] (.e datom))))))
                    ;;   )
                    eql?)) values)))))
