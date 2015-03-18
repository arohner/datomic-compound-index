(ns datomic-compound-index.core-test
  (:require [clojure.test :refer :all]
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
  (is (= "1|:foo" (index-key [1 :foo])))
  (is (= "1|:foo|" (index-key [1 :foo {:partial? true}]))))

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
    (is (= 2 (count (search (d/db conn) :user/foo-bar [1 {:partial? true}]))))
    (is (= 1 (count (search (d/db conn) :user/foo-bar [1 "bar"]))))
    (is (-> (search (d/db conn) :user/foo-bar [1 "bar"])
            (first)
            .v
            (= "1|bar")
            ))
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
    (is (= 2 (count (search-range (d/db conn) :user/foo-bar
                                  [2 {:partial? true}]
                                  [4 {:partial? true}]))))))
