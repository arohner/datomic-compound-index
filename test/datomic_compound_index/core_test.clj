(ns datomic-compound-index.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer (defspec)]
            [datomic.api :as d]
            [datomic-compound-index.core :refer :all])
  (:import java.util.Date
           datomic.db.Datum))

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

(def gen-date (gen/fmap (fn [i] (Date. (long (* i 1003 1007 1001 1002)))) gen/int))

(def gen-supported-values (gen/one-of [gen/int ;; gen/boolean gen/string gen/keyword gen-date
                                       ]))

(def int-schema [(attribute :user/foo :db.type/long)
                 (attribute :user/bar :db.type/long)
                 (attribute :user/foo-bar :db.type/bytes
                            :db/index true)
                 (attribute :user/foo-bar-metadata :db.type/bytes)])
(defspec index-key-works
  100
  (prop/for-all [v gen-supported-values]
    (let [resp (index-key [v])]
      (and resp
           (:data resp)
           (seq (:metadata resp))))))

(defn <order-test
  "Returns true if x is < y, in datomic"
  [x y]
  (let [conn (empty-database)]
    @(d/transact conn int-schema)
    @(d/transact conn [(merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo 1}
                        (insert-index-key :user/foo-bar x))
                       (merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo 2}
                        (insert-index-key :user/foo-bar y))])
    (->> (seq (d/seek-datoms (d/db conn) :avet :user/foo-bar))
         (map (fn [datum]
                (d/pull (d/db conn) [:user/foo] (.e datum))))
         (mapv :user/foo)
         (apply <))))

(deftest ordering-tests
  (are [x y] (and (= true (<order-test x y))
                  (<= (key-compare (index-key x) (index-key y)) 0))
       [] [0]
       [0] [1]
       [-1] [1]
       [0 0] [1 0]
       [0 1] [1 0]
       [0 "foo"] [1 "bar"]
       ["aaaa" 1] ["z" 0]
       [1] [1 2]
       [1 2] [1 2 "foo"]
       [1] [11]
       [1 "z"] [11 "a"]))

(defn search-vector [db eid]
  ((juxt :user/foo :user/bar) (d/pull db [:user/foo :user/bar] eid)))

(deftest search-finds-things-correctly
  (let [schema [(attribute :user/foo :db.type/string
                           :db/index true)
                (attribute :user/foo-bar :db.type/bytes
                           :db/index true)
                (attribute :user/foo-bar-metadata :db.type/bytes)]
        conn (empty-database)]
    @(d/transact conn schema)
    @(d/transact conn [(merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo "1|foo"}
                        (insert-index-key :user/foo-bar [1 "foo"]))
                       (merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo "1|bar"}
                        (insert-index-key :user/foo-bar [1 "bar"]))
                       (merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo "11|bbq"}
                        (insert-index-key :user/foo-bar [11 "bbq"]))
                       (merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo "2|baz"}
                        (insert-index-key :user/foo-bar [2 "baz"]))])
    (is (= 2 (count (search (d/db conn) :user/foo-bar [1]))))
    (is (= 1 (count (search (d/db conn) :user/foo-bar [1 "bar"]))))
    (is (= 0 (count (search (d/db conn) :user/foo-bar [1 "bogus"]))))))

(deftest search-range-finds-things-correctly
  (let [schema [(attribute :user/foo :db.type/long
                           :db/index true)
                (attribute :user/bar :db.type/string
                           :db/index true)
                (attribute :user/foo-bar :db.type/bytes
                           :db/index true)
                (attribute :user/foo-bar-metadata :db.type/bytes)]
        conn (empty-database)]
    @(d/transact conn schema)
    @(d/transact conn [(merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo 1
                         :user/bar "foo"}
                        (insert-index-key :user/foo-bar [1 "foo"]))
                       (merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo 2
                         :user/bar "bar"}
                        (insert-index-key :user/foo-bar [2 "bar"]))
                       (merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo 3
                         :user/bar "bbq"}
                        (insert-index-key :user/foo-bar [3 "bbq"]))
                       (merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo 4
                         :user/bar "baz"}
                        (insert-index-key :user/foo-bar [4 "baz"]))
                       (merge
                        {:db/id (d/tempid :db.part/user)
                         :user/foo 5
                         :user/bar "quux"}
                        (insert-index-key :user/foo-bar [5 "quux"]))])
    (let [db (d/db conn)
          ->vector-result (fn [datum]
                            ((juxt :user/foo :user/bar) (d/pull db [:user/foo :user/bar] (.e datum))))]
      (let [result (search-range (d/db conn) :user/foo-bar [2] [4])]
        (is (= [[2 "bar"] [3 "bbq"] [4 "baz"]] (map ->vector-result result))))

      (let [result (search-range (d/db conn) :user/foo-bar [2 "bar"] [4 "baz"])]
        (is (= [[2 "bar"] [3 "bbq"] [4 "baz"]] (map ->vector-result result)))))))

;; search-spec TODO
;; - make query separate from values
(defspec search-spec
  100
  (prop/for-all [values (gen/vector (gen/tuple gen/pos-int gen/pos-int))]
    (let [conn (empty-database)
          schema [(attribute :user/foo :db.type/long)
                  (attribute :user/bar :db.type/long)
                  (attribute :user/foo-bar :db.type/bytes
                             :db/index true)
                  (attribute :user/foo-bar-metadata :db.type/bytes)]]
      @(d/transact conn schema)
      @(d/transact conn (for [[foo bar] values]
                          (merge
                           {:db/id (d/tempid :db.part/user)
                            :user/foo foo
                            :user/bar bar}
                           (insert-index-key :user/foo-bar [foo bar]))))
      (let [db (d/db conn)]
        (every? (fn [[foo bar]]
                  (let [datomic (->> (d/q '[:find [?e ...] :in $ ?foo ?bar :where
                                            [?e :user/foo ?foo]
                                            [?e :user/bar ?bar]] db foo bar)
                                     (sort))
                        search-result (->> (search db :user/foo-bar [foo bar])
                                           (mapv (fn [^Datum datom]
                                                   (.e datom)))
                                           (sort))
                        eql? (= datomic search-result)]
                    (when-not eql?
                      (let [all-values (d/q '[:find [?e ...] :in $ :where [?e :user/foo]] (d/db conn))
                            datomic (->>
                                     (d/q '[:find [?e ...] :in $ ?foo ?bar :where
                                            [?e :user/foo ?foo]
                                            [?e :user/bar ?bar]] db foo bar)
                                     (mapv (fn [eid]
                                             (search-vector db eid))))
                            search-result (->>
                                           (search db :user/foo-bar [foo bar])
                                           (mapv (fn [datom]
                                                   (search-vector db (.e datom)))))]
                        (println "values:" values)
                        (println "foo:bar" [foo bar])
                        (println "datomic:" datomic)
                        (println "search-result:" search-result)))
                    eql?)) values)))))

(defn search-test
  "Values is a seq of [foo bar] pairs. Query is a vector [foo bar] result is a set of vectors [foo bar]"
  [values query expected]
  (let [conn (empty-database)
        schema [(attribute :user/foo :db.type/long)
                (attribute :user/bar :db.type/long)
                (attribute :user/foo-bar :db.type/bytes
                           :db/index true)
                (attribute :user/foo-bar-metadata :db.type/bytes)]]
    @(d/transact conn schema)
    @(d/transact conn (for [[foo bar] values]
                        (merge
                         {:db/id (d/tempid :db.part/user)
                          :user/foo foo
                          :user/bar bar}
                         (insert-index-key :user/foo-bar [foo bar]))))
    (let [datoms (search (d/db conn) :user/foo-bar query)
          result (set (map (fn [d]
                             ((juxt :user/foo :user/bar) (d/pull (d/db conn) [:user/foo :user/bar] (.e d)))) datoms))]
      (is (= expected result)))))

(deftest search-tests
  (search-test [] [] #{})
  (search-test [[0 0]] [0 0] #{[0 0]})
  (search-test [[1 -1] [1 0]] [1 0] #{[1 0]})

  (search-test [[2 4] [-4 -2] [2 -4]] [-4 -2] #{[-4 -2]}))

(defspec search-range-spec
  100
  (prop/for-all [values (gen/vector (gen/tuple gen/pos-int gen/pos-int))
                 query (gen/fmap (fn [[[foo1 bar1] [foo2 bar2]]]
                                   (if (> foo1 foo2)
                                     [[foo2 bar2] [foo1 bar1]]
                                     [[foo1 bar1] [foo2 bar2]])) (gen/tuple (gen/tuple gen/pos-int gen/pos-int) (gen/tuple gen/pos-int gen/pos-int)))]
    (let [conn (empty-database)
          schema [(attribute :user/foo :db.type/long)
                  (attribute :user/bar :db.type/long)
                  (attribute :user/foo-bar :db.type/bytes
                             :db/index true)
                  (attribute :user/foo-bar-metadata :db.type/bytes)]]
      @(d/transact conn schema)
      @(d/transact conn (for [[foo bar] values]
                          (merge
                           {:db/id (d/tempid :db.part/user)
                            :user/foo foo
                            :user/bar bar}
                           (insert-index-key :user/foo-bar [foo bar]))))
      (let [db (d/db conn)]
        (let [[[foo1 bar1] [foo2 bar2]] query
              datomic (->> (d/q '[:find [?e ...] :in $ ?foo1 ?bar1 ?foo2 ?bar2  :where
                                  [?e :user/foo ?efoo]
                                  [?e :user/bar ?ebar]
                                  (or
                                   (and [(< ?foo1 ?efoo)]
                                        [(identity ?bar1)]
                                        [(identity ?ebar)])
                                   (and [(= ?foo1 ?efoo)]
                                        [(<= ?bar1 ?ebar)]))
                                  (or
                                   (and [(< ?efoo ?foo2)]
                                        [(identity ?bar2)]
                                        [(identity ?ebar)])
                                   (and [(= ?efoo ?foo2)]
                                        [(<= ?ebar ?bar2)]))]
                                  db foo1 bar1 foo2 bar2)
                           (sort))
              search-result (->> (search-range db :user/foo-bar [foo1 bar1] [foo2 bar2])
                                 (mapv (fn [^Datum datom]
                                         (.e datom)))
                                 (sort))
              eql? (= datomic search-result)]
          (when-not eql?
            (let [all-values (d/q '[:find [?e ...] :in $ :where [?e :user/foo]] (d/db conn))
                  all-values (mapv #(search-vector (d/db conn) %) all-values)
                  datomic-result (vec (map #(search-vector (d/db conn) %) datomic))
                  search-result (vec (map #(search-vector (d/db conn) %) search-result))]
              (println "all-values:" all-values)
              (println "query:" query)
              (println "datomic-result:" datomic-result)
              (println "search-result" search-result)))
          eql?)))))

(defn search-range-test
  "Values is a seq of [foo bar] pairs. Query is a vector [[foo bar] [foo bar]] result is a set of vectors [foo bar]"
  [values query expected]
  (let [conn (empty-database)
        schema [(attribute :user/foo :db.type/long)
                (attribute :user/bar :db.type/long)
                (attribute :user/foo-bar :db.type/bytes
                           :db/index true)
                (attribute :user/foo-bar-metadata :db.type/bytes)]]
    @(d/transact conn schema)
    @(d/transact conn (for [[foo bar] values]
                        (merge
                         {:db/id (d/tempid :db.part/user)
                          :user/foo foo
                          :user/bar bar}
                         (insert-index-key :user/foo-bar [foo bar]))))
    (let [datoms (apply search-range (d/db conn) :user/foo-bar query)
          result (set (map (fn [d]
                             ((juxt :user/foo :user/bar) (d/pull (d/db conn) [:user/foo :user/bar] (.e d)))) datoms))]
      (is (= expected result)))))

(deftest subkey-works
  (are [x y result] (= result (subkey= (index-key x) (index-key y)))
       [0] [0 0] true
       [0 1] [0 2] false))

(deftest search-range-tests
  (search-range-test [[0 1]] [[0 0] [0 2]] #{[0 1]})
  (search-range-test [[0 0]] [[0 1] [0 2]] #{})
  (search-range-test [[-1 1]] [[2 0] [3 0]] #{})
  (search-range-test [[0 0]] [[0 0] [0 0]] #{[0 0]})
  (search-range-test [[0 0] [1 1] [2 2]] [[3 3] [4 4]] #{})
  (search-range-test [[0 0] [1 1] [2 2]] [[-2 -2] [-3 -3]] #{})
  (search-range-test [[0 0] [1 1] [2 2]] [[-1 -2] [-3 -3]] #{})
  (search-range-test [[-2 0] [1 2]] [[-4 0] [4 1]] #{[-2 0] [1 2]}))

;; (defspec search-range-partial)
;; (defspec search-partial)
