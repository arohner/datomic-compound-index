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

(defn ordering-test
  [operator x y]
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
         (map (fn [^Datum datum]
                (d/pull (d/db conn) [:user/foo] (.e datum))))
         (mapv :user/foo)
         (apply operator))))

(deftest key-compare-tests
  (are [x y expected] (= (key-compare (index-key x) (index-key y)) expected)
       [0 0] [0 1] -1
       [0 1] [0 1] 0
       [0 2] [0 1] 1
       [0] [0 0] :subkey-a
       [0] [0 1] :subkey-a

       [128] [127] 1
       [127] [128] -1))

(deftest ordering-tests
  (are [x y expected] (and (= true (ordering-test < x y))
                           (= (key-compare (index-key x) (index-key y))) expected)
       [] [0] :subkey-a
       [0] [1] -1
       [-1] [1] -1
       [0 0] [1 0] -1
       [0 1] [1 0] -1
       [0 1] [0 2] -1
       [0 "foo"] [1 "bar"] -1
       ["aaaa" 1] ["z" 0] -1
       [1] [1 2] :subkey-a
       [1 2] [1 2 "foo"] :subkey-a
       [1] [11] -1
       [1 "z"] [11 "a"] -1))

(deftest ordering-tests->
  (are [x y expected] (and (= true (ordering-test > x y))
                           (= (key-compare (index-key x) (index-key y))) expected)
       [0 2] [0 1] 1))

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
          ->vector-result (fn [^Datum datum]
                            ((juxt :user/foo :user/bar) (d/pull db [:user/foo :user/bar] (.e datum))))]
      (let [result (search-range (d/db conn) :user/foo-bar [2] [4])]
        (is (= [[2 "bar"] [3 "bbq"] [4 "baz"]] (map ->vector-result result))))

      (let [result (search-range (d/db conn) :user/foo-bar [2 "bar"] [4 "baz"])]
        (is (= [[2 "bar"] [3 "bbq"] [4 "baz"]] (map ->vector-result result)))))))

(defn search-test
  "Values is a seq of [foo bar] pairs. Query is a vector [foo
  bar]. Asserts the search result and datomic query results are the
  same. When optionally passed expected, a set of vectors, asserts
  that both results also equal expected."

  [values query & [expected]]
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
    (let [db (d/db conn)
          [foo bar] query
          datomic-result (->>
                          (cond
                            (and foo bar) (d/q '[:find [?e ...] :in $ ?foo ?bar :where
                                                 [?e :user/foo ?foo]
                                                 [?e :user/bar ?bar]] db foo bar)
                            (and foo) (d/q '[:find [?e ...] :in $ ?foo :where
                                             [?e :user/foo ?foo]] db foo)
                            :else (d/q '[:find [?e ...] :where [?e :user/foo-bar]] db))
                          (mapv (fn [eid]
                                  (search-vector db eid)))
                          (set))
          datoms (search (d/db conn) :user/foo-bar query)
          search-result (set (map (fn [^Datum d]
                                    (search-vector db (.e d))) datoms))
          expected? (if expected
                      (= expected search-result)
                      true)
          eql? (= search-result datomic-result)]
      (is expected?)
      (is eql?)
      (when (not (and eql? expected?))
        (let [all-values (d/q '[:find [?e ...] :in $ :where [?e :user/foo]] (d/db conn))]
          (println "values:" values)
          (println "foo:bar" [foo bar])
          (println "datomic:" datomic-result)
          (println "search-result:" search-result)))
      (and eql? expected?))))

(defspec search-spec
  100
  (prop/for-all [values (gen/vector (gen/tuple gen/int gen/int))
                 query (gen/tuple gen/int gen/int)]
    (search-test values query)))

(defspec search-partial-spec
  100
  (prop/for-all [values (gen/vector (gen/tuple gen/int gen/int))
                 query (gen/tuple gen/int)]
    (search-test values query)))

(deftest search-tests
  (search-test [] [] #{})
  (search-test [[0 0]] [0 0] #{[0 0]})
  (search-test [[1 -1] [1 0]] [1 0] #{[1 0]})

  (search-test [[2 4] [-4 -2] [2 -4]] [-4 -2] #{[-4 -2]}))

(defn search-range-test
  "Values is a seq of [foo bar] pairs. Query is a vector [[foo bar] [foo bar]] result is a set of vectors [foo bar]"
  [values query & [expected]]
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
    (let [db (d/db conn)
          [[foo1 bar1] [foo2 bar2]] query
          datomic-result (->>
                          (cond
                            (and foo1 bar1 foo2 bar2)  (d/q '[:find [?e ...] :in $ ?foo1 ?bar1 ?foo2 ?bar2  :where
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
                            (and foo1 foo2)  (d/q '[:find [?e ...] :in $ ?foo1 ?foo2 :where
                                                    [?e :user/foo ?efoo]
                                                    [(<= ?foo1 ?efoo)]
                                                    [(<= ?efoo ?foo2)]]
                                                  db foo1 foo2)
                            :else  (d/q '[:find [?e ...] :where [?e :user/foo-bar ?efoo]] db))
                              (map #(search-vector db %))
                              set)
          search-result (->> (apply search-range db :user/foo-bar query)
                             (map (fn [^Datum d] (search-vector db (.e d))))
                             (set))
          expected? (if expected
                      (= expected search-result)
                      true)
          eql? (= search-result datomic-result)]
      (when-not eql?
        (let [all-values (d/q '[:find [?e ...] :in $ :where [?e :user/foo]] (d/db conn))
              all-values (mapv #(search-vector (d/db conn) %) all-values)]
          (println "all-values:" all-values)
          (println "query:" query)
          (println "datomic-result:" datomic-result)
          (println "search-result" search-result)))
      (is expected?)
      (is eql?)
      (and eql? expected?))))

(defspec search-range-spec
  100
  (prop/for-all [values (gen/vector (gen/tuple gen/int gen/int))
                 query (gen/fmap (fn [[[foo1 bar1] [foo2 bar2]]]
                                   (if (> foo1 foo2)
                                     [[foo2 bar2] [foo1 bar1]]
                                     [[foo1 bar1] [foo2 bar2]])) (gen/tuple (gen/tuple gen/int gen/int) (gen/tuple gen/int gen/int)))]
    (search-range-test values query)))

(defspec search-range-partial-spec
  100
  (prop/for-all [values (gen/vector (gen/tuple gen/int gen/int))
                 query (gen/fmap (fn [[[foo1] [foo2]]]
                                   (if (> foo1 foo2)
                                     [[foo2] [foo1]]
                                     [[foo1] [foo2]])) (gen/tuple (gen/tuple gen/int) (gen/tuple gen/int)))]
    (search-range-test values query)))

(deftest search-range-tests
  (search-range-test [[0 1]] [[0 0] [0 2]] #{[0 1]})
  (search-range-test [[0 0]] [[0 1] [0 2]] #{})
  (search-range-test [[-1 1]] [[2 0] [3 0]] #{})
  (search-range-test [[0 0]] [[0 0] [0 0]] #{[0 0]})
  (search-range-test [[0 0] [1 1] [2 2]] [[3 3] [4 4]] #{})
  (search-range-test [[0 0] [1 1] [2 2]] [[-2 -2] [-3 -3]] #{})
  (search-range-test [[0 0] [1 1] [2 2]] [[-1 -2] [-3 -3]] #{})
  (search-range-test [[-2 0] [1 2]] [[-4 0] [4 1]] #{[-2 0] [1 2]}))

(defn sorted-bytes [ints]
  (let [conn (empty-database)]
    @(d/transact conn int-schema)
    @(d/transact conn (vec (map (fn [i]
                                  (let [bytes (to-bytes i)]
                                    (->binary-str {:data bytes})
                                    {:db/id (d/tempid :db.part/user)
                                     :user/foo i
                                     :user/foo-bar bytes})) ints)))
    (let [db (d/db conn)
          result (->>
                  (d/datoms (d/db conn) :avet :user/foo-bar)
                  (seq)
                  (vec)
                  (map (fn [d]
                         (:user/foo (d/pull db [:user/foo] (.e d)))))
                  vec)
          expected (vec (sort ints))]
      (= expected result))))

(defspec sorted-bytes-works
  1000
  (prop/for-all [ints (gen/vector gen/int)]
    (sorted-bytes ints)))
