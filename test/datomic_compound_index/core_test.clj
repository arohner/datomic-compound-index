(ns datomic-compound-index.core-test
  (:require [clojure.data.fressian :as fress]
            [clojure.test :refer :all]
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

(deftest index-key-uses-to-datomic-for-dates
  (let [v (index-key [(Date.) 1234])]
    (is (string? v))
    (is (re-find #"^[0-9|]+$" v))))

(deftest index-key-works
  (is (index-key [1 :foo])))

(deftest search-finds-things-correctly
  (let [schema [(attribute :user/foo-bar :db.type/bytes
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
    (is (= 0 (count (search (d/db conn) :user/foo-bar [1 "bogus"]))))))

(deftest search-range-finds-things-correctly
  (let [schema [(attribute :user/foo-bar :db.type/bytes
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
    (let [result (search-range (d/db conn) :user/foo-bar [2] [4])]
      (is (= [[2 "bar"] [3 "bbq"] [4 "baz"]] (map (fn [^Datum d] (fress/read (.v d))) result))))

    (let [result (search-range (d/db conn) :user/foo-bar [2 "bar"] [4 "baz"])]
      (is (= [[2 "bar"] [3 "bbq"] [4 "baz"]] (map (fn [^Datum d] (fress/read (.v d))) result))))))


(deftest compound-compare-works
  (are [a b op v] (op (compound-compare a b) v)
       [0] [0] = 0
       [0] [1] < 0
       [-4] [-2] < 0
       [-4] [2] < 0
       [0 0] [1 1] < 0
       [-1 0] [0 0] < 0))

(defn normalize-compare
  "Returns -1, 0,1 to make testing easier"
  [c]
  (cond
    (> c 0) 1
    (< c 0) -1
    :else 0))

(defspec index-key-preserves-order
  (prop/for-all [[a b] (gen/tuple gen/int gen/int)]
    (= (normalize-compare (compare a b)) (normalize-compare (compound-compare (index-key a) (index-key b))))))

(defn search-vector [db eid]
  ((juxt :user/foo :user/bar) (d/pull db [:user/foo :user/bar] eid)))

;; search-spec TODO
;; - make query separate from values
(defspec search-spec
  100
  (let [conn (empty-database)
        schema [(attribute :user/foo :db.type/long)
                (attribute :user/bar :db.type/long)
                (attribute :user/foo-bar :db.type/bytes
                           :db/index true)]]
    @(d/transact conn schema)
    (prop/for-all [values (gen/vector (gen/tuple gen/pos-int gen/pos-int))]
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
                        (inspect values)
                        (inspect [foo bar])
                        (inspect datomic)
                        (inspect search-result)))
                    eql?)) values)))))

(defn search-test
  "Values is a seq of [foo bar] pairs. Query is a vector [foo bar] result is a set of vectors [foo bar]"
  [values query expected]
  (let [conn (empty-database)
        schema [(attribute :user/foo :db.type/long)
                (attribute :user/bar :db.type/long)
                (attribute :user/foo-bar :db.type/bytes
                           :db/index true)]]
    @(d/transact conn schema)
    @(d/transact conn (for [[foo bar] values]
                        {:db/id (d/tempid :db.part/user)
                         :user/foo foo
                         :user/bar bar
                         :user/foo-bar (index-key [foo bar])}))
    (let [datoms (search (d/db conn) :user/foo-bar query)
          result (set (map (fn [d]
                             ((juxt :user/foo :user/bar) (d/pull (d/db conn) [:user/foo :user/bar] (.e d)))) datoms))]
      (is (= expected result)))))

(deftest search-tests
  ;; (search-test [] [] #{})
  ;; (search-test [[0 0]] [0 0] #{[0 0]})
  ;; (search-test [[1 -1] [1 0]] [1 0] #{[1 0]})

  (search-test [[2 4] [-4 -2] [2 -4]] [-4 -2] #{[-4 -2]})
  )

(defspec search-range-spec
  100
  (let [conn (empty-database)
        schema [(attribute :user/foo :db.type/long)
                (attribute :user/bar :db.type/long)
                (attribute :user/foo-bar :db.type/bytes
                           :db/index true)]]
    @(d/transact conn schema)
    (prop/for-all [values (gen/vector (gen/tuple gen/pos-int gen/pos-int))
                   query (gen/such-that (fn [[[f1 b1] [f2 b2]]]
                                          (< f1 f2)) (gen/tuple (gen/tuple gen/pos-int gen/pos-int) (gen/tuple gen/pos-int gen/pos-int)))]
      @(d/transact conn (for [[foo bar] values]
                          {:db/id (d/tempid :db.part/user)
                           :user/foo foo
                           :user/bar bar
                           :user/foo-bar (index-key [foo bar])}))
      (let [db (d/db conn)]
        (let [[[foo1 bar1] [foo2 bar2]] query
              datomic (->> (d/q '[:find [?e ...] :in $ ?foo1 ?foo2 ?bar1 ?bar2 :where
                                  [?e :user/foo ?efoo]
                                  [?e :user/bar ?ebar]
                                  [(<= ?foo1 ?efoo)]
                                  [(<= ?efoo ?foo2)]
                                  [(<= ?bar1 ?ebar)]
                                  [(<= ?ebar ?bar2)]] db foo1 foo2 bar1 bar2)
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
              (inspect all-values)
              (inspect query)
              (inspect datomic-result)
              (inspect search-result)
              ;; (inspect datomic)
              ;; (inspect search-result)
              ))
          eql?)))))

(defn search-range-test
  "Values is a seq of [foo bar] pairs. Query is a vector [[foo bar] [foo bar]] result is a set of vectors [foo bar]"
  [values query expected]
  (let [conn (empty-database)
        schema [(attribute :user/foo :db.type/long)
                (attribute :user/bar :db.type/long)
                (attribute :user/foo-bar :db.type/bytes
                           :db/index true)]]
    @(d/transact conn schema)
    @(d/transact conn (for [[foo bar] values]
                        {:db/id (d/tempid :db.part/user)
                         :user/foo foo
                         :user/bar bar
                         :user/foo-bar (index-key [foo bar])}))
    (let [datoms (apply search-range (d/db conn) :user/foo-bar query)
          result (set (map (fn [d]
                             ((juxt :user/foo :user/bar) (d/pull (d/db conn) [:user/foo :user/bar] (.e d)))) datoms))]
      (is (= expected result)))))

(deftest search-range-1
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
