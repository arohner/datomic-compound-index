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

(defn empty-database []
  (let [db-url (str "datomic:mem://" (gensym "db"))]
    (d/create-database db-url)
    (d/connect db-url)))

(def event-schema (concat
                   [(attribute :user/name :db.type/string)
                    (attribute :event/user :db.type/ref)
                    (attribute :event/at :db.type/instant)
                    (attribute :event/type :db.type/keyword)]
                   (compound-index :event/user-at-type [:event/user :event/at :event/type])))

(deftest create-get-index-works
  (let [conn (empty-database)
        schema event-schema]
    @(d/transact conn (install-dci))
    @(d/transact conn event-schema)
    (is (= [:event/user :event/at :event/type] (get-index-attrs (d/db conn) :event/user-at-type)))))

(deftest index-value-works
  (let [conn (empty-database)
        schema event-schema]
    @(d/transact conn (install-dci))
    @(d/transact conn event-schema)
    (let [user {:db/id (d/tempid :db.part/user)
                :user/name "allen"}
          event {:db/id (d/tempid :db.part/user)
                 :event/user (:db/id user)
                 :event/at (Date.)
                 :event/type :foo}
          index-val (index-value (d/db conn) event :event/user-at-type)]
      (inspect index-val)
      @(d/transact conn (concat [user event] index-val)))))
