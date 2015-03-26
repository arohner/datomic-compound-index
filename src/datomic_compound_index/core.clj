(ns datomic-compound-index.core
  (:require [clojure.string :as str]
            [clojure.data.fressian :as fress]
            [datomic.api :as d])
  (:import java.util.Date
           datomic.db.Datum
           java.util.regex.Pattern))


(defn attribute
  "Install a new schema attribute"
  [ident type & {:as opts}]
  (merge {:db/id (d/tempid :db.part/db)
          :db/ident ident
          :db/valueType type
          :db.install/_attribute :db.part/db}
         {:db/cardinality :db.cardinality/one}
         opts))

(defn install-dci
  "Returns a set of datums to define attributes. Must d/transact results before attempting to install a compound index"
  []
  [(attribute :dci/refs :db.type/ref
              :db/cardinality :db.cardinality/many)
   (attribute :dci/index-name :db.type/keyword
              :db/index true)
   (attribute :dci/segment-attr :db.type/keyword)
   (attribute :dci/segment-index :db.type/long)

   (attribute :dci/entity :db.type/ref)
   ;;;...
   ])

(defn compound-index
  "Defines a new compound index. Returns a set of datums that insert attributes. d/transact results"
  [index-name segments]
  (concat

   (concat (map-indexed (fn [i s]
                          {:db/id (d/tempid :db.part/user)
                           :dci/index-name index-name
                           :dci/segment-attr s
                           :dci/segment-index i}) segments))))

(defn get-index-attrs
  "Given the name of a compound index, returns a vector of attr names that comprise the index"
  [db index]
  (->> (d/q '[:find [(pull ?e [:dci/segment-attr :dci/segment-index]) ...] :in $ ?index :where [?e :dci/index-name ?index]] db index)
       (sort-by :dci/segment-index)
       (map :dci/segment-attr)))

(def value-type-map {:db.type/instant :dci/value-instant
                     :db.type/keyword :dci/value-keyword
                     :db.type/long :dci/value-long
                     :db.type/ref :dci/value-ref
                     :db.type/string :dci/value-string})

(defn index-value
  "Returns a set of datums to properly index entity e. e should be a d/entity, or a map that contains all values in the index"
  [db e index]
  (let [attrs (get-index-attrs db index)]
    (assert (seq attrs))
    (let [state (reduce (fn [{:keys [last-val datums] :as state} attr]
                          (update-in state [:datums] conj (merge
                                                           {:db/id (d/tempid :db.part/user)
                                                            :dci/entity (:db/id e)}
                                                           (when last-val
                                                             {:dci/refs last-val})))) {:last-val nil
                                                                                       :datums []} (reverse attrs))
          datums (:datums state)
          first-segment (last datums)
          index-entity (d/q '[:find ?e . :in $ ?index :where [?e :dci/index-name ?index]] db index)]
      (assert index-entity)
      (conj datums {:db/id index-entity
                    :dci/refs first-segment}))))

(defn search* [db entities attrs vals]
  (mapcat (d/q '[:find ?e :in $ :where ]))
  )
(defn search [db index vals]
  (let [a (get-index-attrs index)
        v (first vals)]
    (d/q '[:find ?e :in $ :where [?e ?a ?v]] db a v))
  )
