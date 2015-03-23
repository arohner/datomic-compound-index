(ns datomic-compound-index.core
  (:require [clojure.string :as str]
            [datomic.api :as d])
  (:import java.util.Date
           datomic.db.Datum))

(defprotocol DatomicRepresentation
  (to-datomic [x]
    "Given a value that can be stored in datomic, returns a string representation of the value for use in a compound-index"))

(extend-protocol DatomicRepresentation
  Object
  (to-datomic [x]
    (str x))
  java.util.Date
  (to-datomic [x]
    (.getTime x)))

(defn index-key*
  ([args]
   (index-key* args {}))
  ([args {:keys [separator]
          :or {separator "|"}}]
   (str (str/join separator (map to-datomic args)) separator)))

(defn split-options [key]
  (if (map? (last key))
    [(butlast key) (last key)]
    [key nil]))

(defn index-key
  "Create an index key. Can be used for inserting, or querying.

  key is a vector of datomc values, that can optionally end with a map of options

  options:
  - :separator String

  Use separator to specify a different separator charactor between
  values in the index. Note that if separator is used, it must be used
  for all insertions and queries on that attribute."
  [key]
  (apply index-key* (split-options key)))

(defn search
  "Search a compound index. attr is the attribute to search. Returns a seq of datoms. key, key1,key2 are vectors that will be passed to index-key.

   Passing a single key returns all datoms , two keys uses d/index-range.

   (search db :event/user-at-type [1234 (Date.) {:partial? true}])
 "
  ([db attr key]
   (let [key (index-key key)
         attr-id (:id (d/attribute db attr))]
     (seq (take-while (fn [^Datum d]
                        (and (= attr-id (.a d))
                             (.startsWith ^String (.v d) key))) (d/seek-datoms db :avet attr key))))))

(defn search-range
  "Search a compound index. Returns a seq of datoms.

  Uses d/index-range to find all datoms between key1 and key2, inclusive"
  [db attr key1 key2]
  (let [k1 (index-key key1)
        k2 (index-key key2)
        attr-id (:id (d/attribute db attr))]
    (->> (d/seek-datoms db :avet attr key1)
         (seq)
         (drop-while (fn [^Datum d]
                       (not (.startsWith ^String (.v d) k1))))
         (take-while (fn [^Datum d]
                       (and (= attr-id (.a d))
                            (or (<= (compare (.v d) k2) 0)
                                (.startsWith ^String (.v d) k2))))))))
