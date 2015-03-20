(ns datomic-compound-index.core
  (:require [clojure.string :as str]
            [datomic.api :as d])
  (:import java.util.Date))

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
  ([args {:keys [partial?
                 separator]
          :or {partial? false
               separator "|"}}]
   (str (str/join separator (map to-datomic args)) separator)))

(defn strip-partial [key]
  (if (map? (last key))
    [(butlast key) (last key)]
    [key nil]))

(defn index-key
  "Create an index key. Can be used for inserting, or querying.

  key is a vector of datomc values, that can optionally end with a map of options

  options:
  - :partial? Boolean
  - :separator String

  When querying against a partial index (i.e. specifying fewer
  attributes than are in the index), set partial? true, which adds a
  separator to the end of the key, so queries don't inadvertently
  match other rows

  Use separator to specify a different separator charactor between
  values in the index. Note that if separator is used, it must be used
  for all insertions and queries on that attribute.
"
  [key]
  (apply index-key* (strip-partial key)))

(defn search
  "Search a compound index. attr is the attribute to search. Returns a seq of datoms. key, key1,key2 are vectors that will be passed to index-key.

   Passing a single key returns all datoms , two keys uses d/index-range.

   (search db :event/user-at-type [1234 (Date.) {:partial? true}])
 "
  ([db attr key]
   (let [key (index-key key)]
     (seq (take-while (fn [d]
                        (.startsWith (.v d) key)) (d/seek-datoms db :avet attr key))))))

(defn search-range
  "Search a compound index. Returns a seq of datoms.

  Uses d/index-range to find all datoms between key1 and key2, inclusive of key1, *exclusive* of key2"
  [db attr key1 key2]
  (seq (d/index-range db attr
                      (index-key key1)
                      (index-key key2))))
