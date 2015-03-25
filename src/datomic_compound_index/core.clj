(ns datomic-compound-index.core
  (:require [clojure.string :as str]
            [clojure.data.fressian :as fress]
            [datomic.api :as d])
  (:import java.util.Date
           datomic.db.Datum
           java.util.regex.Pattern))

;; approaches
;; get encoding to work properly

(defprotocol SerializeKey
  (to-bytes [this])
  (from-bytes [this]))

(extend-protocol SerializeKey
  java.lang.Long
  (to-bytes [x]
    (* -1 x))
  (from-bytes [x]
    (* -1 x)))

(defn serialize-key []
  ()
  )

(defn deserialize-key [])
(defn index-key
  "Create an index key. Can be used for inserting, or querying.

  key is a vector of datomic values"
  [key]
  (.array (fress/write (to-bytes key))))

(defn compound-compare
  "compare operation. If the key is shorter than value, and all previous sub-keys match, returns 0"
  [ak bk]
  (loop [[a & as] ak
         [b & bs] bk]
    (if (or (nil? a)
            (nil? b))
      0
      (let [result (compare a b)]
        (if (= 0 result)
          (recur as bs)
          result)))))

(defn search
  "Search a compound index. attr is the attribute to search. Returns a seq of datoms. key, key1,key2 are vectors that will be passed to index-key.

   Passing a single key returns all datoms , two keys uses d/index-range.

   (search db :event/user-at-type [1234 (Date.) {:partial? true}])
 "
  ([db attr key]
   (let [attr-id (:id (d/attribute db attr))]
     (->> (d/seek-datoms db :avet attr key)
          (mapv identity)
          (seq)
          (drop-while (fn [^Datum d]
                        (and (= attr-id (.a d))
                             (< (compound-compare (fress/read (.v d)) key) 0))))
          (take-while (fn [^Datum d]
                        (and (= attr-id (.a d))
                             (= (compound-compare (fress/read (.v d)) key) 0))))))))

(defn search-range
  "Search a compound index. Returns a seq of datoms.

  Uses d/index-range to find all datoms between key1 and key2, inclusive"
  [db attr key1 key2]
  (let [attr-id (:id (d/attribute db attr))]
    (->> (d/seek-datoms db :avet attr key1)
         (seq)
         (drop-while (fn [^Datum d]
                       (and (= attr-id (.a d))
                            (< (compound-compare (fress/read (.v d)) key1) 0))))
         (take-while (fn [^Datum d]
                       (and (= attr-id (.a d))
                            (<= (compound-compare (fress/read (.v d)) key2) 0)))))))
