(ns datomic-compound-index.core
  (:require [clojure.string :as str]
            [clojure.data.fressian :as fress]
            [clojure.set :as set]
            [datomic.api :as d])
  (:import java.util.Date
           datomic.db.Datum
           java.util.regex.Pattern
           java.nio.ByteBuffer))

(defprotocol Serialize
  (to-bytes [x]
    "Given a value that can be stored in datomic, returns a byte array
    representation of the value for use in a compound-index. to-bytes
    should be designed such that for two values x,y, if (< x y)
    then (< (to-bytes x) (to-bytes y))"))

(defn pad-str-bytes [^String str len]
  (let [bytes (.getBytes str)
        byte-len (min (count bytes) len)
        bb (ByteBuffer/allocate len)
        pad-len (- len (count bytes))]
    (.put bb bytes 0 byte-len)
    (.put bb ^bytes (into-array Byte/TYPE (take pad-len (repeat (byte 0)))))
    (.array bb)))

(def classmap {Long 0
               String 1
               clojure.lang.Keyword 2
               java.util.Date 3})

(extend-protocol Serialize
  String
  (to-bytes [x]
    (pad-str-bytes x 255))
  java.util.Date
  (to-bytes [x]
    (to-bytes (.getTime x)))
  java.lang.Long
  (to-bytes [x]
    (-> (ByteBuffer/allocate 8)
        (.putLong (+ x (bit-shift-left 1 62)))
        (.array))))

(defn serialize [x]
  (let [bytes (to-bytes x)]
    {:data bytes
     :metadata {:type (classmap (class x))
                :len (count bytes)}}))

(defn concat-bytes [arrs]
  (let [len (reduce + (map count arrs))
        bb (ByteBuffer/allocate len)]
    (doseq [a arrs]
      (.put bb ^bytes a))
    (.array bb)))

(defn conj-metas [metas]
  (->
   (reduce (fn [state meta]
             (let [{:keys [pos]} state
                   {:keys [len]} meta]
               (assert pos)
               (assert len)
               (-> state
                   (update-in [:return] (fnil conj []) (assoc meta :pos [pos (+ pos len)]))
                   (update-in [:pos] + len)))) {:pos 0
                                                :ret []} metas)
   :return))

(defn index-key
  "Create an index key. Can be used for querying. For inserting, see insert-index-key

  key is a vector of datomic values"
  [args]
  (let [s (map serialize args)]
    {:data (concat-bytes (map :data s))
     :metadata (conj-metas (map :metadata s))}))

(defn attr-meta-col-name [attr]
  (keyword (namespace attr) (str (name attr) "-metadata")))

(defn insert-index-key
  "Returns a map of data to transact. Returned data is missing a :db/id, so merge it into the entity map when transacting. i.e.

  (d/transact conn [(merge {:db/id id, :user/foo x, :user/bar y} (insert-index-key :user/foo-bar [x y]))])

  Arguments:

  attr - the compound index attribute. Should be indexed, of type :bytes. A second attribute named <attr>-metadata should also exist, of type :bytes, but not indexed.
  args - a vector of values"
  [attr args]
  (let [{:keys [data metadata]} (index-key args)]
    {attr data
     (attr-meta-col-name attr) (-> (fress/write metadata) (.array))}))

(defn key-compare
  "Compare two keys. Behaves like clojure.core/compare, returning -1,0,1, but also returns :subkey-a when a is a subkey of b, and :subkey-b when b is a subkey of a"
  [a b]
  (let [{adata :data ametas :metadata} a
        {bdata :data bmetas :metadata} b
        alast (count adata)
        blast (count bdata)]
    (assert adata)
    (assert bdata)
    (loop [pos 0
           ameta (-> ametas first)
           bmeta (-> bmetas first)]
      (cond
        (= pos alast blast) 0
        (= pos alast) :subkey-a
        (= pos blast) :subkey-b
        :else
        (let [abyte (aget ^bytes adata pos)
              bbyte (aget ^bytes bdata pos)
              astop (-> ameta :pos second)
              bstop (-> bmeta :pos second)]
          (cond
            (= pos astop bstop) (recur (inc pos) (rest ametas) (rest bmetas))
            (= pos astop) -1
            (= pos bstop) 1
            (< abyte bbyte) -1
            (> abyte bbyte) 1
            (= abyte bbyte) (recur (inc pos) ametas bmetas)
            :else (assert false)))))))

(defn deserialize!
  "Given an eid and the a compound-indexed attr, return the deserialized value"
  [db e attr]
  (let [meta-col (attr-meta-col-name attr)
        k (d/pull db [attr meta-col] e)
        metadata (get k meta-col)]
    (assert metadata)
    {:data (get k attr)
     :metadata (fress/read metadata)}))

(defn search
  "Search a compound index. attr is the attribute to search. Returns a seq of datoms. key is a vectors that will be passed to index-key.

   (search db :event/user-at-type [1234 (Date.)])
 "
  ([db attr key]
   (let [key (index-key key)
         attr-id (:id (d/attribute db attr))]
     (->> (d/seek-datoms db :avet attr key)
          (drop-while (fn [^Datum d]
                        (let [vkey (deserialize! db (.e d) attr)
                              result (key-compare vkey key)]
                          (and (not (= :subkey-b result))
                               (< result 0)))))
          (take-while (fn [^Datum d]
                        (let [result (key-compare key (deserialize! db (.e d) attr))]
                          (and (= attr-id (.a d))
                               (or (= :subkey-a result)
                                   (= 0 result))))))
          (seq)))))

(defn search-range
  "Search a compound index. Returns a seq of datoms.

  Uses d/index-range to find all datoms between key1 and key2, inclusive"
  [db attr key1 key2]
  (let [key1 (index-key key1)
        key2 (index-key key2)
        attr-id (:id (d/attribute db attr))]
    (->> (d/seek-datoms db :avet attr key1)
         (drop-while (fn [^Datum d]
                       (and (= attr-id (.a d))
                            (let [vkey (deserialize! db (.e d) attr)
                                  result (key-compare vkey key1)]
                              (and (not= :subkey-b result)
                                   (< result 0))))))
         (take-while (fn [^Datum d]
                       (and (= attr-id (.a d))
                            (let [vkey (deserialize! db (.e d) attr)
                                  result1 (delay (key-compare key1 vkey))
                                  result2 (delay (key-compare vkey key2))]
                              (or (= :subkey-a @result1)
                                  (= :subkey-b @result2)
                                  (and
                                   (<= @result1 0)
                                   (<= @result2 0)))))))
         (seq))))
