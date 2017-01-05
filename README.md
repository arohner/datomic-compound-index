# datomic-compound-index

A library that adds support for simple compound indices to datomic.

# Not Functional

Due to limitations in Datomic, this library cannot work as orginally designed. Code is left here in case it's useful to anyone.

DCI uses byte-arrays to form compound keys. Datomic relies on the
JVM's implementation of hashcode() and compare() to correctly sort
values in the datomic indexes. Since `hashcode` and `compare` are
completely inadequate (they don't actually sort by contents), DCI ends
up storing elements randomly, so lookup is `O(n)` rather than
`O(log(n))`.

The concepts behind DCI are probably sounds, and the approach should work with some other type that can smuggle bytes correctly, presumably BigInt

# Installation

```clojure
(:require [datomic-compound-index.core :as dci])
```

# Motivating Example

Suppose you have time-series event data. You have lots of users, and
each user creates lots of events. Each event has a timestamp. You
would like to query "find all events for User X on day Y".

schema:

```clojure
(attribute :event/at :db.type/instant
           :db/index true)
(attribute :event/user :db.type/ref
           :db/index true)
```

Datomic queries use a single index only. If the DB is indexed on users, the
query will sequential scan on all events from all time, created by
that user. If the DB is indexed on event time, the query will
sequential scan on all events on that day, for all users.

# How It Works

This readme spends a lot of time talking about how to solve the
problem without datomic-compound-index (dci), so you'll understand
what's going on under the covers.

The solution is to use a compound-index, an indexed attribute that contains both values.

```clojure
(attribute :event/user-at :db.type/string
           :db/index true)
```

Let's say the current time is `1426630517988` (epoch), and the user we're interested in is user-id `1234`. Then we store

"1234|1426630517988|" in the attribute :event/user-at. (In reality, dci uses byte-arrays rather than strings, but the principle is the same).

```clojure
(d/transact conn [{:db/id (d/tempid :events)
                   :event/user 1234
                   :event/at 1426630517988
                   :event/user-at "1234|1426630517988|"}])
```

To find all events created by that user today, without dci, we'd use d/index-range:

```clojure
(d/index-range db :event/user-at "1234|1426550400000|" "1234|1426636800000|")
```

This can also be extended past two attributes. Let's say we want to find all events of a specific type, by a certain user, on a specific day:

```clojure
(attribute :event/type :db.type/keyword)
(attribute :event/user-at-type :db.type/string
           :db/index true)
```

```clojure
(d/index-range db :event/user-at-type "1234|1426550400000|:foo|" "1234|1426636800000|:foo|")
```

(of course, if your DB has :event/user-at-type, then :event/user-at is unnecessary).

# Features

- efficiently query on attributes comprised of multiple values
- efficiently query on partial keys, i.e. an attribute indexed on 3
- attributes, and you know the first two parts.

# Usage

dci provides functions for the use cases described
above. dci/index-key is the main entry point, it creates the keys used
for inserting and querying on compound indices.

Create an indexed attribute, of type bytes. Create a second attribute
of type bytes with the same name, with the suffix "-metadata". If the
indexed attribute is named ":user/foo", the second attribute would be
named ":user/foo-metadata". In a comment/docstring, specify the type
and order of values that will be indexed.

When inserting new entities,
use insert-index-key to create the value for the compound-indexed attribute:

```clojure
(let [event-at (Date.)
      user-id 1234
      event-type :foo]
  @(d/transact conn [(merge
                     {:db/id (d/tempid :eventss)
                      :event/type event-type}
                      (dci/insert-index-key :event/user-at-type [user-id event-at event-type]))]))
```

`insert-index-key` takes two args, the compound attribute, and a
vector of values. Every entity with this attribute must use values of
the same type, in the same order. insert-index-key returns a map
containing {<attr> <bytes>, <attr-metadata> <bytes>}, so it should be
merged in with the entity map. Values can be String or Long (or
something that implements d-c-i.core/Serialize, see below).

## Query

## single key

If you're querying for an entire compound key (not partial), you can use `d/q` as normal, using index-key for the value

```clojure
(d/q '[:find ?e :in $ ?v :where [?e :event/user-at-type ?v]] db (dci/index-key [:foo :bar :baz]))
```

### range

When querying across a range, e.g. all events in a single day, use dci/search-range:

```clojure
(dci/search-range db :event/user-at [1234 (-> (time/today-at-midnight) to-date)] [1234 (-> (time/today-at-midnight) (time/plus (time/days 1)) to-date)])
```

`search-range` takes two keys, and returns the seq of datoms between them.

(Note that index-key converts values to byte-array representations, so j.u.Date instances can be passed in. (See the Serialize Protocol in source))

## Partial
When searching for a partial key (i.e. you know the first two values of a 3-value compound key) use dci/search:

```clojure
(dci/search db :user/type-created-at [123 :foo])
```

This returns all datoms where the initial part of the key is identical. dci/search can also be used for entire key lookups, but d/q might be more idiomatic.

dci/search-range also supports partial key searches:

```clojure
(dci/search-range db :event/user-at-type [1234 1426550400000] [1234 1426636800000])
```

Under the covers, partial searches work by creating a partial
index-key (i.e. a byte-array shorter than the 'full' key), and then
matching on values stored in the DB. Datomic indices are used
(d/seek-datoms, and d/index-range, respectively), so these are
efficient.

# Limitations

- DCI stores attributes as *fixed width* byte arrays. This is 8 bytes *per value*,
  for longs, and 255 bytes for strings. Since String storage is
  expensive, it's highly recommended that you intern strings in
  another entity, and store the db/id instead, when possible.
- Due to bytes being signed on the JVM, and Datomic's byte-array
  comparison, longs can only use 7 bits out of each byte. This limits the
  maximum number that can be stored to 2^55 - 1, rather than the 2^64 - 1 you'd expect from an 8-byte long.
- index-key does not automatically update the compound index attribute
  when any of 'source' attributes change.
- Using d/q with a partial index-key won't work.

# Changelog

Note that dci is still very early. I'm using it in staging, but not yet production. If you do use it production, be able to re-create the values of your compound indices (i.e. store the component pieces in other attributes).

- 0.3.1: small performance improvements in `search` and `searc-range`
- 0.3.0: store longs in 7 bits per byte. This is a breaking schema change.
- 0.2.5: Fix a bug that caused incorrect results due to java signed bytes
- 0.2.4: fixes an exception caused by going out-of-bounds with d/seek-datoms
- 0.2.0: switch to using byte-arrays rather than strings. More tests.
- 0.1.2: made search-range inclusive of the end key. Add type hints.
- 0.1.1: Added separator characters to all keys, even complete ones. This is a breaking schema change; meaning values inserted via 0.1.0 will not be accessible in 0.1.1 unless they're re-asserted.
- 0.1.0: Initial Release

## License

Copyright © 2015 Allen Rohner

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
