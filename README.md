# datomic-compound-index

A library that adds support for simple compound indices to datomic.

# Motivating Example

Suppose you have time-series event data. You have lots of users, and
each user creates lots of events. Each event has a timestamp. You
would like to query "find all events for User X on day Y".

schema:

(attribute :event/at :db.type/instant
           :db/index true)
(attribute :event/user :db.type/ref
           :db/index true)

Datomic queries use a single index only. If the DB is indexed on users, the
query will sequential scan on all events from all time, created by
that user. If the DB is indexed on event time, the query will
sequential scan on all events on that day, for all users.

# How It Works

This readme spends a lot of time talking about how to solve the
problem without datomic-compound-index (dci), so you'll understand
what's going on under the covers.

The solution is to use a compound-index, an indexed attribute that contains both values.

(attribute :event/user-at :db.type/string
           :db/index true)

Let's say the current time is 1426630517988 (epoch), and the user we're interested in is user-id 1234. Then we store

"1234|1426630517988" in the attribute :event/user-at. Here we're assigning "|" as the separator character.

(d/transact conn [{:db/id (d/tempid :events)
                   :event/user 1234
                   :event/at 1426630517988
                   :event/user-at "1234|1426630517988"}])

To find all events created by that user today, without dci, we'd use d/index-range:

(d/index-range db :event/user-at "1234|1426550400000" "1234|1426636800000")

This can also be extended past two attributes. Let's say we want to find all events of a specific type, by a certain user, on a specific day:

(attribute :event/type :db.type/keyword)
(attribute :event/user-at-type :db.type/string
           :db/index true)

(d/index-range db :event/user-at-type "1234|1426550400000|:foo" "1234|1426636800000|:foo")

(of course, if your DB has :event/user-at-type, then :event/user-at is unnecessary).

# Usage

dci provides functions for the use cases described
above. dci/index-key is the main entry point, it creates the keys used
for inserting and querying on compound indices.

Create an indexed attribute, of type string. In a comment/docstring,
specify the type and order of values that will be indexed. When
inserting new entities, use index-key on the attribute:

(let [event-at (Date.)
      user-id 1234
      event-type :foo]
  (d/transact conn [{:db/id (d/tempid :eventss)
                   :event/type event-type
                   :event/user-at-type (dci/index-key [user-id event-at event-type])}]))

## Query

## single key

If you're querying for an entire compound key (not partial), you can use d/q as normal, using index-key for the value

(d/q '[:find ?e :in $ ?v :where [?e :event/user-at-type ?v]] db (index-key [:foo :bar :baz]))

### range

When querying across a range, e.g. all events in a single day, use dci/search-range:

(dci/search-range db :event/user-at [1234 (-> (time/today-at-midnight) to-date)] [1234 (-> (time/today-at-midnight) (time/plus (time/days 1)) to-date)])

Note that search-range is inclusive of the starting key, and exclusive of the ending key.

Note that index-key converts values to resonable string representations, so j.u.Date instances can be passed in. (See the DatomicRepresentation Protocol in source)

## Partial
When searching for a partial key (i.e. you know the first two values of a 3-value compound key) use dci/search:

(dci/search db :user/type-created-at [:foo {:partial? true}])

This returns all datoms where the initial part of the key is identical. dci/search can also be used for entire key lookups, but d/q might be more idiomatic.

dci/search-range also supports :partial?

(d/index-range db :event/user-at-type "1234|1426550400000|" "1234|1426636800000|")

Note the use of the separator at the end, this is to avoid accidentally matching a longer string, like 1426550400000<b>1</b>

(dci/search-range db :event/user-at-type [1234 1426550400000 {:partial? true}] [1234 1426636800000 {:partial? true}])

# Limitations

- index-key does not automatically update the compound index attribute when any of 'source' attributes change.
- Using index-key with {:partial? true} won't work in d/q, unless you're searching for a completely specified key, because d/q doesn't support substring matches.
- keys are sorted & searched via string representation, lexographically.
- datomic supports using re-find in a database function, but AFAICT, there's no way for it to use an index, so avoid re-find.

## License

Copyright © 2015 Allen Rohner

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
