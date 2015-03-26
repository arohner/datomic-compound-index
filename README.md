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

Continuing with the previous example, we're indexing on event/at, then event/user. We'll call this a two-segement index.
DCI creates a tree of datomic entities. At each level of the index, there is an entity representing a single value, with an attribute of type ref pointing at all sub-values. i.e. in this example, there is an entity for each timestamp, with each timestamp entity having values pointing at users.

(attribute :event/user-at-1-value :db.type/ref)
(attribute :event/user-at-1-refs :db.type/ref
           :db/cardinality :db.cardinality/many)