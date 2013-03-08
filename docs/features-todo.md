# Parity TODO features

The following features need implementation, but do not really break
any new ground.

## TAP receiving

Currently, the project is only a TAP source, not a TAP receiver.

## TAP takeover

## TAP filtering

## 1K buckets chained by TAP replication streams

## Immediately consistent views

## More memcached commands

More memcached commands need implementation, including
gat, touch, observe.

## Eviction policy

Probably random eviction, to start.

## REST API compatibility

Compability with Couchbase REST API needs implementation.

## Flushing & compacting by activity, not only by time interval

Flushing and compacting are currently triggered only by time interval,
not by changes to item counts or fragmentation metrics due to
mutations.

## Histograms

Capturing performance histograms needs implementation.

# Exploration TODO features

The following features and ideas, in no particular order, are on the
radar for exploration.

## Declarative indexes

JSONPointer as an optional alternative to javascript map functions.

## Unified Protocol for Replication (UPR)

## Compression

## Ad-hoc queries

Integration with tuq (another go-based project) for ad-hoc query
support should be possible.

## Sub-key structure

Similar to the redis project, this project will explore sub-key
structure like sorted lists, sets, etc.

The underlying data storage layer (gkvlite) already supports nestable,
ordered sub-key collections (a key X can have ordered sub-keys (A, B,
C), and sub-keys X.A can have their own ordered sub-keys (x, y, z), so
X.A.x).  Deleting or expiring a parent key (like X) gets rid of the
whole sub-tree.  But these need to be exposed through a protocol.

## Message queue transactions

This project will explore API and implementation around message queue
transactions, as described in the proposal for Couchbase support for
enterprise, transactional applications (2012).

## Server-wide quotas

In addition to bucket-centric quotas, this project will also explore
additional server-wide quotas of resource utilization.

## Hot item optimizations

The underlying treap (tree + heap) data structure allows items to have
a priority.  Hotter items might receive incremented priorities, so
that they will migrate closer to the tops of the balanced search
trees.

## Sync-gateway integration

## Document dictionary compression

## Network compression

## Bucket password hashing

## Cluster orchestration

This project is currently single node.

## Server-side logic (stored procedures)
