cbgb - prototyping couchbase ideas
==================================

cbgb is a project where we can experiment with next-generation server
ideas at a cheaper cost (due to ability to ignore existing-production
compatibility requirements) and at fast pace (due to experimenting
with go as the implementation language).

Ideas under exploration include...

Developing software using go
----------------------------

Is go a worthy language for distributed, server-side software
development?  Is it productive not only for initial development but
also for full-lifecycle issues of debuggability, issue analysis, crash
postmortems, hot re-deployment and optimization?

Does a go-based server run well under duress?  What techniques are
needed to handle duress (for example, stop-the-world GC)?

High multi-tenancy
------------------

Database-as-a-service requires cost effectiveness of scaling the
number of accounts per host, allowing hosting operators to squeeze as
many tenants as they can onto a single host.

Can we reach 1M buckets per server?

(2013/02/14 performance - 101 buckets of 1024 vbuckets each requires
20% cpu utilization.)

Bucket hibernation
------------------

This software supports a configurable amount of time before it closes
the file descriptors that are held by an unaccessed bucket.  This
helps support high multi-tenancy.

Item metadata is evictable from memory
--------------------------------------

The underlying data structures allows item data and item metadata to
be fully evictable from memory.  This helps support high multi-tenancy
and high DGM (data greater than memory) scenarios.

(2013/02/14 - the server does not leverage the eviction features of
the underlying data structures, as an eviction policy needs to be
implemented.)

Tree nodes are cached in memory
-------------------------------

For higher performance, tree nodes may be cached in memory to avoid
disk seeks.

Multiple vbuckets per file
--------------------------

The data of a bucket is split across mutliple files (4 files is the
current default).  The 1024 vbuckets or partitions of a buck are then
modulus'ed into those files.  That is, each file has 256 vbuckets;
and, 2 buckets would mean 8 files.

Copy on write, immutable tree instead of separate persistence queue
-------------------------------------------------------------------

The dirty nodes in the immutable balanced item tree are used to track
dirtiness, instead of managing a separate persistence queue.

Continuous tree balancing
-------------------------

Instead of balancing search trees only at compaction, search trees are
re-balanced actively as part of mutation operations.

Changes-stream instead of checkpoints
-------------------------------------

The design was built with UPR in mind, levering the changes-stream as
the "source of truth".  The main document-id or key-based index and
secondary indexes are all considered secondary to the changes-stream.

MVCC
----

Readers have their own isolated views of the dataset.  Concurrent
readers and writers to not block each other, even if they have long
operations (such as a slow range scan).

VBucket state changes are first-class
-------------------------------------

Changes in vbucket state (active, replica, pending, dead) are recorded
explicitly as entries in the changes-stream.

Range scans
-----------

In addition to key-value Get/Set/Delete, this software also supports
key-range scans due to using an ordered, balanced search tree.

Distributed, range partitioned indexes
--------------------------------------

This project is meant to explore distributed, range partitioned
indexes.  As part of that...

* Users may assign an optional allowed-key-range (inclusive min-key
and exclusive max-key) to each partition or vbucket.

* Additionally, this project implements the SPLIT_RANGE command that
can atomically split a vbucket into one or more vbuckets at given
split keys.

Those two features are the basis for supporting distributed,
range partitioned indexes.

Integrated REST webserver
-------------------------

The software can optionally listen on a REST/HTTP port for
administrative and monitoring needs.

Integrated admin web UI
-----------------------

A web application that provides simple administrative commands and
information is provided.

REST for DDL
------------

Online commands to create/delete buckets and such are only available
via the REST protocol, not the memcached protocol.

Aggregated stats
----------------

Per-second, per-minute, per-hour, and per-day level stat aggregates
are available via the REST protocol.

Download & run simplicity
-------------------------

Benefits of go include cross-platform support (linux, osx, windows)
and simple, single executable deployment.

No privileges needed
--------------------

For fewer speedbumps to developer adoption, the goal is to allow "test
drive" developers to download a single executable into whatever
directory they want and just run.  No sudo/root/package-management is
required, with the goal that all batteries are included.

Unit test coverage from day zero
--------------------------------

Memcached binary-protocol focused
---------------------------------

The initial focus is only on memcached binary protocol, with no
planned support or machinery for memcached ascii protocol.

Integrated profiling
--------------------

A REST API to capture process CPU profiling information for N seconds
is supported.

Relatively small codebase
-------------------------

2012/02/16 - ~3.5K lines of go code, not including unit test code or
library dependencies.

Similar features
================

The following features are similar to existing couchbase approaches.

Append-only, recovery-oriented file format
------------------------------------------

Time interval compaction
------------------------

Parity TODO features
====================

The following features need implementation, but do not really break
any new ground.

SASL auth
---------

TAP receiving
-------------

Currently, the project is only a TAP source, not a TAP receiver.

TAP takeover
------------

Changes stream de-duplication
-----------------------------

More memcached commands
-----------------------

More memcached commands need implementation, including
append/prepend, incr/decr, add/replace.

Expiration
----------

Expiration needs implementation.

REST API compatibility
----------------------

Compability with Couchbase REST API needs implementation.

Flushing & compacting by activity, not only by time interval
------------------------------------------------------------

Flushing and compacting are currently triggered only by time interval,
not by changes to item counts or fragmentation metrics due to
mutations.

Resource quotas
---------------

High/low watermarks and such need implementation.

Histograms
----------

Capturing performance histograms needs implementation.

Exploration TODO features
=========================

The following features and ideas, in no particular order, are on the
radar for exploration.

Integrated javascript evaluation
--------------------------------

Map/reduce functions, etc. likely by using Otto.

Declarative indexes
-------------------

JSONPointer as an optional alternative to javascript map functions.

Unified Protocol for Replication (UPR)
--------------------------------------

Compression
-----------

Immediately consistent views
----------------------------

Ad-hoc queries
--------------

Integration with tuq (another go-based project) for ad-hoc query
support should be possible.

Sub-key structure
-----------------

Similar to the redis project, this project will explore sub-key
structure like sorted lists, sets, etc.

The underlying data storage layer (gkvlite) already supports nestable,
ordered sub-key collections (a key X can have ordered sub-keys (A, B,
C), and sub-keys X.A can have their own ordered sub-keys (x, y, z), so
X.A.x).  Deleting or expiring a parent key (like X) gets rid of the
whole sub-tree.  But these need to be exposed through a protocol.

Message queue transactions
--------------------------

This project will explore API and implementation around message queue
transactions, as described in the proposal for Couchbase support for
enterprise, transactional applications (2012).

Server-wide quotas
------------------

In addition to bucket-centric quotas, this project will also explore
additional server-wide quotas of resource utilization.

Hot item optimizations
----------------------

The underlying treap (tree + heap) data structure allows items to have
a priority.  Hotter items might receive incremented priorities, so
that they will migrate closer to the tops of the balanced search
trees.

Sync-gateway integration
------------------------

Key dictionary compression
--------------------------

Network compression
-------------------

Bucket password hashing
-----------------------
