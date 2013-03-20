Ideas under exploration include...

# Ongoing features

## Developing software using go

Is go a worthy language for distributed, server-side software
development?  Is it productive not only for initial development but
also for full-lifecycle issues of debuggability, issue analysis, crash
postmortems, hot re-deployment and optimization?

Does a go-based server run well under duress?  What techniques are
needed to handle duress (for example, stop-the-world GC)?

## High multi-tenancy

Database-as-a-service requires cost effectiveness of scaling the
number of accounts per host, allowing hosting operators to squeeze as
many tenants as they can onto a single host.

Can we reach 1M buckets per server?

(2013/02/14 performance - 101 buckets of 1024 vbuckets each requires
20% cpu utilization.)

# Done features

## Append-only, recovery-oriented file format

## MVCC

Readers have their own isolated views of the dataset.  Concurrent
readers and writers to not block each other, even if they have long
operations (such as a slow range scan).

## File descriptor limits

This software has an internal file service API which limits the number
of open file descriptors that will be used.  This helps support high
multi-tenancy.

## Time interval compaction

## Item metadata is evictable from memory

The underlying data structures allows item data and item metadata to
be fully evictable from memory.  This helps support high multi-tenancy
and high DGM (data greater than memory) scenarios.

(2013/02/14 - the server does not leverage the eviction features of
the underlying data structures, as an eviction policy needs to be
implemented.)

## Tree nodes are cached in memory

For higher performance, tree nodes are cached in memory to avoid disk
accesses.

## Multiple vbuckets per file

The data of a bucket is split across mutliple files (see
STORES_PER_BUCKET for the current default number of files per bucket).
The 1024 vbuckets or partitions of a bucket are then modulus'ed into
those files.  That is, each file has 256 vbuckets; so, 2 buckets would
mean 8 files, etc.

## Copy on write, immutable tree instead of separate persistence queue

The dirty nodes in the immutable balanced item tree are used to track
dirtiness, instead of managing a separate persistence queue.

## Continuous tree balancing

Instead of balancing search trees only at compaction, search trees are
re-balanced actively as part of mutation operations.

## Changes stream de-duplication

## Changes-stream instead of checkpoints

The design was built with UPR in mind, levering the changes-stream as
the "source of truth".  The main document-id or key-based index and
secondary indexes are all considered secondary to the changes-stream.

## VBucket state changes are first-class

Changes in vbucket state (active, replica, pending, dead) are recorded
explicitly as entries in the changes-stream.

## Range scans

In addition to key-value Get/Set/Delete, this software also supports
key-range scans (RGet) due to using an ordered, balanced search tree.

## Distributed, range partitioned indexes

This project is meant to explore distributed, range partitioned
indexes.  As part of that...

* Users may assign an optional allowed-key-range (inclusive min-key
and exclusive max-key) to each partition or vbucket.

* Additionally, this project implements the SPLIT_RANGE command that
can atomically split a vbucket into one or more vbuckets at given
split keys.

Those two features are the basis for supporting distributed,
range partitioned indexes.

## SASL auth

## Integrated REST webserver

The software can optionally listen on a REST/HTTP port for
administrative and monitoring needs.

## Integrated admin web UI

A web application that provides simple administrative commands and
information is provided.

## REST for DDL

Online commands to create/delete buckets and such are only available
via the REST protocol, not the memcached protocol.

## Aggregated stats

Per-second, per-minute, per-hour, and per-day level stat aggregates
are available via the REST protocol.

## Cross platform

Benefits of go include cross-platform support (linux, osx, windows)
and simple, single executable deployment.

## No privileges needed

## Unit test coverage from day zero

## Memcached binary-protocol focused

The initial focus is only on memcached binary protocol, with no
planned support or machinery for memcached ascii protocol.

## Integrated profiling

A REST API to capture CPU profiling information for N seconds is
supported.  Another REST API to catpure memory profiling info is
supported.  These can be used as input to the pprof tool.

## Relatively small codebase

2012/03/05 - ~4K lines of go code, not including unit test code or
library dependencies.

## Add/Replace commands

## Append/Prepend commands

## Incr/Decr commands

## Integrated javascript evaluation

Map/reduce functions, etc. likely by using Otto.

## Expiration

## Bucket quotas

Simple storage quota per bucket is supported.

## Management web U/I

Optional management web U/I is auto-downloaded on startup.

## REST API compatibility

Compability with Couchbase REST API for basic SDK cases, only for
single-node situations.
