A quick tour of the key data structures...

cbgb depends on gkvlite, using gkvlite for ordered, concurrently
accessible, persistent collections of key-value items.  See:
https://github.com/steveyen/gkvlite

Of note, the terms vbucket and partiton are synonyms.  VBucket was the
original term, which many found was too confusing as too similar to
"bucket".

Key data structures
===================

buckets
-------

Manages a map of bucket's keyed by bucket name.

bucket (or livebucket)
----------------------

Manages an array of vbucket's and an array of bucketstore's.  Each
vbucket is modulo assigned to a single bucketstore, so multiple
vbuckets may be assigned to share a bucketstore.

Also manages a special vbucket for design docs.

vbucket
-------

Transforms network protocol requests into method invocations on a
single, associated partitionstore.  Of note, mutation requests are
serialized.  Read-only requests (eg., gets, range-scans) are
non-serialized and concurrent.

bucketstore
-----------

Manages flushing dirty items to a bucketstorefile and manages
compacting that bucketstorefile (by copying data to a new
bucketstorefile and switching to the new bucketstorefile).

Also manages a map of partitionstore's keyed by vbid.

bucketstorefile
---------------

Manages a FileLike instance (which is a virtualized os.File) and an
associated gkvlite.Store.  Also, serializes access to the FileLike
instance.

partitionstore
--------------

Manages the changes-stream collection and key-index collection that's
associated to a single partition / vbucket.  Serializes mutations to
those collections.

file service
------------

Provides file access virtualization, so that higher layers can "open"
high numbers of FileLike objects, but the actual os.File usage at any
time is much lower with a fixed count of open os.File's.

periodic task service
---------------------

The 'periodically' service allows scheduled, asynchronous tasks to be
completed by pools of worker goroutines.

Lock ordering
=============

For deadlock prevention, acquire left-ward/higher locks before
right-ward/lower locks.

    bucketstore.apply
      vbucket.Apply
        partitionstore.mutate
