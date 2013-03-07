A quick tour of the key data structures...

cbgb depends gkvlite, using gkvlite for ordered, concurrently
accessible, persistent collections of key-value items.  See:
https://github.com/steveyen/gkvlite

Of note, the terms vbucket and partiton are synonyms.  VBucket was the
original term, which we found was confusing as being too similar to
"bucket".

Key data structures
===================

buckets
-------

Responsibility: map of bucket's

bucket (or livebucket)
----------------------

Responsibility: array of vbucket's and array of bucketstore's

Each vbucket is modulo assigned to a bucketstore, so multiple vbuckets
may be assigned to the same bucketstore.

vbucket
-------

Responsibility: dispatches network protocol requests into
partitionstore method invocations. Of note, mutation requests are
serialized on a per-vbucket basis.  Read-only requests (eg., gets,
range-scans) are meant to be non-serialized and concurrent.

bucketstore
-----------

Responsibility: manages a bucketstorefile, especially controlling
flushing of dirty items to the bucketstorefile and compaction of
of the bucketstorefile.

Also manages a map of partitionstore's.

bucketstorefile
---------------

Responsibility: manages an os.File and an associated
gkvlite.Store, including serializing reads and writes to the os.File.
Also handles sleeping (aka, hibernation or passivation) when there
haven't been any file operations in awhile, in which case the os.File
and gkvlite.Store are closed until the next request.

partitionstore
--------------

Responsibility: manages the changes-stream collection and
key-index collection for a single vbucket/partition.  Serializes
mutations against those collections.

Lock ordering
=============

For deadlock prevention, acquire left-ward/higher locks before
right-ward/lower locks.

    bucketstore.apply
      vbucket.Apply
        partitionstore.mutate
