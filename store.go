package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/steveyen/gkvlite"
)

var fileService *FileService

var persistPeriodic *periodically

type bucketstore struct {
	name          string
	dirtiness     int64          // To track when we need flush to storage.
	bsf           unsafe.Pointer // *bucketstorefile
	bsfMemoryOnly *bucketstorefile
	endch         chan bool
	partitions    map[uint16]*partitionstore
	stats         *BucketStoreStats

	keyCompareForCollection func(collName string) gkvlite.KeyCompare

	diskLock sync.Mutex
}

func newBucketStore(name, path string, settings BucketSettings,
	keyCompareForCollection func(collName string) gkvlite.KeyCompare) (
	res *bucketstore, err error) {
	var file FileLike
	if settings.MemoryOnly < MemoryOnly_LEVEL_PERSIST_NOTHING {
		file, err = fileService.OpenFile(path, os.O_RDWR|os.O_CREATE)
		if err != nil {
			fmt.Printf("!!!! %v\n", err)
			return nil, err
		}
	}

	sc := mkBucketStoreCallbacks(keyCompareForCollection)

	bsf := NewBucketStoreFile(path, file, &BucketStoreStats{})
	bsfForGKVLite := bsf
	if settings.MemoryOnly >= MemoryOnly_LEVEL_PERSIST_NOTHING {
		bsfForGKVLite = nil
	}
	bsf.store, err = gkvlite.NewStoreEx(bsfForGKVLite, sc)
	if err != nil {
		return nil, err
	}

	var bsfMemoryOnly *bucketstorefile
	if settings.MemoryOnly > MemoryOnly_LEVEL_PERSIST_EVERYTHING {
		bsfMemoryOnly = NewBucketStoreFile(path, file, bsf.stats)
		bsfMemoryOnly.store, err = gkvlite.NewStoreEx(nil, sc)
		if err != nil {
			return nil, err
		}
	}

	return &bucketstore{
		name:          name,
		bsf:           unsafe.Pointer(bsf),
		bsfMemoryOnly: bsfMemoryOnly,
		endch:         make(chan bool),
		partitions:    make(map[uint16]*partitionstore),
		stats:         bsf.stats,
		keyCompareForCollection: keyCompareForCollection,
	}, nil
}

func (s *bucketstore) BSF() *bucketstorefile {
	return (*bucketstorefile)(atomic.LoadPointer(&s.bsf))
}

// Returns a bucketstorefile for data only, where the caller doesn't
// need to access metadata.  In a memory-only setting, the returned
// bucketstorefile would not be backed by a real file.
func (s *bucketstore) BSFData() *bucketstorefile {
	if s.bsfMemoryOnly != nil {
		return s.bsfMemoryOnly
	}
	return (*bucketstorefile)(atomic.LoadPointer(&s.bsf))
}

func (s *bucketstore) Close() {
	select {
	case <-s.endch:
	default:
		close(s.endch)
	}
}

func (s *bucketstore) Stats() *BucketStoreStats {
	bss := &BucketStoreStats{}
	bss.Add(s.stats)

	m := map[string]uint64{}
	s.BSF().store.Stats(m)
	if v, ok := m["fileSize"]; ok {
		bss.FileSize += int64(v)
	}
	if v, ok := m["nodeAllocs"]; ok {
		bss.NodeAllocs += int64(v)
	}
	return bss
}

// Returns the number of unprocessed dirty items and an error if we
// had an issue doing things.
func (s *bucketstore) Flush() (int64, error) {
	s.diskLock.Lock()
	defer s.diskLock.Unlock()
	return s.flush_unlocked()
}

func (s *bucketstore) flush_unlocked() (int64, error) {
	d := atomic.LoadInt64(&s.dirtiness)
	bsf := s.BSF()
	if bsf.file != nil {
		if err := bsf.store.Flush(); err != nil {
			atomic.AddInt64(&s.stats.FlushErrors, 1)
			return atomic.LoadInt64(&s.dirtiness), err
		}
	} // else, we're in memory-only mode.
	atomic.AddInt64(&s.stats.Flushes, 1)

	sendEvent(s.name, "persist", s.Stats())
	return atomic.AddInt64(&s.dirtiness, -d), nil
}

func (s *bucketstore) periodicPersist(time.Time) bool {
	d, _ := s.Flush()
	if s.stats.Writes-s.stats.LastCompactAt > int64(*compactEvery) {
		s.stats.LastCompactAt = s.stats.Writes
		if err := s.Compact(); err != nil {
			log.Printf("compact err: %v", err)
		}
	}
	if d > 0 {
		log.Printf("flushed all but %v items (retrying)", d)
	}
	return d > 0
}

func (s *bucketstore) mkPersistFun() func(time.Time) bool {
	return func(t time.Time) bool {
		return s.periodicPersist(t)
	}
}

func (s *bucketstore) dirty(force bool) {
	if force || s.bsfMemoryOnly == nil {
		newval := atomic.AddInt64(&s.dirtiness, 1)
		if newval == 1 {
			// TODO: Might want to kick off a persistence right now
			// rather than only schedule a periodic persistence.
			persistPeriodic.Register(s.endch, s.mkPersistFun())
		}
	}
}

func (s *bucketstore) collMeta(collName string) *gkvlite.Collection {
	c := s.BSF().store.GetCollection(collName)
	if c == nil {
		c = s.BSF().store.SetCollection(collName, nil)
	}
	return c
}

func (s *bucketstore) coll(collName string) *gkvlite.Collection {
	return s.collWithKeyCompare(collName, nil)
}

func (s *bucketstore) collWithKeyCompare(collName string,
	compare gkvlite.KeyCompare) *gkvlite.Collection {
	c := s.BSFData().store.GetCollection(collName)
	if c == nil {
		c = s.BSFData().store.SetCollection(collName, compare)
	}
	return c
}

func (s *bucketstore) apply(f func()) {
	if s == nil {
		return
	}
	s.diskLock.Lock()
	defer s.diskLock.Unlock()
	f()
}

func (s *bucketstore) getPartitionStore(vbid uint16) (res *partitionstore) {
	s.diskLock.Lock()
	defer s.diskLock.Unlock()

	k := s.coll(fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_KEYS))
	c := s.coll(fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_CHANGES))

	res = s.partitions[vbid]
	if res == nil {
		res = &partitionstore{vbid: vbid, parent: s}
		s.partitions[vbid] = res
	}
	res.keys = unsafe.Pointer(k)
	res.changes = unsafe.Pointer(c)
	return res
}

func mkBucketStoreCallbacks(keyCompareForCollection func(string) gkvlite.KeyCompare) gkvlite.StoreCallbacks {
	return gkvlite.StoreCallbacks{
		ItemValLength:           itemValLength,
		ItemValWrite:            itemValWrite,
		ItemValRead:             itemValRead,
		KeyCompareForCollection: keyCompareForCollection,
	}
}
