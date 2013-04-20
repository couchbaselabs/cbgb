package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/steveyen/gkvlite"
)

// TODO: Make this configurable.  Not totally obvious since the
// actual server is a different package.
var fileService = NewFileService(32)

var persistPeriodic *periodically

const compact_every = 10000

type bucketstore struct {
	dirtiness     int64          // To track when we need flush to storage.
	bsf           unsafe.Pointer // *bucketstorefile
	bsfMemoryOnly *bucketstorefile
	endch         chan bool
	partitions    map[uint16]*partitionstore
	stats         *BucketStoreStats

	keyCompareForCollection func(collName string) gkvlite.KeyCompare

	diskLock sync.Mutex
}

func newBucketStore(path string, settings BucketSettings,
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

	bsf := NewBucketStoreFile(path, file, &BucketStoreStats{})
	bsfForGKVLite := bsf
	if settings.MemoryOnly >= MemoryOnly_LEVEL_PERSIST_NOTHING {
		bsfForGKVLite = nil
	}

	sc := gkvlite.StoreCallbacks{KeyCompareForCollection: keyCompareForCollection}

	store, err := gkvlite.NewStoreEx(bsfForGKVLite, sc)
	if err != nil {
		return nil, err
	}
	bsf.store = store

	var bsfMemoryOnly *bucketstorefile
	if settings.MemoryOnly > MemoryOnly_LEVEL_PERSIST_EVERYTHING {
		bsfMemoryOnly = NewBucketStoreFile(path, file, bsf.stats)
		bsfMemoryOnly.store, err = gkvlite.NewStoreEx(nil, sc)
		if err != nil {
			return nil, err
		}
	}

	return &bucketstore{
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
	return atomic.AddInt64(&s.dirtiness, -d), nil
}

func (s *bucketstore) periodicPersist(time.Time) bool {
	d, _ := s.Flush()
	if s.stats.Writes-s.stats.LastCompactAt > compact_every {
		s.stats.LastCompactAt = s.stats.Writes
		s.Compact()
	}
	if d > 0 {
		log.Printf("Flushed all but %v items (retrying)", d)
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

// ------------------------------------------------------------

// Rather than having everything in a single bucketstore struct, the
// additional/separate bucketstorefile allows us to track multiple
// bucketstorefile's, such as during compaction.
type bucketstorefile struct {
	path  string
	file  FileLike
	store *gkvlite.Store
	lock  sync.Mutex
	purge bool // When true, purge file when GC finalized.
	stats *BucketStoreStats
}

func NewBucketStoreFile(path string, file FileLike,
	stats *BucketStoreStats) *bucketstorefile {
	res := &bucketstorefile{
		path:  path,
		file:  file,
		stats: stats,
	}
	runtime.SetFinalizer(res, finalizeBucketStoreFile)
	return res
}

func finalizeBucketStoreFile(bsf *bucketstorefile) {
	if bsf.purge {
		bsf.purge = false
		os.Remove(bsf.path)
	}
}

func (bsf *bucketstorefile) apply(fun func()) {
	bsf.lock.Lock()
	defer bsf.lock.Unlock()
	fun()
}

// The following bucketstore methods implement the gkvlite.StoreFile
// interface: ReadAt(), WriteAt(), Stat().

func (bsf *bucketstorefile) ReadAt(p []byte, off int64) (n int, err error) {
	bsf.apply(func() {
		atomic.AddInt64(&bsf.stats.Reads, 1)
		n, err = bsf.file.ReadAt(p, off)
		if err != nil {
			atomic.AddInt64(&bsf.stats.ReadErrors, 1)
		}
		atomic.AddInt64(&bsf.stats.ReadBytes, int64(n))
	})
	return n, err
}

func (bsf *bucketstorefile) WriteAt(p []byte, off int64) (n int, err error) {
	bsf.apply(func() {
		if bsf.purge {
			err = fmt.Errorf("WriteAt to purgable bucketstorefile: %v",
				bsf.path)
			return
		}
		atomic.AddInt64(&bsf.stats.Writes, 1)
		n, err = bsf.file.WriteAt(p, off)
		if err != nil {
			atomic.AddInt64(&bsf.stats.WriteErrors, 1)
		}
		atomic.AddInt64(&bsf.stats.WriteBytes, int64(n))
	})
	return n, err
}

func (bsf *bucketstorefile) Stat() (fi os.FileInfo, err error) {
	bsf.apply(func() {
		atomic.AddInt64(&bsf.stats.Stats, 1)
		fi, err = bsf.file.Stat()
		if err != nil {
			atomic.AddInt64(&bsf.stats.StatErrors, 1)
		}
	})
	return fi, err
}

// Find the highest version-numbered store files in a bucket directory.
func latestStoreFileNames(dirForBucket string, storesPerBucket int,
	suffix string) (res []string, err error) {
	res = make([]string, storesPerBucket)
	for i := 0; i < storesPerBucket; i++ {
		prefix := strconv.FormatInt(int64(i), 10)
		res[i], err = latestStoreFileName(dirForBucket, prefix, suffix)
		if err != nil {
			return nil, err
		}
	}
	return res, err
}

func latestStoreFileName(dirForBucket string, prefix string, suffix string) (
	string, error) {
	fileInfos, err := ioutil.ReadDir(dirForBucket)
	if err != nil {
		return "", err
	}
	latestVer := 0
	latestName := makeStoreFileName(prefix, latestVer, suffix)
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}
		prefixCur, ver, err :=
			parseStoreFileName(fileInfo.Name(), suffix)
		if err != nil {
			continue
		}
		if prefixCur != prefix {
			continue
		}
		if latestVer < ver {
			latestVer = ver
			latestName = fileInfo.Name()
		}
	}
	return latestName, nil
}

// The store files follow a "PREFIX-VER.SUFFIX" naming pattern,
// such as "0-0.store".
func makeStoreFileName(prefix string, ver int, suffix string) string {
	return fmt.Sprintf("%v-%v.%v", prefix, ver, suffix)
}

func parseStoreFileName(fileName string, suffix string) (
	prefix string, ver int, err error) {
	if !strings.HasSuffix(fileName, "."+suffix) {
		return "", -1, fmt.Errorf("missing suffix: %v in filename: %v",
			suffix, fileName)
	}
	base := fileName[0 : len(fileName)-(1+len(suffix))]
	parts := strings.Split(base, "-")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		return "", -1, fmt.Errorf("not a store filename: %v", fileName)
	}
	ver, err = strconv.Atoi(parts[1])
	if err != nil {
		return "", -1, err
	}
	return parts[0], ver, nil
}
