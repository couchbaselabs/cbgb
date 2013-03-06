package cbgb

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

// TODO: Make this configurable.  Note totally obvious since the
// actual server is a different package.
var fileService = NewFileService(32)

var flushRunner = newPeriodically(10*time.Second, 5)

const compact_every = 10000

type bucketstore struct {
	bsf           unsafe.Pointer // *bucketstorefile
	bsfMemoryOnly *bucketstorefile
	endch         chan bool
	dirtiness     int64
	partitions    map[uint16]*partitionstore
	stats         *BucketStoreStats

	diskLock sync.Mutex
}

type BucketStoreStats struct {
	Time int64 `json:"time"`

	Flushes       uint64 `json:"flushes"`
	Reads         uint64 `json:"reads"`
	Writes        uint64 `json:"writes"`
	Stats         uint64 `json:"stats"`
	Compacts      uint64 `json:"compacts"`
	LastCompactAt uint64 `json:"lastCompactAt"`

	FlushErrors   uint64 `json:"flushErrors"`
	ReadErrors    uint64 `json:"readErrors"`
	WriteErrors   uint64 `json:"writeErrors"`
	StatErrors    uint64 `json:"statErrors"`
	CompactErrors uint64 `json:"compactErrors"`

	ReadBytes  uint64 `json:"readBytes"`
	WriteBytes uint64 `json:"writeBytes"`
}

func newBucketStore(path string, settings BucketSettings) (*bucketstore, error) {
	file, err := fileService.OpenFile(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}

	bsf := NewBucketStoreFile(path, file, &BucketStoreStats{})
	store, err := gkvlite.NewStore(bsf)
	if err != nil {
		return nil, err
	}
	bsf.store = store

	var bsfMemoryOnly *bucketstorefile
	if settings.MemoryOnly {
		bsfMemoryOnly = NewBucketStoreFile(path, file, bsf.stats)
		bsfMemoryOnly.store, err = gkvlite.NewStore(nil)
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
	if err := s.BSF().store.Flush(); err != nil {
		atomic.AddUint64(&s.stats.FlushErrors, 1)
		return atomic.LoadInt64(&s.dirtiness), err
	}
	atomic.AddUint64(&s.stats.Flushes, 1)
	return atomic.AddInt64(&s.dirtiness, -d), nil
}

func (s *bucketstore) periodicFlushAndCompact(time.Time) bool {
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

func (s *bucketstore) mkFlushFun() func(time.Time) bool {
	return func(t time.Time) bool {
		return s.periodicFlushAndCompact(t)
	}
}

func (s *bucketstore) dirty(force bool) {
	if force || s.bsfMemoryOnly == nil {
		newval := atomic.AddInt64(&s.dirtiness, 1)
		if newval == 1 {
			flushRunner.Register(s.endch, s.mkFlushFun())
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
	c := s.BSFData().store.GetCollection(collName)
	if c == nil {
		c = s.BSFData().store.SetCollection(collName, nil)
	}
	return c
}

func (s *bucketstore) collNames() []string {
	return s.BSF().store.GetCollectionNames()
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

	// TODO: Handle cleanup of partitions when bucket/vbucket closes.
	res = s.partitions[vbid]
	if res == nil {
		res = &partitionstore{parent: s}
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
		atomic.AddUint64(&bsf.stats.Reads, 1)
		n, err = bsf.file.ReadAt(p, off)
		if err != nil {
			atomic.AddUint64(&bsf.stats.ReadErrors, 1)
		}
		atomic.AddUint64(&bsf.stats.ReadBytes, uint64(n))
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
		atomic.AddUint64(&bsf.stats.Writes, 1)
		n, err = bsf.file.WriteAt(p, off)
		if err != nil {
			atomic.AddUint64(&bsf.stats.WriteErrors, 1)
		}
		atomic.AddUint64(&bsf.stats.WriteBytes, uint64(n))
	})
	return n, err
}

func (bsf *bucketstorefile) Stat() (fi os.FileInfo, err error) {
	bsf.apply(func() {
		atomic.AddUint64(&bsf.stats.Stats, 1)
		fi, err = bsf.file.Stat()
		if err != nil {
			atomic.AddUint64(&bsf.stats.StatErrors, 1)
		}
	})
	return fi, err
}

func (bss *BucketStoreStats) Add(in *BucketStoreStats) {
	bss.Op(in, addUint64)
}

func (bss *BucketStoreStats) Sub(in *BucketStoreStats) {
	bss.Op(in, subUint64)
}

func (bss *BucketStoreStats) Op(in *BucketStoreStats, op func(uint64, uint64) uint64) {
	bss.Flushes = op(bss.Flushes, atomic.LoadUint64(&in.Flushes))
	bss.Reads = op(bss.Reads, atomic.LoadUint64(&in.Reads))
	bss.Writes = op(bss.Writes, atomic.LoadUint64(&in.Writes))
	bss.Stats = op(bss.Stats, atomic.LoadUint64(&in.Stats))
	bss.Compacts = op(bss.Compacts, atomic.LoadUint64(&in.Compacts))
	bss.FlushErrors = op(bss.FlushErrors, atomic.LoadUint64(&in.FlushErrors))
	bss.ReadErrors = op(bss.ReadErrors, atomic.LoadUint64(&in.ReadErrors))
	bss.WriteErrors = op(bss.WriteErrors, atomic.LoadUint64(&in.WriteErrors))
	bss.StatErrors = op(bss.StatErrors, atomic.LoadUint64(&in.StatErrors))
	bss.CompactErrors = op(bss.CompactErrors, atomic.LoadUint64(&in.CompactErrors))
	bss.ReadBytes = op(bss.ReadBytes, atomic.LoadUint64(&in.ReadBytes))
	bss.WriteBytes = op(bss.WriteBytes, atomic.LoadUint64(&in.WriteBytes))
}

func (bss *BucketStoreStats) Aggregate(in Aggregatable) {
	if in == nil {
		return
	}
	bss.Add(in.(*BucketStoreStats))
}

func (bss *BucketStoreStats) Equal(in *BucketStoreStats) bool {
	return bss.Flushes == atomic.LoadUint64(&in.Flushes) &&
		bss.Reads == atomic.LoadUint64(&in.Reads) &&
		bss.Writes == atomic.LoadUint64(&in.Writes) &&
		bss.Stats == atomic.LoadUint64(&in.Stats) &&
		bss.Compacts == atomic.LoadUint64(&in.Compacts) &&
		bss.FlushErrors == atomic.LoadUint64(&in.FlushErrors) &&
		bss.ReadErrors == atomic.LoadUint64(&in.ReadErrors) &&
		bss.WriteErrors == atomic.LoadUint64(&in.WriteErrors) &&
		bss.StatErrors == atomic.LoadUint64(&in.StatErrors) &&
		bss.CompactErrors == atomic.LoadUint64(&in.CompactErrors) &&
		bss.ReadBytes == atomic.LoadUint64(&in.ReadBytes) &&
		bss.WriteBytes == atomic.LoadUint64(&in.WriteBytes)
}

// Find the highest version-numbered store files in a bucket directory.
func latestStoreFileNames(dirForBucket string, storesPerBucket int) ([]string, error) {
	fileInfos, err := ioutil.ReadDir(dirForBucket)
	if err != nil {
		return nil, err
	}
	res := make([]string, storesPerBucket)
	for i := 0; i < storesPerBucket; i++ {
		latestVer := 0
		latestName := makeStoreFileName(i, latestVer)
		for _, fileInfo := range fileInfos {
			if fileInfo.IsDir() {
				continue
			}
			idx, ver, err := parseStoreFileName(fileInfo.Name())
			if err != nil {
				continue
			}
			if idx != i {
				continue
			}
			if latestVer < ver {
				latestVer = ver
				latestName = fileInfo.Name()
			}
		}
		res[i] = latestName
	}
	return res, err
}

// The store files follow a "IDX-VER.store" naming pattern.
func makeStoreFileName(idx int, ver int) string {
	return fmt.Sprintf("%v-%v.store", idx, ver)
}

func parseStoreFileName(fileName string) (idx int, ver int, err error) {
	if !strings.HasSuffix(fileName, ".store") {
		return -1, -1, fmt.Errorf("missing a store filename suffix: %v", fileName)
	}
	base := fileName[0 : len(fileName)-len(".store")]
	parts := strings.Split(base, "-")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		return -1, -1, fmt.Errorf("not a store filename: %v", fileName)
	}
	idx, err = strconv.Atoi(parts[0])
	if err != nil {
		return -1, -1, err
	}
	ver, err = strconv.Atoi(parts[1])
	if err != nil {
		return -1, -1, err
	}
	return idx, ver, nil
}
