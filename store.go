package cbgb

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/steveyen/gkvlite"
)

// TODO: Make this configurable.  Note totally obvious since the
// actual server is a different package.
var fileService = NewFileService(32)

type bucketstore struct {
	bsf             unsafe.Pointer // *bucketstorefile
	endch           chan bool
	ch              chan *funreq
	dirtiness       int64
	flushInterval   time.Duration // Time between checking whether to flush.
	compactInterval time.Duration // Time between checking whether to compact.
	purgeTimeout    time.Duration // Time to keep old, unused file after compaction.
	partitions      map[uint16]*partitionstore
	stats           *BucketStoreStats
}

type BucketStoreStats struct {
	Time int64 `json:"time"`

	Flushes  uint64 `json:"flushes"`
	Reads    uint64 `json:"reads"`
	Writes   uint64 `json:"writes"`
	Stats    uint64 `json:"stats"`
	Sleeps   uint64 `json:"sleeps"`
	Wakes    uint64 `json:"wakes"`
	Compacts uint64 `json:"compacts"`

	FlushErrors   uint64 `json:"flushErrors"`
	ReadErrors    uint64 `json:"readErrors"`
	WriteErrors   uint64 `json:"writeErrors"`
	StatErrors    uint64 `json:"statErrors"`
	WakeErrors    uint64 `json:"wakeErrors"`
	CompactErrors uint64 `json:"compactErrors"`

	ReadBytes  uint64 `json:"readBytes"`
	WriteBytes uint64 `json:"writeBytes"`
}

func newBucketStore(path string, settings BucketSettings) (*bucketstore, error) {
	file, err := fileService.OpenFile(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}

	bsf := &bucketstorefile{
		path:          path,
		file:          file,
		endch:         make(chan bool),
		ch:            make(chan *funreq),
		sleepInterval: settings.SleepInterval,
		sleepPurge:    time.Duration(0),
		insomnia:      false,
		stats:         &BucketStoreStats{},
	}
	go bsf.service()

	store, err := gkvlite.NewStore(bsf)
	if err != nil {
		bsf.Close()
		return nil, err
	}
	bsf.store = store

	res := &bucketstore{
		bsf:             unsafe.Pointer(bsf),
		endch:           make(chan bool),
		ch:              make(chan *funreq),
		flushInterval:   settings.FlushInterval,
		compactInterval: settings.CompactInterval,
		purgeTimeout:    settings.PurgeTimeout,
		partitions:      make(map[uint16]*partitionstore),
		stats:           bsf.stats,
	}
	go res.service()

	return res, nil
}

func (s *bucketstore) BSF() *bucketstorefile {
	return (*bucketstorefile)(atomic.LoadPointer(&s.bsf))
}

func (s *bucketstore) service() {
	tickerF := time.NewTicker(s.flushInterval)
	defer tickerF.Stop()

	tickerC := time.NewTicker(s.compactInterval)
	defer tickerC.Stop()

	numFlushes := uint64(0)
	lastCompact := uint64(0)

	for {
		select {
		case <-s.endch:
			return
		case r, ok := <-s.ch:
			if !ok {
				return
			}
			r.fun()
			close(r.res)
		case <-tickerF.C:
			d := atomic.LoadInt64(&s.dirtiness)
			if d > 0 {
				s.flush()
				numFlushes++
			}
		case <-tickerC.C:
			if lastCompact != numFlushes {
				s.compact()
				lastCompact = numFlushes
			}
		}
	}
}

func (s *bucketstore) apply(fun func()) {
	req := &funreq{fun: fun, res: make(chan bool)}
	s.ch <- req
	<-req.res
}

func (s *bucketstore) Close() {
	s.apply(func() {
		select {
		case <-s.endch:
		default:
			close(s.endch)
			s.BSF().Close()
		}
	})
}

func (s *bucketstore) Stats() *BucketStoreStats {
	bss := &BucketStoreStats{}
	bss.Add(s.stats)
	return bss
}

func (s *bucketstore) Flush() (err error) {
	s.apply(func() {
		err = s.flush()
	})
	return err
}

func (s *bucketstore) flush() error {
	d := atomic.LoadInt64(&s.dirtiness)
	if err := s.BSF().store.Flush(); err != nil {
		atomic.AddUint64(&s.stats.FlushErrors, 1)
		return err
	}
	atomic.AddInt64(&s.dirtiness, -d)
	atomic.AddUint64(&s.stats.Flushes, 1)
	return nil
}

func (s *bucketstore) dirty() {
	atomic.AddInt64(&s.dirtiness, 1)
}

func (s *bucketstore) Compact() (err error) {
	s.apply(func() {
		err = s.compact()
	})
	return err
}

func (s *bucketstore) coll(collName string) *gkvlite.Collection {
	c := s.BSF().store.GetCollection(collName)
	if c == nil {
		c = s.BSF().store.SetCollection(collName, nil)
	}
	return c
}

func (s *bucketstore) collNames() []string {
	return s.BSF().store.GetCollectionNames()
}

func (s *bucketstore) collExists(collName string) bool {
	return s.BSF().store.GetCollection(collName) != nil
}

func (s *bucketstore) getPartitionStore(vbid uint16) (res *partitionstore) {
	s.apply(func() {
		k := s.coll(fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_KEYS))
		c := s.coll(fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_CHANGES))

		// TODO: Handle numItems and lastCas initialization.
		// TODO: Handle cleanup of partitions when bucket/vbucket closes.
		res = s.partitions[vbid]
		if res == nil {
			res = &partitionstore{parent: s}
			s.partitions[vbid] = res
		}
		res.keys = unsafe.Pointer(k)
		res.changes = unsafe.Pointer(c)
	})
	return res
}

// ------------------------------------------------------------

// Rather than having everything in a single bucketstore struct, the
// additional/separate bucketstorefile allows us to track multiple
// bucketstorefile's, such as during compaction.
type bucketstorefile struct {
	path          string
	file          FileLike
	store         *gkvlite.Store
	endch         chan bool
	ch            chan *funreq
	sleepInterval time.Duration // Time until we sleep, closing file until next request.
	sleepPurge    time.Duration // When >0, purge file after sleeping + this duration.
	insomnia      bool          // When true, no sleeping.
	stats         *BucketStoreStats
}

func (bsf *bucketstorefile) service() {
	defer func() {
		if bsf.file != nil {
			bsf.file.Close()
		}
	}()

	for {
		select {
		case <-bsf.endch:
			return
		case r, ok := <-bsf.ch:
			if !ok {
				bsf.end()
				return
			}
			r.fun()
			close(r.res)
		case <-time.After(bsf.sleepInterval):
			// TODO: Check for dirtiness before we sleep?
			if !bsf.insomnia && bsf.Sleep() != nil {
				bsf.end()
				return
			}
		}
	}
}

func (bsf *bucketstorefile) apply(fun func()) {
	req := &funreq{fun: fun, res: make(chan bool)}
	bsf.ch <- req
	<-req.res
}

func (bsf *bucketstorefile) Close() {
	bsf.apply(func() {
		bsf.end()
	})
}

func (bsf *bucketstorefile) end() {
	if !bsf.isEnded() {
		close(bsf.endch)
	}
}

func (bsf *bucketstorefile) isEnded() bool {
	select {
	case <-bsf.endch:
		return true
	default:
	}
	return false
}

func (bsf *bucketstorefile) Sleep() error {
	// TODO: Flush before we sleep?
	atomic.AddUint64(&bsf.stats.Sleeps, 1)

	bsf.file.Close()
	bsf.file = nil

	var r *funreq
	var ok bool

	if bsf.sleepPurge > time.Duration(0) {
		select {
		case <-bsf.endch:
			os.Remove(bsf.path) // TODO: Double check we're not the latest ver.
			return fmt.Errorf("ended while sleeping to purge: %v", bsf.path)
		case r, ok = <-bsf.ch:
			if !ok {
				bsf.end()
				os.Remove(bsf.path) // TODO: Double check we're not the latest ver.
				return fmt.Errorf("closed while sleeping to purge: %v", bsf.path)
			}
			// Reach here if we need to wake up from sleeping.
		case <-time.After(bsf.sleepPurge):
			bsf.end()
			os.Remove(bsf.path) // TODO: Double check we're not the latest ver.
			return fmt.Errorf("purged after sleeping: %v", bsf.path)
		}
	} else {
		select {
		case <-bsf.endch:
			return fmt.Errorf("ended while sleeping: %v", bsf.path)
		case r, ok = <-bsf.ch:
			if !ok {
				bsf.end()
				return fmt.Errorf("closed while sleeping: %v", bsf.path)
			}
		}
	}

	atomic.AddUint64(&bsf.stats.Wakes, 1)

	file, err := fileService.OpenFile(bsf.path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		// TODO: Log this siesta-wakeup / re-open error.
		atomic.AddUint64(&bsf.stats.WakeErrors, 1)
		bsf.end()
		return err
	}
	bsf.file = file

	r.fun()
	close(r.res)
	return nil
}

// The following bucketstore methods implement the gkvlite.StoreFile
// interface.

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
		if bsf.sleepPurge > time.Duration(0) {
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
	bss.Sleeps = op(bss.Sleeps, atomic.LoadUint64(&in.Sleeps))
	bss.Wakes = op(bss.Wakes, atomic.LoadUint64(&in.Wakes))
	bss.Compacts = op(bss.Compacts, atomic.LoadUint64(&in.Compacts))
	bss.FlushErrors = op(bss.FlushErrors, atomic.LoadUint64(&in.FlushErrors))
	bss.ReadErrors = op(bss.ReadErrors, atomic.LoadUint64(&in.ReadErrors))
	bss.WriteErrors = op(bss.WriteErrors, atomic.LoadUint64(&in.WriteErrors))
	bss.StatErrors = op(bss.StatErrors, atomic.LoadUint64(&in.StatErrors))
	bss.WakeErrors = op(bss.WakeErrors, atomic.LoadUint64(&in.WakeErrors))
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
		bss.Sleeps == atomic.LoadUint64(&in.Sleeps) &&
		bss.Wakes == atomic.LoadUint64(&in.Wakes) &&
		bss.Compacts == atomic.LoadUint64(&in.Compacts) &&
		bss.FlushErrors == atomic.LoadUint64(&in.FlushErrors) &&
		bss.ReadErrors == atomic.LoadUint64(&in.ReadErrors) &&
		bss.WriteErrors == atomic.LoadUint64(&in.WriteErrors) &&
		bss.StatErrors == atomic.LoadUint64(&in.StatErrors) &&
		bss.WakeErrors == atomic.LoadUint64(&in.WakeErrors) &&
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
