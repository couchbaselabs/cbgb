package cbgb

import (
	"bytes"
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

type collKeysChanges func() (*gkvlite.Collection, *gkvlite.Collection)

type bucketstore struct {
	bsf             unsafe.Pointer // *bucketstorefile
	ch              chan *funreq
	dirtiness       int64
	flushInterval   time.Duration // Time between checking whether to flush.
	compactInterval time.Duration // Time between checking whether to compact.
	purgeTimeout    time.Duration // Time to keep old, unused file after compaction..
	stats           *bucketstorestats

	// Map of callbacks, which are invoked when we need the
	// bucketstore client to pause its mutations and when we want to
	// provide new keys and changes collections to the bucketstore
	// client (such as because of compaction).
	mapPauseSwapColls map[uint16]func(collKeysChanges)
}

type bucketstorefile struct {
	path          string
	file          *os.File
	store         *gkvlite.Store
	ch            chan *funreq
	sleepInterval time.Duration // Time until we sleep, closing file until next request.
	sleepPurge    time.Duration // When >0, purge file after sleeping + this duration.
	insomnia      bool          // When true, no sleeping.
	stats         *bucketstorestats
}

type bucketstorestats struct { // TODO: Unify stats naming conventions.
	TotFlush   uint64
	TotRead    uint64
	TotWrite   uint64
	TotStat    uint64
	TotSleep   uint64
	TotWake    uint64
	TotCompact uint64

	FlushErrors   uint64
	ReadErrors    uint64
	WriteErrors   uint64
	StatErrors    uint64
	WakeErrors    uint64
	CompactErrors uint64

	ReadBytes  uint64
	WriteBytes uint64
}

func newBucketStore(path string,
	flushInterval time.Duration,
	sleepInterval time.Duration,
	compactInterval time.Duration,
	purgeTimeout time.Duration) (*bucketstore, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	bss := &bucketstorestats{}
	bsf := &bucketstorefile{
		path:          path,
		file:          file,
		ch:            make(chan *funreq),
		sleepInterval: sleepInterval,
		sleepPurge:    time.Duration(0),
		insomnia:      false,
		stats:         bss,
	}
	go bsf.service()

	store, err := gkvlite.NewStore(bsf)
	if err != nil {
		bsf.Close()
		return nil, err
	}
	bsf.store = store

	res := &bucketstore{
		bsf:               unsafe.Pointer(bsf),
		ch:                make(chan *funreq),
		flushInterval:     flushInterval,
		compactInterval:   compactInterval,
		purgeTimeout:      purgeTimeout,
		stats:             bss,
		mapPauseSwapColls: make(map[uint16]func(collKeysChanges)),
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

	for {
		select {
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
			}
		case <-tickerC.C:
			s.compact()
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
		close(s.ch)
		s.BSF().Close()
	})
}

func (s *bucketstore) Stats() *bucketstorestats {
	bss := &bucketstorestats{}
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
	atomic.AddUint64(&s.stats.TotFlush, 1)
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

func (s *bucketstore) collKeysChanges(id uint16,
	psc func(collKeysChanges)) (*gkvlite.Collection, *gkvlite.Collection) {
	var k, c *gkvlite.Collection
	s.apply(func() {
		k = s.coll(fmt.Sprintf("%v%s", id, COLL_SUFFIX_KEYS))
		c = s.coll(fmt.Sprintf("%v%s", id, COLL_SUFFIX_CHANGES))
		s.mapPauseSwapColls[id] = psc
	})
	return k, c
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

func (s *bucketstore) get(keys *gkvlite.Collection, changes *gkvlite.Collection,
	key []byte) (*item, error) {
	return s.getItem(keys, changes, key, true)
}

func (s *bucketstore) getMeta(keys *gkvlite.Collection, changes *gkvlite.Collection,
	key []byte) (*item, error) {
	return s.getItem(keys, changes, key, false)
}

func (s *bucketstore) getItem(keys *gkvlite.Collection, changes *gkvlite.Collection,
	key []byte, withValue bool) (i *item, err error) {
	iItem, err := keys.GetItem(key, true)
	if err != nil {
		return nil, err
	}
	if iItem != nil {
		// TODO: Use the Transient field in gkvlite to optimize away
		// the double lookup here with memoization.
		// TODO: What if a compaction happens in between the lookups,
		// and the changes-feed no longer has the item?  Answer: compaction
		// must not remove items that the key-index references.
		cItem, err := changes.GetItem(iItem.Val, withValue)
		if err != nil {
			return nil, err
		}
		if cItem != nil {
			i := &item{key: key}
			if err = i.fromValueBytes(cItem.Val); err != nil {
				return nil, err
			}
			return i, nil
		}
	}
	return nil, nil
}

func (s *bucketstore) visitItems(keys *gkvlite.Collection, changes *gkvlite.Collection,
	start []byte, withValue bool,
	visitor func(*item) bool) (err error) {
	var vErr error
	v := func(iItem *gkvlite.Item) bool {
		cItem, vErr := changes.GetItem(iItem.Val, withValue)
		if vErr != nil {
			return false
		}
		if cItem == nil {
			return true // TODO: track this case; might have been compacted away.
		}
		i := &item{key: iItem.Key}
		if vErr = i.fromValueBytes(cItem.Val); vErr != nil {
			return false
		}
		return visitor(i)
	}
	if err := s.visit(keys, start, withValue, v); err != nil {
		return err
	}
	return vErr
}

func (s *bucketstore) visitChanges(changes *gkvlite.Collection,
	start []byte, withValue bool,
	visitor func(*item) bool) (err error) {
	var vErr error
	v := func(cItem *gkvlite.Item) bool {
		i := &item{}
		if vErr = i.fromValueBytes(cItem.Val); vErr != nil {
			return false
		}
		return visitor(i)
	}
	if err := s.visit(changes, start, withValue, v); err != nil {
		return err
	}
	return vErr
}

func (s *bucketstore) visit(coll *gkvlite.Collection,
	start []byte, withValue bool,
	v func(*gkvlite.Item) bool) (err error) {
	if start == nil {
		i, err := coll.MinItem(false)
		if err != nil {
			return err
		}
		if i == nil {
			return nil
		}
		start = i.Key
	}
	return coll.VisitItemsAscend(start, withValue, v)
}

// All the following mutation methods need to be called while
// single-threaded with respect to the mutating collection.

func (s *bucketstore) set(keys *gkvlite.Collection, changes *gkvlite.Collection,
	newItem *item, oldMeta *item) error {
	vBytes := newItem.toValueBytes()
	cBytes := casBytes(newItem.cas)

	// TODO: should we be de-duplicating older changes from the changes feed?
	if err := changes.Set(cBytes, vBytes); err != nil {
		return err
	}
	// An nil/empty key means this is a metadata change.
	if newItem.key != nil && len(newItem.key) > 0 {
		// TODO: What if we flush between the keys update and changes
		// update?  That could result in an inconsistent db file?
		// Solution idea #1 is to have load-time fixup, that
		// incorporates changes into the key-index.
		if err := keys.Set(newItem.key, cBytes); err != nil {
			return err
		}
	}
	s.dirty()
	return nil
}

func (s *bucketstore) del(keys *gkvlite.Collection, changes *gkvlite.Collection,
	key []byte, cas uint64) error {
	cBytes := casBytes(cas)
	vBytes := (&item{key: key, cas: cas}).markAsDeletion().toValueBytes()

	// TODO: should we be de-duplicating older changes from the changes feed?
	if err := changes.Set(cBytes, vBytes); err != nil {
		return err
	}
	// An nil/empty key means this is a metadata change.
	if key != nil && len(key) > 0 {
		// TODO: What if we flush between the keys update and changes
		// update?  That could result in an inconsistent db file?
		// Solution idea #1 is to have load-time fixup, that
		// incorporates changes into the key-index.
		if err := keys.Delete(key); err != nil {
			return err
		}
	}
	s.dirty()
	return nil
}

func (s *bucketstore) rangeCopy(srcColl *gkvlite.Collection,
	dst *bucketstore, dstColl *gkvlite.Collection,
	minKeyInclusive []byte, maxKeyExclusive []byte) error {
	minItem, err := srcColl.MinItem(false)
	if err != nil {
		return err
	}
	// TODO: What if we flush between the keys update and changes
	// update?  That could result in an inconsistent db file?
	// Solution idea #1 is to have load-time fixup, that
	// incorporates changes into the key-index.
	if minItem != nil {
		if err := collRangeCopy(srcColl, dstColl, minItem.Key,
			minKeyInclusive, maxKeyExclusive); err != nil {
			return err
		}
		dst.dirty()
	}
	return nil
}

func collRangeCopy(src *gkvlite.Collection, dst *gkvlite.Collection,
	minKey []byte,
	minKeyInclusive []byte,
	maxKeyExclusive []byte) error {
	var errVisit error
	visitor := func(i *gkvlite.Item) bool {
		if len(minKeyInclusive) > 0 &&
			bytes.Compare(i.Key, minKeyInclusive) < 0 {
			return true
		}
		if len(maxKeyExclusive) > 0 &&
			bytes.Compare(i.Key, maxKeyExclusive) >= 0 {
			return true
		}
		errVisit = dst.SetItem(i)
		if errVisit != nil {
			return false
		}
		return true
	}
	if errVisit != nil {
		return errVisit
	}
	return src.VisitItemsAscend(minKey, true, visitor)
}

// ------------------------------------------------------------

func (bsf *bucketstorefile) service() {
	defer func() {
		if bsf.file != nil {
			bsf.file.Close()
		}
	}()

	for {
		select {
		case r, ok := <-bsf.ch:
			if !ok {
				return
			}
			r.fun()
			close(r.res)
		case <-time.After(bsf.sleepInterval):
			if !bsf.insomnia && bsf.Sleep() != nil {
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
	close(bsf.ch)
}

func (bsf *bucketstorefile) Sleep() error {
	atomic.AddUint64(&bsf.stats.TotSleep, 1)

	bsf.file.Close()
	bsf.file = nil

	var r *funreq
	var ok bool

	if bsf.sleepPurge > time.Duration(0) {
		select {
		case r, ok = <-bsf.ch:
			if !ok {
				os.Remove(bsf.path) // TODO: Double check we're not the latest ver.
				return nil
			}
		case <-time.After(bsf.sleepPurge):
			close(bsf.ch)
			os.Remove(bsf.path) // TODO: Double check we're not the latest ver.
			return nil
		}
	} else {
		r, ok = <-bsf.ch
		if !ok {
			return nil
		}
	}

	atomic.AddUint64(&bsf.stats.TotWake, 1)

	file, err := os.OpenFile(bsf.path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		// TODO: Log this siesta-wakeup / re-open error.
		atomic.AddUint64(&bsf.stats.WakeErrors, 1)
		bsf.Close()
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
		atomic.AddUint64(&bsf.stats.TotRead, 1)
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
		atomic.AddUint64(&bsf.stats.TotWrite, 1)
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
		atomic.AddUint64(&bsf.stats.TotStat, 1)
		fi, err = bsf.file.Stat()
		if err != nil {
			atomic.AddUint64(&bsf.stats.StatErrors, 1)
		}
	})
	return fi, err
}

func (bss *bucketstorestats) Add(in *bucketstorestats) {
	bss.TotFlush += atomic.LoadUint64(&in.TotFlush)
	bss.TotRead += atomic.LoadUint64(&in.TotRead)
	bss.TotWrite += atomic.LoadUint64(&in.TotWrite)
	bss.TotStat += atomic.LoadUint64(&in.TotStat)
	bss.TotSleep += atomic.LoadUint64(&in.TotSleep)
	bss.TotWake += atomic.LoadUint64(&in.TotWake)

	bss.FlushErrors += atomic.LoadUint64(&in.FlushErrors)
	bss.ReadErrors += atomic.LoadUint64(&in.ReadErrors)
	bss.WriteErrors += atomic.LoadUint64(&in.WriteErrors)
	bss.StatErrors += atomic.LoadUint64(&in.StatErrors)
	bss.WakeErrors += atomic.LoadUint64(&in.WakeErrors)

	bss.ReadBytes += atomic.LoadUint64(&in.ReadBytes)
	bss.WriteBytes += atomic.LoadUint64(&in.WriteBytes)
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
