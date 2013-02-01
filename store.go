package cbgb

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/steveyen/gkvlite"
)

type collItemsChanges func() (*gkvlite.Collection, *gkvlite.Collection)

type bucketstore struct {
	bsf             unsafe.Pointer // *bucketstorefile
	ch              chan *funreq
	dirtiness       int64
	flushInterval   time.Duration // Time between checking whether to flush.
	compactInterval time.Duration // Time between checking whether to compact.
	stats           *bucketstorestats

	// Map of callbacks, which are invoked when we need the
	// bucketstore client to pause its mutations and when we want to
	// provide new items and changes collections to the bucketstore
	// client (such as because of compaction).
	mapPauseSwapColls map[uint16]func(collItemsChanges)
}

type bucketstorefile struct {
	path          string
	file          *os.File
	store         *gkvlite.Store
	ch            chan *funreq
	sleepInterval time.Duration // Time until we sleep, closing file until next request.
	stats         *bucketstorestats
}

type bucketstorestats struct { // TODO: Unify stats naming conventions.
	TotFlush uint64
	TotRead  uint64
	TotWrite uint64
	TotStat  uint64
	TotSleep uint64
	TotWake  uint64

	FlushErrors uint64
	ReadErrors  uint64
	WriteErrors uint64
	StatErrors  uint64
	WakeErrors  uint64

	ReadBytes  uint64
	WriteBytes uint64
}

func newBucketStore(path string,
	flushInterval time.Duration,
	compactInterval time.Duration,
	sleepInterval time.Duration) (*bucketstore, error) {
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
		stats:             bss,
		mapPauseSwapColls: make(map[uint16]func(collItemsChanges)),
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
			bsfCompact, err := s.compact()
			if err != nil {
				atomic.StorePointer(&s.bsf, unsafe.Pointer(bsfCompact))
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

func (s *bucketstore) compact() (*bucketstorefile, error) {
	// This should be invoked via bucketstore serice(), so there's
	// no concurrent flushing.
	bsf := s.BSF()
	// TODO: copy VBMeta (vbm).
	// TODO: what if there's an error during compaction, when we're half moved over?
	// TODO: how to get a items index that's sync'ed with the changes stream?
	// TODO: perhaps need to copy bunch of items index history (multiple roots)?
	return bsf, nil
}

func (s *bucketstore) collItemsChanges(id uint16,
	psc func(collItemsChanges)) (*gkvlite.Collection, *gkvlite.Collection) {
	var i, c *gkvlite.Collection
	s.apply(func() {
		i = s.coll(fmt.Sprintf("%v%s", id, COLL_SUFFIX_ITEMS))
		c = s.coll(fmt.Sprintf("%v%s", id, COLL_SUFFIX_CHANGES))
		s.mapPauseSwapColls[id] = psc
	})
	return i, c
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

func (s *bucketstore) get(items *gkvlite.Collection, changes *gkvlite.Collection,
	key []byte) (*item, error) {
	return s.getItem(items, changes, key, true)
}

func (s *bucketstore) getMeta(items *gkvlite.Collection, changes *gkvlite.Collection,
	key []byte) (*item, error) {
	return s.getItem(items, changes, key, false)
}

func (s *bucketstore) getItem(items *gkvlite.Collection, changes *gkvlite.Collection,
	key []byte, withValue bool) (i *item, err error) {
	iItem, err := items.GetItem(key, true)
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

func (s *bucketstore) visitItems(items *gkvlite.Collection, changes *gkvlite.Collection,
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
	if err := s.visit(items, start, withValue, v); err != nil {
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

func (s *bucketstore) set(items *gkvlite.Collection, changes *gkvlite.Collection,
	newItem *item, oldMeta *item) error {
	vBytes := newItem.toValueBytes()
	cBytes := casBytes(newItem.cas)

	// TODO: should we be de-duplicating older changes from the changes feed?
	if err := changes.Set(cBytes, vBytes); err != nil {
		return err
	}
	// An nil/empty key means this is a metadata change.
	if newItem.key != nil && len(newItem.key) > 0 {
		// TODO: What if we flush between the items update and changes
		// update?  That could result in an inconsistent db file?
		// Solution idea #1 is to have load-time fixup, that
		// incorporates changes into the key-index.
		if err := items.Set(newItem.key, cBytes); err != nil {
			return err
		}
	}
	s.dirty()
	return nil
}

func (s *bucketstore) del(items *gkvlite.Collection, changes *gkvlite.Collection,
	key []byte, cas uint64) error {
	cBytes := casBytes(cas)

	// Empty value means it was a deletion.
	// TODO: should we be de-duplicating older changes from the changes feed?
	if err := changes.Set(cBytes, []byte("")); err != nil {
		return err
	}
	// An nil/empty key means this is a metadata change.
	if key != nil && len(key) > 0 {
		// TODO: What if we flush between the items update and changes
		// update?  That could result in an inconsistent db file?
		// Solution idea #1 is to have load-time fixup, that
		// incorporates changes into the key-index.
		if err := items.Delete(key); err != nil {
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
	// TODO: What if we flush between the items update and changes
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
			if bsf.Sleep() != nil {
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

	// TODO: If we sleep for too long, then we should die.
	r, ok := <-bsf.ch
	if !ok {
		return nil
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
