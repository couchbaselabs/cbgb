package cbgb

import (
	"bytes"
	"os"
	"sync/atomic"
	"time"

	"github.com/steveyen/gkvlite"
)

type bucketstorereq struct {
	cb  func(*bucketstore)
	res chan bool
}

type bucketstore struct {
	path          string
	file          *os.File
	store         *gkvlite.Store
	ch            chan *bucketstorereq
	fch           chan *bucketstorereq // Channel for file operations
	dirtiness     int64
	flushInterval time.Duration
}

func newBucketStore(path string, flushInterval time.Duration) (*bucketstore, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	res := &bucketstore{
		path:          path,
		file:          file,
		ch:            make(chan *bucketstorereq),
		fch:           make(chan *bucketstorereq),
		flushInterval: flushInterval,
	}

	go res.service(res.ch)
	go res.serviceFile(res.fch)

	store, err := gkvlite.NewStore(res)
	if err != nil {
		res.Close()
		return nil, err
	}
	res.store = store

	return res, nil
}

func (s *bucketstore) service(ch chan *bucketstorereq) {
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return
			}
			r.cb(s)
			close(r.res)
		case <-ticker.C:
			d := atomic.LoadInt64(&s.dirtiness)
			if d > 0 {
				err := s.store.Flush() // TODO: Handle flush error.
				if err == nil {
					atomic.AddInt64(&s.dirtiness, -d)
				}
			}
		}
	}
}

func (s *bucketstore) serviceFile(ch chan *bucketstorereq) {
	defer s.file.Close()

	for r := range ch {
		r.cb(s)
		close(r.res)
	}
}

func (s *bucketstore) apply(cb func(*bucketstore)) {
	req := &bucketstorereq{cb: cb, res: make(chan bool)}
	s.ch <- req
	<-req.res
}

func (s *bucketstore) applyFile(cb func(*bucketstore)) {
	req := &bucketstorereq{cb: cb, res: make(chan bool)}
	s.fch <- req
	<-req.res
}

func (s *bucketstore) Close() {
	close(s.ch)
	close(s.fch)
}

func (s *bucketstore) flush() (err error) {
	s.apply(func(sLocked *bucketstore) {
		d := atomic.LoadInt64(&s.dirtiness)
		err = sLocked.store.Flush()
		if err == nil {
			atomic.AddInt64(&s.dirtiness, -d)
		}
	})
	return err
}

func (s *bucketstore) dirty() {
	atomic.AddInt64(&s.dirtiness, 1)
}

func (s *bucketstore) coll(collName string) *gkvlite.Collection {
	c := s.store.GetCollection(collName)
	if c == nil {
		c = s.store.SetCollection(collName, nil)
	}
	return c
}

func (s *bucketstore) collNames() []string {
	return s.store.GetCollectionNames()
}

func (s *bucketstore) collExists(collName string) bool {
	return s.store.GetCollection(collName) != nil
}

func (s *bucketstore) get(items string, changes string, key []byte) (*item, error) {
	return s.getItem(items, changes, key, true)
}

func (s *bucketstore) getMeta(items string, changes string, key []byte) (*item, error) {
	return s.getItem(items, changes, key, false)
}

func (s *bucketstore) getItem(items string, changes string,
	key []byte, withValue bool) (i *item, err error) {
	iItem, err := s.coll(items).GetItem(key, true)
	if err != nil {
		return nil, err
	}
	if iItem != nil {
		// TODO: Use the Transient field in gkvlite to optimize away
		// the double lookup here with memoization.
		// TODO: What if a compaction happens in between the lookups,
		// and the changes-feed no longer has the item?  Answer: compaction
		// must not remove items that the key-index references.
		cItem, err := s.coll(changes).GetItem(iItem.Val, withValue)
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

func (s *bucketstore) visitItems(items string, changes string, start []byte, withValue bool,
	visitor func(*item) bool) (err error) {
	var vErr error
	v := func(iItem *gkvlite.Item) bool {
		cItem, vErr := s.coll(changes).GetItem(iItem.Val, withValue)
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
	if err := s.visit(s.coll(items), start, withValue, v); err != nil {
		return err
	}
	return vErr
}

func (s *bucketstore) visitChanges(collName string, start []byte, withValue bool,
	visitor func(*item) bool) (err error) {
	var vErr error
	v := func(cItem *gkvlite.Item) bool {
		i := &item{}
		if vErr = i.fromValueBytes(cItem.Val); vErr != nil {
			return false
		}
		return visitor(i)
	}
	if err := s.visit(s.coll(collName), start, withValue, v); err != nil {
		return err
	}
	return vErr
}

func (s *bucketstore) visit(coll *gkvlite.Collection, start []byte, withValue bool,
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

func (s *bucketstore) set(items string, changes string,
	newItem *item, oldMeta *item) error {
	collItems := s.coll(items)
	collChanges := s.coll(changes)
	vBytes := newItem.toValueBytes()
	cBytes := casBytes(newItem.cas)

	// TODO: should we be de-duplicating older changes from the changes feed?
	if err := collChanges.Set(cBytes, vBytes); err != nil {
		return err
	}
	// TODO: What if we flush between the items update and changes
	// update?  That could result in an inconsistent db file?
	// Solution idea #1 is to have load-time fixup, that
	// incorporates changes into the key-index.
	if err := collItems.Set(newItem.key, cBytes); err != nil {
		return err
	}
	s.dirty()
	return nil
}

func (s *bucketstore) del(items string, changes string,
	key []byte, cas uint64) error {
	collItems := s.coll(items)
	collChanges := s.coll(changes)
	cBytes := casBytes(cas)

	// Empty value means it was a deletion.
	// TODO: should we be de-duplicating older changes from the changes feed?
	if err := collChanges.Set(cBytes, []byte("")); err != nil {
		return err
	}
	// TODO: What if we flush between the items update and changes
	// update?  That could result in an inconsistent db file?
	// Solution idea #1 is to have load-time fixup, that
	// incorporates changes into the key-index.
	if err := collItems.Delete(key); err != nil {
		return err
	}
	s.dirty()
	return nil
}

func (s *bucketstore) rangeCopy(srcColl string, dst *bucketstore, dstColl string,
	minKeyInclusive []byte, maxKeyExclusive []byte) error {
	minItem, err := s.coll(srcColl).MinItem(false)
	if err != nil {
		return err
	}
	// TODO: What if we flush between the items update and changes
	// update?  That could result in an inconsistent db file?
	// Solution idea #1 is to have load-time fixup, that
	// incorporates changes into the key-index.
	if minItem != nil {
		if err := collRangeCopy(s.coll(srcColl), dst.coll(dstColl), minItem.Key,
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

// The following methods implement the gkvlite.StoreFile interface.

func (s *bucketstore) ReadAt(p []byte, off int64) (n int, err error) {
	s.applyFile(func(bs *bucketstore) {
		n, err = bs.file.ReadAt(p, off)
	})
	return n, err
}

func (s *bucketstore) WriteAt(p []byte, off int64) (n int, err error) {
	s.applyFile(func(bs *bucketstore) {
		n, err = bs.file.WriteAt(p, off)
	})
	return n, err
}

func (s *bucketstore) Stat() (fi os.FileInfo, err error) {
	s.applyFile(func(bs *bucketstore) {
		fi, err = bs.file.Stat()
	})
	return fi, err
}
