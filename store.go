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
	dirtiness     int64
	flushInterval time.Duration
}

func newBucketStore(path string, flushInterval time.Duration) (*bucketstore, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	store, err := gkvlite.NewStore(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	res := &bucketstore{
		file:          file,
		store:         store,
		ch:            make(chan *bucketstorereq, 1),
		flushInterval: flushInterval,
	}
	go res.service()
	return res, nil
}

func (s *bucketstore) service() {
	defer s.file.Close()

	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case r, ok := <-s.ch:
			if !ok {
				return
			}
			r.cb(s)
			if r.res != nil {
				close(r.res)
			}
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

func (s *bucketstore) apply(synchronous bool, cb func(*bucketstore)) {
	req := &bucketstorereq{cb: cb}
	if synchronous {
		req.res = make(chan bool)
	}
	s.ch <- req
	if synchronous {
		<-req.res
	}
}

func (s *bucketstore) Close() {
	close(s.ch)
}

func (s *bucketstore) flush() (err error) {
	s.apply(true, func(sLocked *bucketstore) {
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

func (s *bucketstore) get(items string, key []byte) (*item, error) {
	return s.getItem(items, key, true)
}

func (s *bucketstore) getMeta(items string, key []byte) (*item, error) {
	return s.getItem(items, key, false)
}

func (s *bucketstore) getItem(items string, key []byte, withValue bool) (i *item, err error) {
	v, err := s.coll(items).GetItem(key, withValue)
	if err != nil {
		return nil, err
	}
	if v != nil {
		i := &item{key: key}
		if err = i.fromValueBytes(v.Val); err != nil {
			return nil, err
		}
		return i, nil
	}
	return nil, nil
}

func (s *bucketstore) visit(collName string, start []byte, withValue bool,
	visitor func(*item) bool) (err error) {
	if start == nil {
		i, err := s.coll(collName).MinItem(false)
		if err != nil {
			return err
		}
		if i == nil {
			return nil
		}
		start = i.Key
	}

	var vErr error
	v := func(x *gkvlite.Item) bool {
		i := &item{key: x.Key}
		if withValue {
			if vErr = i.fromValueBytes(x.Val); vErr != nil {
				return false
			}
		}
		return visitor(i)
	}
	if err := s.coll(collName).VisitItemsAscend(start, withValue, v); err != nil {
		return err
	}
	return vErr
}

// All the following mutation methods need to be called while
// single-threaded with respect to the mutating collection.

func (s *bucketstore) set(items string, changes string,
	newItem *item, oldMeta *item) error {
	collItems := s.coll(items)
	collChanges := s.coll(changes)

	bytes := newItem.toValueBytes()

	if err := collItems.Set(newItem.key, bytes); err != nil {
		return err
	}
	// TODO: What if we flush between the item set and changes set?  Inconsistent db?
	// TODO: should we include the item value in the changes feed?
	// TODO: should we be deleting older changes from the changes feed?
	if err := collChanges.Set(casBytes(newItem.cas), bytes); err != nil {
		return err
	}

	s.dirty()
	return nil
}

func (s *bucketstore) del(items string, changes string,
	key []byte, cas uint64) error {
	collItems := s.coll(items)
	collChanges := s.coll(changes)

	if err := collItems.Delete(key); err != nil {
		return err
	}
	// Empty value represents a deletion tombstone.
	// TODO: What if we flush between the item set and changes set?  Inconsistent db?
	// TODO: should we be deleting older changes from the changes feed?
	// TODO: Currently wrong just deleting last change.
	if err := collChanges.Delete(casBytes(cas)); err != nil {
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
