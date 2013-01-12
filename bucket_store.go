package cbgb

import (
	"bytes"
	"os"

	"github.com/steveyen/gkvlite"
)

type bucketstorereq struct {
	cb  func(*bucketstore)
	res chan bool
}

type bucketstore struct {
	path  string
	file  *os.File // May be nil for in-memory only (e.g., for unit tests).
	store *gkvlite.Store
	ch    chan *bucketstorereq
}

func newBucketStore(path string) (*bucketstore, error) {
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
		file:  file,
		store: store,
		ch:    make(chan *bucketstorereq),
	}
	go res.service()
	return res, nil
}

func (s *bucketstore) service() {
	for r := range s.ch {
		r.cb(s)
		if r.res != nil {
			close(r.res)
		}
	}
	if s.file != nil {
		s.file.Close()
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

// All the following methods need to be called while single-threaded,
// so invoke them only in your apply() callback functions.

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

func (s *bucketstore) flush() error {
	return s.store.Flush()
}

func (s *bucketstore) get(items string, key []byte) (*item, error) {
	return s.getItem(items, key, true)
}

func (s *bucketstore) getMeta(items string, key []byte) (*item, error) {
	return s.getItem(items, key, false)
}

func (s *bucketstore) getItem(items string, key []byte, withValue bool) (i *item, err error) {
	if v, err := s.coll(items).GetItem(key, withValue); err == nil && v != nil {
		i := &item{key: key}
		if err = i.fromValueBytes(v.Val); err == nil {
			return i, nil
		}
	}
	return nil, err
}

func (s *bucketstore) set(items string, changes string,
	newItem *item, oldMeta *item) error {
	collItems := s.coll(items)
	collChanges := s.coll(changes)

	if err := collItems.Set(newItem.key, newItem.toValueBytes()); err != nil {
		return err
	}
	// TODO: should we include the item value in the changes feed?
	if err := collChanges.Set(casBytes(newItem.cas), newItem.key); err != nil {
		return err
	}
	// TODO: should we be deleting older changes from the changes feed?
	return s.store.Flush() // TODO: flush less often.
}

func (s *bucketstore) del(items string, changes string,
	key []byte, cas uint64) error {
	collItems := s.coll(items)
	collChanges := s.coll(changes)

	if err := collItems.Delete(key); err != nil {
		return err
	}
	// Empty value represents a deletion tombstone.
	if err := collChanges.Set(casBytes(cas), []byte{}); err != nil {
		return err
	}
	// TODO: should we be deleting older changes from the changes feed?
	return s.store.Flush() // TODO: flush less often.
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
