package cbgb

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/steveyen/gkvlite"
)

// The storeMem implementation is in-memory, based on immutable treaps.
type storePersist struct {
	s       *gkvlite.Store
	items   *gkvlite.Collection
	changes *gkvlite.Collection
}

func newStorePersist(path string) (res store, err error) {
	if f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666); err == nil {
		return newStorePersistFromFile(f)
	}
	return nil, err
}

func newStorePersistFromFile(f *os.File) (res store, err error) {
	if s, err := gkvlite.NewStore(f); err == nil {
		res := &storePersist{s: s}
		res.items = s.SetCollection("i", nil)
		res.changes = s.SetCollection("c", nil)
	}
	return nil, err
}

func (s *storePersist) snapshot() store {
	panic("TODO: storePersist.snapshot()")
	return nil
}

func (s *storePersist) get(key []byte) *item {
	return s.getItem(key, true)
}

func (s *storePersist) getMeta(key []byte) *item {
	return s.getItem(key, false)
}

func (s *storePersist) getItem(key []byte, withValue bool) *item {
	if v, err := s.items.GetItem(key, withValue); err == nil && v != nil {
		i := &item{key: key}
		if err := i.fromValueBytes(v.Val); err == nil {
			return i
		}
	}
	// TODO: error propagation.
	return nil
}

func (s *storePersist) set(newItem *item, oldMeta *item) {
	if err := s.items.Set(newItem.key, newItem.toValueBytes()); err == nil {
		if err := s.changes.Set(casBytes(newItem.cas), newItem.key); err == nil {
			if oldMeta != nil {
				s.changes.Delete(casBytes(oldMeta.cas))
			}
			s.s.Flush() // TODO: flush less often.
		}
	}
	// TODO: error propagation.
}

func (s *storePersist) del(key []byte, cas uint64) {
	if err := s.items.Delete(key); err == nil {
		// Empty value represents a deletion.
		if err := s.changes.Set(casBytes(cas), []byte{}); err == nil {
			s.s.Flush() // TODO: flush less often.
		}
	}
	// TODO: should we be deleting older changes from the changes feed?
	// TODO: error propagation.
}

func (s *storePersist) visitItems(key []byte, visitor storeVisitor) {
	s.items.VisitItemsAscend(key, true, func(x *gkvlite.Item) bool {
		i := &item{key: x.Key}
		if err := i.fromValueBytes(x.Val); err == nil {
			return visitor(i)
		}
		return false
	})
	// TODO: error propagation.
}

func (s *storePersist) visitChanges(cas uint64, visitor storeVisitor) {
	s.changes.VisitItemsAscend(casBytes(cas), true, func(x *gkvlite.Item) bool {
		i := &item{key: x.Key}
		if err := i.fromValueBytes(x.Val); err == nil {
			return visitor(i)
		}
		return false
	})
	// TODO: error propagation.
}

func (s *storePersist) rangeCopy(minKeyInclusive []byte, maxKeyExclusive []byte) store {
	// TODO: error handling.
	res, _ := newStorePersist("TODO.tmp")
	dst := res.(*storePersist)
	minItem, _ := dst.items.MinItem(false)
	minChange, _ := dst.items.MaxItem(false)

	collRangeCopy(s.items, dst.items, minItem.Key,
		minKeyInclusive, maxKeyExclusive)
	collRangeCopy(s.changes, dst.changes, minChange.Key,
		minKeyInclusive, maxKeyExclusive)

	return res
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

func casBytes(cas uint64) []byte {
	buf := bytes.NewBuffer(make([]byte, 8)[:0])
	binary.Write(buf, binary.BigEndian, cas)
	return buf.Bytes()
}
