package cbgb

import (
	"bytes"
	"math/rand"

	"github.com/steveyen/gtreap"
)

type storeVisitor func(*item) bool

// The store abstraction provides a simple, synchronous,
// single-threaded items and changes-feed storage container.  Users
// that need asynchronous behavior should handle it themselves (such
// as by spawning their own goroutines) and by using
// store.snapshot()'s.
type store interface {
	snapshot() store
	getMeta(key []byte) *item
	get(key []byte) *item
	set(newItem *item, oldMeta *item)
	del(key []byte, cas uint64)
	visitItems(key []byte, visitor storeVisitor)
	visitChanges(cas uint64, visitor storeVisitor)
	rangeCopy(minKeyInclusive []byte, maxKeyExclusive []byte) store
}

// The storeMem implementation is in-memory, based on immutable treaps.
type storeMem struct {
	items   *gtreap.Treap
	changes *gtreap.Treap
}

func newStoreMem() store {
	return &storeMem{
		items:   gtreap.NewTreap(KeyLess),
		changes: gtreap.NewTreap(CASLess),
	}
}

// The snapshot() method allows users to get a stable, immutable
// snapshot of a store, which they can safely access while others are
// concurrently accessing and modifying the original store.
func (s *storeMem) snapshot() store {
	return &storeMem{
		items:   s.items,
		changes: s.changes,
	}
}

func (s *storeMem) get(key []byte) *item {
	x := s.items.Get(&item{key: key})
	if x != nil {
		return x.(*item)
	}
	return nil
}

// The getMeta() method is just like get(), except is might return
// item.data of nil.
func (s *storeMem) getMeta(key []byte) *item {
	// This storeMem implementation, though, just uses get().
	return s.get(key)
}

func (s *storeMem) set(newItem *item, oldMeta *item) {
	s.items = s.items.Upsert(newItem, rand.Int())
	s.changes = s.changes.Upsert(newItem, rand.Int())
	if oldMeta != nil {
		// TODO: Should we be de-duplicating oldMeta from the changes feed?
		s.changes.Delete(oldMeta)
	}
}

func (s *storeMem) del(key []byte, cas uint64) {
	t := &item{
		key:  key,
		cas:  cas, // The cas to represent the delete mutation.
		data: nil, // A nil data represents a delete mutation.
	}
	s.items = s.items.Delete(t)
	s.changes = s.changes.Upsert(t, rand.Int())
	// TODO: Should we be deleting older changes from the changes feed?
}

func (s *storeMem) visitItems(key []byte, visitor storeVisitor) {
	s.items.VisitAscend(&item{key: key}, func(x gtreap.Item) bool {
		return visitor(x.(*item))
	})
}

func (s *storeMem) visitChanges(cas uint64, visitor storeVisitor) {
	s.changes.VisitAscend(&item{cas: cas}, func(x gtreap.Item) bool {
		return visitor(x.(*item))
	})
}

func (s *storeMem) rangeCopy(minKeyInclusive []byte, maxKeyExclusive []byte) store {
	return &storeMem{
		items: treapRangeCopy(s.items, gtreap.NewTreap(KeyLess),
			s.items.Min(), // TODO: inefficient.
			minKeyInclusive,
			maxKeyExclusive),
		changes: treapRangeCopy(s.changes, gtreap.NewTreap(CASLess),
			s.changes.Min(), // TODO: inefficient.
			minKeyInclusive,
			maxKeyExclusive),
	}
}

func treapRangeCopy(src *gtreap.Treap, dst *gtreap.Treap, minItem gtreap.Item,
	minKeyInclusive []byte, maxKeyExclusive []byte) *gtreap.Treap {
	visitor := func(x gtreap.Item) bool {
		i := x.(*item)
		if len(minKeyInclusive) > 0 &&
			bytes.Compare(i.key, minKeyInclusive) < 0 {
			return true
		}
		if len(maxKeyExclusive) > 0 &&
			bytes.Compare(i.key, maxKeyExclusive) >= 0 {
			return true
		}
		dst.Upsert(x, rand.Int())
		return true
	}
	src.VisitAscend(minItem, visitor)
	return dst
}
