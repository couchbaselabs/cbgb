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
	snapshot() (store, error)
	getMeta(key []byte) (*item, error)
	get(key []byte) (*item, error)
	set(newItem *item, oldMeta *item) error
	del(key []byte, cas uint64) error
	visitItems(key []byte, visitor storeVisitor) error
	visitChanges(cas uint64, visitor storeVisitor) error
	rangeCopy(minKeyInclusive []byte, maxKeyExclusive []byte, path string) (store, error)
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
func (s *storeMem) snapshot() (store, error) {
	return &storeMem{
		items:   s.items,
		changes: s.changes,
	}, nil
}

func (s *storeMem) get(key []byte) (*item, error) {
	x := s.items.Get(&item{key: key})
	if x != nil {
		return x.(*item), nil
	}
	return nil, nil
}

// The getMeta() method is just like get(), except is might return
// item.data of nil.
func (s *storeMem) getMeta(key []byte) (*item, error) {
	// This storeMem implementation, though, just uses get().
	return s.get(key)
}

func (s *storeMem) set(newItem *item, oldMeta *item) error {
	s.items = s.items.Upsert(newItem, rand.Int())
	s.changes = s.changes.Upsert(newItem, rand.Int())
	if oldMeta != nil {
		// TODO: Should we be de-duplicating oldMeta from the changes feed?
		s.changes.Delete(oldMeta)
	}
	return nil
}

func (s *storeMem) del(key []byte, cas uint64) error {
	t := &item{
		key:  key,
		cas:  cas, // The cas to represent the delete mutation.
		data: nil, // A nil data represents a delete mutation.
	}
	s.items = s.items.Delete(t)
	s.changes = s.changes.Upsert(t, rand.Int())
	// TODO: Should we be deleting older changes from the changes feed?
	return nil
}

func (s *storeMem) visitItems(key []byte, visitor storeVisitor) error {
	s.items.VisitAscend(&item{key: key}, func(x gtreap.Item) bool {
		return visitor(x.(*item))
	})
	return nil
}

func (s *storeMem) visitChanges(cas uint64, visitor storeVisitor) error {
	s.changes.VisitAscend(&item{cas: cas}, func(x gtreap.Item) bool {
		return visitor(x.(*item))
	})
	return nil
}

func (s *storeMem) rangeCopy(minKeyInclusive []byte, maxKeyExclusive []byte,
	path string) (store, error) {
	return &storeMem{
		items: treapRangeCopy(s.items, gtreap.NewTreap(KeyLess),
			s.items.Min(), // TODO: inefficient.
			minKeyInclusive,
			maxKeyExclusive),
		changes: treapRangeCopy(s.changes, gtreap.NewTreap(CASLess),
			s.changes.Min(), // TODO: inefficient.
			minKeyInclusive,
			maxKeyExclusive),
	}, nil
}

func treapRangeCopy(src *gtreap.Treap, dst *gtreap.Treap, minItem gtreap.Item,
	minKeyInclusive []byte, maxKeyExclusive []byte) *gtreap.Treap {
	// TODO: should instead use the treap's split capability, which
	// would be O(log N) instead of an O(N) visit, and should also use
	// its snapshot feature so that this can be done asynchronously
	// outside of the vbucket's service() loop.
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
