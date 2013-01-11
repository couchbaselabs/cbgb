package cbgb

import (
	"bytes"
	"math/rand"

	"github.com/steveyen/gtreap"
)

type storeVisitor func(*item) bool

// The memstore implementation is in-memory only, single-threaded,
// based on immutable treaps.
type memstore struct {
	items   *gtreap.Treap
	changes *gtreap.Treap
}

func newMemStore() *memstore {
	return &memstore{
		items:   gtreap.NewTreap(KeyLess),
		changes: gtreap.NewTreap(CASLess),
	}
}

// The snapshot() method allows users to get a stable, immutable
// snapshot of a store, which they can safely access while others are
// concurrently accessing and modifying the original store.
func (s *memstore) snapshot() (*memstore, error) {
	return &memstore{
		items:   s.items,
		changes: s.changes,
	}, nil
}

func (s *memstore) get(key []byte) (*item, error) {
	x := s.items.Get(&item{key: key})
	if x != nil {
		return x.(*item), nil
	}
	return nil, nil
}

// The getMeta() method is just like get(), except it might return
// item.data of nil.
func (s *memstore) getMeta(key []byte) (*item, error) {
	return s.get(key)
}

func (s *memstore) set(newItem *item, oldMeta *item) error {
	s.items = s.items.Upsert(newItem, rand.Int())
	s.changes = s.changes.Upsert(newItem, rand.Int())
	if oldMeta != nil {
		// TODO: Should we be de-duplicating oldMeta from the changes feed?
		s.changes.Delete(oldMeta)
	}
	return nil
}

func (s *memstore) del(key []byte, cas uint64) error {
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

func (s *memstore) visitItems(key []byte, visitor storeVisitor) error {
	s.items.VisitAscend(&item{key: key}, func(x gtreap.Item) bool {
		return visitor(x.(*item))
	})
	return nil
}

func (s *memstore) visitChanges(cas uint64, visitor storeVisitor) error {
	s.changes.VisitAscend(&item{cas: cas}, func(x gtreap.Item) bool {
		return visitor(x.(*item))
	})
	return nil
}

func (s *memstore) rangeCopy(minKeyInclusive []byte, maxKeyExclusive []byte,
	path string) (*memstore, error) {
	return &memstore{
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
