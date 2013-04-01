//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/steveyen/gkvlite"
)

type partitionstore struct {
	vbid    uint16
	parent  *bucketstore
	lock    sync.Mutex     // Properties below here are covered by this lock.
	keys    unsafe.Pointer // *gkvlite.Collection
	changes unsafe.Pointer // *gkvlite.Collection
}

// Should only be used by readers.
func (p *partitionstore) colls() (keys, changes *gkvlite.Collection) {
	return (*gkvlite.Collection)(atomic.LoadPointer(&p.keys)),
		(*gkvlite.Collection)(atomic.LoadPointer(&p.changes))
}

func (p *partitionstore) mutate(cb func(keys, changes *gkvlite.Collection)) {
	p.lock.Lock()
	defer p.lock.Unlock()

	cb((*gkvlite.Collection)(atomic.LoadPointer(&p.keys)),
		(*gkvlite.Collection)(atomic.LoadPointer(&p.changes)))
}

func (p *partitionstore) collsPauseSwap(cb func() (keys, changes *gkvlite.Collection)) {
	p.lock.Lock()
	defer p.lock.Unlock()

	k, c := cb()

	// Update the changes first, so that readers see a key index that's older.
	atomic.StorePointer(&p.changes, unsafe.Pointer(c))
	atomic.StorePointer(&p.keys, unsafe.Pointer(k))
}

func (p *partitionstore) get(key []byte) (*item, error) {
	return p.getItem(key, true)
}

func (p *partitionstore) getMeta(key []byte) (*item, error) {
	return p.getItem(key, false)
}

func (p *partitionstore) getItem(key []byte, withValue bool) (i *item, err error) {
	for retries := 0; retries < 5; retries++ {
		keys, changes := p.colls()
		iItem, err := keys.GetItem(key, true)
		if err != nil {
			return nil, err
		}
		if iItem == nil {
			return nil, nil
		}
		// TODO: What if a compaction happens in between the lookups,
		// and the changes-feed no longer has the item?  Answer: compaction
		// must not remove items that the key-index references.
		cItem := (*gkvlite.Item)(atomic.LoadPointer(&iItem.Transient))
		if cItem == nil {
			cItem, err = changes.GetItem(iItem.Val, true)
			if err != nil {
				return nil, err
			}
		}
		if cItem != nil {
			i := &item{key: key}
			if err = i.fromValueBytes(cItem.Val); err != nil {
				return nil, err
			}
			return i, nil
		}
		// If cItem is nil, perhaps a concurrent set() happened after
		// the keys.GetItem() and de-duped the old change.  So, retry.
	}
	return nil, fmt.Errorf("max getItem retries for key: %v", key)
}

func (p *partitionstore) getTotals() (numItems uint64, numItemBytes uint64, err error) {
	keys, changes := p.colls()
	numItems, _, err = keys.GetTotals()
	if err != nil {
		return 0, 0, err
	}
	_, numChangesBytes, err := changes.GetTotals()
	if err != nil {
		return 0, 0, err
	}
	return numItems, numChangesBytes, nil
}

func (p *partitionstore) visitItems(start []byte, withValue bool,
	visitor func(*item) bool) (err error) {
	keys, changes := p.colls()
	var vErr error
	v := func(iItem *gkvlite.Item) bool {
		cItem := (*gkvlite.Item)(atomic.LoadPointer(&iItem.Transient))
		if cItem == nil {
			cItem, vErr = changes.GetItem(iItem.Val, withValue)
			if vErr != nil {
				return false
			}
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
	if err := p.visit(keys, start, withValue, v); err != nil {
		return err
	}
	return vErr
}

func (p *partitionstore) visitChanges(start []byte, withValue bool,
	visitor func(*item) bool) (err error) {
	_, changes := p.colls()
	var vErr error
	v := func(cItem *gkvlite.Item) bool {
		i := &item{}
		if vErr = i.fromValueBytes(cItem.Val); vErr != nil {
			return false
		}
		return visitor(i)
	}
	if err := p.visit(changes, start, withValue, v); err != nil {
		return err
	}
	return vErr
}

func (p *partitionstore) visit(coll *gkvlite.Collection,
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

func (p *partitionstore) set(newItem *item, oldItem *item) (
	deltaItemBytes int64, err error) {
	vBytes := newItem.toValueBytes()
	cBytes := casBytes(newItem.cas)

	deltaItemBytes = newItem.NumBytes()
	if oldItem != nil {
		deltaItemBytes -= oldItem.NumBytes()
	}

	p.mutate(func(keys, changes *gkvlite.Collection) {
		cItem := &gkvlite.Item{Key: cBytes, Val: vBytes, Priority: int32(rand.Int())}
		if err = changes.SetItem(cItem); err != nil {
			return
		}

		dirtyForce := false

		if newItem.key != nil && len(newItem.key) > 0 {
			// TODO: What if we flush between the keys update and changes
			// update?  That could result in an inconsistent db file?
			// Solution idea #1 is to have load-time fixup, that
			// incorporates changes into the key-index.
			kItem := &gkvlite.Item{
				Key:       newItem.key,
				Val:       cBytes,
				Priority:  int32(rand.Int()),
				Transient: unsafe.Pointer(cItem),
			}
			if err = keys.SetItem(kItem); err != nil {
				return
			}
		} else {
			dirtyForce = true // An nil/empty key means this is a metadata change.
		}

		if oldItem != nil {
			// TODO: Need a "frozen" CAS point where we don't de-duplicate changes stream.
			changes.Delete(casBytes(oldItem.cas))
		}

		p.parent.dirty(dirtyForce)
	})
	return deltaItemBytes, err
}

func (p *partitionstore) del(key []byte, cas uint64, oldItem *item) (
	deltaItemBytes int64, err error) {
	cBytes := casBytes(cas)
	dItem := &item{key: key, cas: cas}
	vBytes := dItem.markAsDeletion().toValueBytes()

	deltaItemBytes = dItem.NumBytes()
	if oldItem != nil {
		deltaItemBytes -= oldItem.NumBytes()
	}

	p.mutate(func(keys, changes *gkvlite.Collection) {
		if err = changes.Set(cBytes, vBytes); err != nil {
			return
		}

		dirtyForce := false
		if key != nil && len(key) > 0 {
			// TODO: What if we flush between the keys update and changes
			// update?  That could result in an inconsistent db file?
			// Solution idea #1 is to have load-time fixup, that
			// incorporates changes into the key-index.
			if _, err = keys.Delete(key); err != nil {
				return
			}
		} else {
			dirtyForce = true // An nil/empty key means this is a metadata change.
		}

		if oldItem != nil {
			// TODO: Need a "frozen" CAS point where we don't de-duplicate changes stream.
			changes.Delete(casBytes(oldItem.cas))
		}

		p.parent.dirty(dirtyForce)
	})
	return deltaItemBytes, err
}

// ------------------------------------------------------------

func rangeCopy(srcColl *gkvlite.Collection, dstColl *gkvlite.Collection,
	minKeyInclusive []byte, maxKeyExclusive []byte) (hasItems bool, err error) {
	minItem, err := srcColl.MinItem(false)
	if err != nil {
		return false, err
	}
	// TODO: What if we flush between the keys update and changes
	// update?  That could result in an inconsistent db file?
	// Solution idea #1 is to have load-time fixup, that
	// incorporates changes into the key-index.
	if minItem != nil {
		if err := collRangeCopy(srcColl, dstColl, minItem.Key,
			minKeyInclusive, maxKeyExclusive); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
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
