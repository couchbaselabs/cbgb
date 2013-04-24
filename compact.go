package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/steveyen/gkvlite"
)

func (s *bucketstore) Compact() error {
	s.diskLock.Lock()
	defer s.diskLock.Unlock()

	bsf := s.BSF()
	if bsf.file == nil { // We're in memory-only mode.
		return nil
	}

	compactPath := bsf.path + ".compact"
	if err := s.compactGo(bsf, compactPath); err != nil {
		atomic.AddInt64(&s.stats.CompactErrors, 1)
		return err
	}

	atomic.AddInt64(&s.stats.Compacts, 1)
	return nil
}

func (s *bucketstore) compactGo(bsf *bucketstorefile, compactPath string) error {
	bsf.removeOldFiles()   // Clean up previous, successful compactions.
	os.Remove(compactPath) // Clean up previous, aborted compaction attempts.

	compactFile, err := fileService.OpenFile(compactPath, os.O_RDWR|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return err
	}
	defer func() {
		if compactFile != nil {
			compactFile.Close()
			os.Remove(compactPath)
		}
	}()

	sc := gkvlite.StoreCallbacks{KeyCompareForCollection: s.keyCompareForCollection}

	compactStore, err := gkvlite.NewStoreEx(compactFile, sc)
	if err != nil {
		return err
	}

	// TODO: Parametrize writeEvery.
	writeEvery := 1000

	lastChanges := make(map[uint16]*gkvlite.Item) // Last items in changes colls.
	collNames := bsf.store.GetCollectionNames()   // Names of collections to process.
	collRest := make([]string, 0, len(collNames)) // Names of unprocessed collections.
	vbids := make([]uint16, 0, len(collNames))    // VBucket id's that we processed.

	// Process compaction in a few steps: first, unlocked,
	// snapshot-based collection copying meant to handle most of each
	// vbucket's data; and then, locked copying of any vbucket
	// mutations (deltas) that happened in the meantime.  Then, while
	// still holding all vbucket collection locks, we copy any
	// remaining non-vbucket collections and atomically swap the files.
	for _, collName := range collNames {
		if !strings.HasSuffix(collName, COLL_SUFFIX_CHANGES) {
			if !strings.HasSuffix(collName, COLL_SUFFIX_KEYS) {
				// It's neither a changes nor a keys collection.
				collRest = append(collRest, collName)
			}
			continue
		}
		vbid, lastChange, err :=
			s.copyVBucketColls(bsf, collName, compactStore, writeEvery)
		if err != nil {
			return err
		}
		lastChanges[uint16(vbid)] = lastChange
		vbids = append(vbids, uint16(vbid))
	}

	return s.copyBucketStoreDeltas(bsf, compactStore,
		vbids, 0, lastChanges, writeEvery, func() (err error) {
			// Copy any remaining (simple) collections (like COLL_VBMETA).
			err = s.copyRemainingColls(bsf, collRest, compactStore, writeEvery)
			if err != nil {
				return err
			}
			err = compactStore.Flush()
			if err != nil {
				return err
			}
			compactFile.Close()

			return s.compactSwapFile(bsf, compactPath) // The last step.
		})
}

func (s *bucketstore) compactSwapFile(bsf *bucketstorefile, compactPath string) error {
	fname := filepath.Base(bsf.path)
	suffix, err := parseFileNameSuffix(fname)
	if err != nil {
		return err
	}
	prefix, ver, err := parseStoreFileName(fname, suffix)
	if err != nil {
		return err
	}

	nextName := makeStoreFileName(prefix, ver+1, suffix)
	nextPath := filepath.Join(filepath.Dir(bsf.path), nextName)

	if err = os.Rename(compactPath, nextPath); err != nil {
		return err
	}

	nextFile, err := fileService.OpenFile(nextPath, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return err
	}

	sc := gkvlite.StoreCallbacks{KeyCompareForCollection: s.keyCompareForCollection}

	nextBSF := NewBucketStoreFile(nextPath, nextFile, bsf.stats)
	nextStore, err := gkvlite.NewStoreEx(nextBSF, sc)
	if err != nil {
		// TODO: Rollback the previous *.orig rename.
		return err
	}
	nextBSF.store = nextStore

	atomic.StorePointer(&s.bsf, unsafe.Pointer(nextBSF))

	bsf.apply(func() {
		bsf.purge = true // Mark the old file as purgable.
	})

	return nil
}

func copyColl(srcColl *gkvlite.Collection, dstColl *gkvlite.Collection,
	writeEvery int) (numItems uint64, lastItem *gkvlite.Item, err error) {
	minItem, err := srcColl.MinItem(true)
	if err != nil {
		return 0, nil, err
	}
	if minItem == nil {
		return 0, nil, nil
	}

	var errVisit error = nil
	err = srcColl.VisitItemsAscend(minItem.Key, true, func(i *gkvlite.Item) bool {
		if errVisit = dstColl.SetItem(i); errVisit != nil {
			return false
		}
		numItems++
		lastItem = i
		if writeEvery > 0 && numItems%uint64(writeEvery) == 0 {
			if errVisit = dstColl.Write(); errVisit != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return 0, nil, err
	}
	if errVisit != nil {
		return 0, nil, errVisit
	}

	return numItems, lastItem, nil
}

func copyDelta(lastChangeCAS []byte, cName string, kName string,
	srcStore *gkvlite.Store, dstStore *gkvlite.Store,
	writeEvery int) (numVisits uint64, err error) {
	cSrc := srcStore.GetCollection(cName)
	cDst := dstStore.GetCollection(cName)
	kDst := dstStore.GetCollection(kName)
	if cSrc == nil || cDst == nil || kDst == nil {
		return 0, fmt.Errorf("compact copyDelta missing colls: %v, %v",
			cName, kName)
	}

	var errVisit error

	err = cSrc.VisitItemsAscend(lastChangeCAS, true, func(cItem *gkvlite.Item) bool {
		numVisits++
		if numVisits <= 1 {
			return true
		}
		if errVisit = cDst.SetItem(cItem); errVisit != nil {
			return false
		}
		// Update the keys index with the latest change.
		i := &item{}
		if errVisit = i.fromValueBytes(cItem.Val); errVisit != nil {
			return false
		}
		if i.key == nil || len(i.key) <= 0 {
			return true // A nil/empty key means a metadata change.
		}
		if i.isDeletion() {
			if _, errVisit = kDst.Delete(i.key); errVisit != nil {
				return false
			}
		} else {
			if errVisit = kDst.Set(i.key, cItem.Key); errVisit != nil {
				return false
			}
		}
		// Persist to storage as needed.
		if writeEvery > 0 && numVisits%uint64(writeEvery) == 0 {
			if errVisit = cDst.Write(); errVisit != nil {
				return false
			}
			if errVisit = kDst.Write(); errVisit != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if errVisit != nil {
		return 0, errVisit
	}

	return numVisits, nil
}

func (s *bucketstore) copyVBucketColls(bsf *bucketstorefile,
	collName string, compactStore *gkvlite.Store, writeEvery int) (
	uint16, *gkvlite.Item, error) {
	vbidStr := collName[0 : len(collName)-len(COLL_SUFFIX_CHANGES)]
	vbid, err := strconv.Atoi(vbidStr)
	if err != nil {
		return 0, nil, err
	}
	if vbid < 0 || vbid > MAX_VBID {
		return 0, nil, fmt.Errorf("compact vbid out of range: %v, vbid: %v",
			bsf.path, vbid)
	}
	cName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_CHANGES)
	kName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_KEYS)
	cDest := compactStore.SetCollection(cName, nil)
	kDest := compactStore.SetCollection(kName, nil)
	if cDest == nil || kDest == nil {
		return 0, nil, fmt.Errorf("compact could not create colls for vbid: %v",
			vbid)
	}
	cCurr := s.coll(cName) // The c prefix in cFooBar means 'changes'.
	kCurr := s.coll(kName) // The k prefix in kFooBar means 'keys'.
	if cCurr == nil || kCurr == nil {
		return 0, nil, fmt.Errorf("compact source colls missing: %v, vbid: %v",
			bsf.path, vbid)
	}
	// Get a consistent snapshot (keys reflect all changes) of the
	// keys & changes collections.
	ps := s.partitions[uint16(vbid)]
	if ps == nil {
		return 0, nil, fmt.Errorf("compact missing partition for vbid: %v", vbid)
	}
	var currSnapshot *gkvlite.Store
	ps.mutate(func(key, changes *gkvlite.Collection) {
		currSnapshot = bsf.store.Snapshot()
	})
	if currSnapshot == nil {
		return 0, nil, fmt.Errorf("compact source snapshot failed: %v, vbid: %v",
			bsf.path, vbid)
	}
	cCurrSnapshot := currSnapshot.GetCollection(cName)
	kCurrSnapshot := currSnapshot.GetCollection(kName)
	if cCurrSnapshot == nil || kCurrSnapshot == nil {
		return 0, nil, fmt.Errorf("compact missing colls from snapshot: %v, vbid: %v",
			bsf.path, vbid)
	}
	// TODO: Record stats on # changes processed.
	_, lastChange, err := copyColl(cCurrSnapshot, cDest, writeEvery)
	if err != nil {
		return 0, nil, err
	}
	// TODO: Record stats on # keys processed.
	_, _, err = copyColl(kCurrSnapshot, kDest, writeEvery)
	if err != nil {
		return 0, nil, err
	}
	return uint16(vbid), lastChange, err
}

func (s *bucketstore) copyRemainingColls(bsf *bucketstorefile,
	collRest []string, compactStore *gkvlite.Store, writeEvery int) error {
	currSnapshot := bsf.store.Snapshot()
	if currSnapshot == nil {
		return fmt.Errorf("compact source snapshot failed: %v", bsf.path)
	}
	for _, collName := range collRest {
		collCurr := currSnapshot.GetCollection(collName)
		if collCurr == nil {
			return fmt.Errorf("compact rest coll missing: %v, collName: %v",
				bsf.path, collName)
		}
		collNext := compactStore.SetCollection(collName, nil)
		if collCurr == nil {
			return fmt.Errorf("compact rest dest missing: %v, collName: %v",
				bsf.path, collName)
		}
		_, _, err := copyColl(collCurr, collNext, writeEvery)
		if err != nil {
			return err
		}
	}
	return nil
}

// Copy any mutations that concurrently just came in.  We use
// recursion to naturally have a phase of pausing & copying,
// and then unpausing as the recursion unwinds.

func (s *bucketstore) copyBucketStoreDeltas(bsf *bucketstorefile,
	compactStore *gkvlite.Store, vbids []uint16, vbidIdx int,
	lastChanges map[uint16]*gkvlite.Item, writeEvery int,
	done func() error) (err error) {
	if vbidIdx >= len(vbids) {
		return done() // Callback while we have all the locks.
	}

	vbid := vbids[vbidIdx]
	cName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_CHANGES)
	kName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_KEYS)
	ps := s.partitions[vbid]
	if ps == nil {
		return fmt.Errorf("compact missing parititon for vbid: %v", vbid)
	}
	ps.collsPauseSwap(func() (*gkvlite.Collection, *gkvlite.Collection) {
		_, err = copyDelta(lastChanges[vbid].Key, cName, kName,
			bsf.store.Snapshot(), compactStore, writeEvery)
		if err != nil {
			return s.coll(kName), s.coll(cName)
		}
		err = s.copyBucketStoreDeltas(bsf, compactStore,
			vbids, vbidIdx+1, lastChanges, writeEvery, done)
		if err != nil {
			return s.coll(kName), s.coll(cName)
		}
		// If it all succeeded, then we will provide new
		// collections to be used as we unwind / unpause.
		return s.coll(kName), s.coll(cName)
	})
	return err
}
