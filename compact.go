package cbgb

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/steveyen/gkvlite"
)

func (s *bucketstore) compact() error {
	// This should be invoked via bucketstore.service(), so there's no
	// concurrent flushing.
	bsf := s.BSF()

	bsf.apply(func() {
		bsf.insomnia = true // Turn off concurrent sleeping.
	})
	defer func() {
		bsf.apply(func() {
			bsf.insomnia = false
		})
	}()

	compactPath := bsf.path + ".compact"

	if err := s.compactGo(bsf, compactPath); err != nil {
		atomic.AddUint64(&s.stats.CompactErrors, 1)
		return err
	}
	atomic.AddUint64(&s.stats.TotCompact, 1)
	return nil
}

func (s *bucketstore) compactGo(bsf *bucketstorefile, compactPath string) error {
	os.Remove(compactPath) // Ignore any previous attempts.

	compactFile, err := os.OpenFile(compactPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if compactFile != nil {
			compactFile.Close()
			os.Remove(compactPath)
		}
	}()
	compactStore, err := gkvlite.NewStore(compactFile)
	if err != nil {
		return err
	}

	// TODO: Parametrize writeEvery.
	writeEvery := 1000

	lastChanges := make(map[uint16]*gkvlite.Item) // Last items in changes colls.
	collNames := bsf.store.GetCollectionNames()   // Names of collections to process.
	collRest := make([]string, 0, len(collNames)) // Names of unprocessed collections.
	vbids := make([]uint16, 0, len(collNames))    // VBucket id's that we processed.

	// Process vbucket changes/keys collections.
	for _, collName := range collNames {
		if !strings.HasSuffix(collName, COLL_SUFFIX_CHANGES) {
			if !strings.HasSuffix(collName, COLL_SUFFIX_KEYS) {
				// It's neither a changes nor a keys collection.
				collRest = append(collRest, collName)
			}
			continue
		}
		vbidStr := collName[0 : len(collName)-len(COLL_SUFFIX_CHANGES)]
		vbid, err := strconv.Atoi(vbidStr)
		if err != nil {
			return err
		}
		if vbid < 0 || vbid > 0x0000ffff {
			return fmt.Errorf("compact vbid out of range: %v, vbid: %v",
				bsf.path, vbid)
		}
		cName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_CHANGES)
		kName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_KEYS)
		cDest := compactStore.SetCollection(cName, nil)
		kDest := compactStore.SetCollection(kName, nil)
		if cDest == nil || kDest == nil {
			return fmt.Errorf("compact could not create colls: %v, vbid: %v",
				compactPath, vbid)
		}
		cCurr := s.coll(cName) // The c prefix in cFooBar means 'changes'.
		kCurr := s.coll(kName) // The k prefix in kFooBar means 'keys'.
		if cCurr == nil || kCurr == nil {
			return fmt.Errorf("compact source colls missing: %v, vbid: %v",
				bsf.path, vbid)
		}
		// Using mapPauseSwapColl to get a consistent snapshot (keys
		// reflect all changes) of the keys & changes collections.
		pauseSwapColls := s.mapPauseSwapColls[uint16(vbid)]
		if pauseSwapColls == nil {
			return fmt.Errorf("compact missing pauseSwapColls, vbid: %v", vbid)
		}
		var currSnapshot *gkvlite.Store
		pauseSwapColls(func() (*gkvlite.Collection, *gkvlite.Collection) {
			currSnapshot = bsf.store.Snapshot()
			return kCurr, cCurr
		})
		if currSnapshot == nil {
			return fmt.Errorf("compact source snapshot failed: %v, vbid: %v",
				bsf.path, vbid)
		}
		cCurrSnapshot := currSnapshot.GetCollection(cName)
		kCurrSnapshot := currSnapshot.GetCollection(kName)
		if cCurrSnapshot == nil || kCurrSnapshot == nil {
			return fmt.Errorf("compact missing colls from snapshot: %v, vbid: %v",
				bsf.path, vbid)
		}
		// TODO: Record stats on # changes processed.
		_, lastChange, err := copyColl(cCurrSnapshot, cDest, writeEvery)
		if err != nil {
			return err
		}
		// TODO: Record stats on # keys processed.
		_, _, err = copyColl(kCurrSnapshot, kDest, writeEvery)
		if err != nil {
			return err
		}
		lastChanges[uint16(vbid)] = lastChange
		vbids = append(vbids, uint16(vbid))
	}

	// Copy any remaining (simple) collections (like COLL_VBMETA).
	// And, swap the compacted file into use.
	copyRemainingColls := func() error {
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
			_, _, err = copyColl(collCurr, collNext, writeEvery)
			if err != nil {
				return err
			}
		}
		if err = compactStore.Flush(); err != nil {
			return err
		}
		compactFile.Close()
		compactFile = nil
		return s.compactSwapFile(bsf, compactPath)
	}

	// Copy any mutations that concurrently just came in.  We use
	// recursion to naturally have a phase of pausing & copying,
	// and then unpausing as the recursion unwinds.
	var copyDeltas func(vbidIdx int) (err error)
	copyDeltas = func(vbidIdx int) (err error) {
		if vbidIdx >= len(vbids) {
			return copyRemainingColls()
		}
		vbid := vbids[vbidIdx]
		cName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_CHANGES)
		kName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_KEYS)
		pauseSwapColls := s.mapPauseSwapColls[vbid]
		if pauseSwapColls == nil {
			return fmt.Errorf("compact missing pauseSwapColls, vbid: %v", vbid)
		}
		pauseSwapColls(func() (*gkvlite.Collection, *gkvlite.Collection) {
			_, err = copyDelta(lastChanges[vbid].Key, cName, kName,
				bsf.store.Snapshot(), compactStore, writeEvery)
			if err != nil {
				return s.coll(kName), s.coll(cName)
			}
			err = copyDeltas(vbidIdx + 1)
			if err != nil {
				return s.coll(kName), s.coll(cName)
			}
			// If it all succeeded, then we will provide new
			// collections to be used as we unwind / unpause.
			return s.coll(kName), s.coll(cName)
		})
		return err
	}

	return copyDeltas(0)
}

func (s *bucketstore) compactSwapFile(bsf *bucketstorefile, compactPath string) error {
	idx, ver, err := parseStoreFileName(filepath.Base(bsf.path))
	if err != nil {
		return err
	}

	nextName := makeStoreFileName(idx, ver+1)
	nextPath := path.Join(filepath.Dir(bsf.path), nextName)

	if err = os.Rename(compactPath, nextPath); err != nil {
		return err
	}

	nextFile, err := os.OpenFile(nextPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	nextBSF := &bucketstorefile{
		path:          nextPath,
		file:          nextFile,
		ch:            make(chan *funreq),
		sleepInterval: bsf.sleepInterval,
		stats:         bsf.stats,
	}
	go nextBSF.service()

	nextStore, err := gkvlite.NewStore(nextBSF)
	if err != nil {
		nextBSF.Close()
		// TODO: Rollback the previous *.orig rename.
		return err
	}
	nextBSF.store = nextStore

	atomic.StorePointer(&s.bsf, unsafe.Pointer(nextBSF))

	// TODO: Shut down old bsf.

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
	writeEvery int) (numItems uint64, err error) {
	cSrc := srcStore.GetCollection(cName)
	cDst := dstStore.GetCollection(cName)
	kDst := dstStore.GetCollection(kName)
	if cSrc == nil || cDst == nil || kDst == nil {
		return 0, fmt.Errorf("compact copyDelta missing colls: %v, %v",
			cName, kName)
	}

	var errVisit error = nil
	err = cSrc.VisitItemsAscend(lastChangeCAS, true, func(cItem *gkvlite.Item) bool {
		if numItems <= 0 {
			return true
		}
		numItems++
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
			if errVisit = kDst.Delete(i.key); errVisit != nil {
				return false
			}
		} else {
			if errVisit = kDst.Set(i.key, cItem.Key); errVisit != nil {
				return false
			}
		}
		// Persist to storage as needed.
		if writeEvery > 0 && numItems%uint64(writeEvery) == 0 {
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

	return numItems, nil
}
