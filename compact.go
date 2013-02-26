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

	// Turn off concurrent sleeping.
	bsf.apply(func() { bsf.insomnia = true })
	defer bsf.apply(func() { bsf.insomnia = false })

	compactPath := bsf.path + ".compact"
	if err := s.compactGo(bsf, compactPath); err != nil {
		atomic.AddUint64(&s.stats.CompactErrors, 1)
		return err
	}

	atomic.AddUint64(&s.stats.Compacts, 1)
	return nil
}

func (s *bucketstore) compactGo(bsf *bucketstorefile, compactPath string) error {
	// TODO: Should cleanup all old, previous attempts to rescue disk space.
	os.Remove(compactPath) // Clean up any previous attempts.

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
		vbid, lastChange, err :=
			s.copyVBucketColls(bsf, collName, compactStore, writeEvery)
		if err != nil {
			return err
		}
		lastChanges[uint16(vbid)] = lastChange
		vbids = append(vbids, uint16(vbid))
	}

	return s.copyDeltas(bsf, collRest, compactPath, compactFile, compactStore,
		vbids, 0, lastChanges, writeEvery)
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

	// Shut down old bsf by setting its sleepPurge.
	bsf.apply(func() {
		bsf.sleepInterval = s.purgeTimeout
		bsf.sleepPurge = s.purgeTimeout
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
			if _, errVisit = kDst.Delete(i.key); errVisit != nil {
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
func (s *bucketstore) copyDeltas(bsf *bucketstorefile, collRest []string,
	compactPath string, compactFile *os.File, compactStore *gkvlite.Store,
	vbids []uint16, vbidIdx int, lastChanges map[uint16]*gkvlite.Item,
	writeEvery int) (err error) {
	// Workaround for go compiler/runtime limitation of "closure needs too
	// many variables; runtime will reject it"
	p := struct {
		compactPath  string
		compactFile  *os.File
		compactStore *gkvlite.Store
	}{compactPath, compactFile, compactStore}

	if vbidIdx >= len(vbids) {
		// Copy any remaining (simple) collections (lifke COLL_VBMETA).
		if err = s.copyRemainingColls(bsf, collRest, compactStore, writeEvery); err != nil {
			return err
		}
		if err = compactStore.Flush(); err != nil {
			return err
		}

		// Before we unwind the recursion (which has everything
		// paused), swap the compacted file into use.
		compactFile.Close()
		compactFile = nil
		return s.compactSwapFile(bsf, compactPath)
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
			bsf.store.Snapshot(), p.compactStore, writeEvery)
		if err != nil {
			return s.coll(kName), s.coll(cName)
		}
		err = s.copyDeltas(bsf, collRest, p.compactPath, p.compactFile, p.compactStore,
			vbids, vbidIdx+1, lastChanges, writeEvery)
		if err != nil {
			return s.coll(kName), s.coll(cName)
		}
		// If it all succeeded, then we will provide new
		// collections to be used as we unwind / unpause.
		return s.coll(kName), s.coll(cName)
	})
	return err
}
