package cbgb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"

	"github.com/dustin/gomemcached"
)

const (
	CHANGES_SINCE       = gomemcached.CommandCode(0x60)
	GET_VBMETA          = gomemcached.CommandCode(0x61)
	SET_VBMETA          = gomemcached.CommandCode(0x62)
	SPLIT_RANGE         = gomemcached.CommandCode(0x63)
	NOT_MY_RANGE        = gomemcached.Status(0x60)
	COLL_SUFFIX_ITEMS   = ".i"
	COLL_SUFFIX_CHANGES = ".c"
	COLL_VBMETA         = "vbm"
)

type vbreq struct {
	w     io.Writer
	req   *gomemcached.MCRequest
	resch chan *gomemcached.MCResponse
}

type vbapplyreq struct {
	cb  func(*vbucket)
	res chan bool
}

type vbucket struct {
	parent      bucket
	meta        VBMeta
	mem         *memstore
	bs          *bucketstore
	ch          chan vbreq
	ach         chan vbapplyreq
	stats       Stats
	observer    *broadcaster
	collItems   string // Name of persistent items collection.
	collChanges string // Name of persistent changes collection.
}

// Message sent on object change
type mutation struct {
	vb      uint16
	key     []byte
	cas     uint64
	deleted bool
}

func (m mutation) String() string {
	sym := "M"
	if m.deleted {
		sym = "D"
	}
	return fmt.Sprintf("%v: vb:%v %s -> %v", sym, m.vb, m.key, m.cas)
}

// Sentinel value indicating a dispatch handler wishes to take control
// of the response.
var overrideResponse = &gomemcached.MCResponse{}

type dispatchFun func(v *vbucket, vbreq *vbreq) (*gomemcached.MCResponse, *mutation)

var dispatchTable = [256]dispatchFun{
	gomemcached.GET:   vbGet,
	gomemcached.GETK:  vbGet,
	gomemcached.GETQ:  vbGet,
	gomemcached.GETKQ: vbGet,

	gomemcached.SET:  vbSet,
	gomemcached.SETQ: vbSet,

	gomemcached.DELETE:  vbDelete,
	gomemcached.DELETEQ: vbDelete,

	gomemcached.RGET: vbRGet,

	// TODO: Replace CHANGES_SINCE with enhanced TAP.
	CHANGES_SINCE: vbChangesSince,

	// TODO: Move new command codes to gomemcached one day.
	GET_VBMETA: vbGetVBMeta,
	SET_VBMETA: vbSetVBMeta,
}

func init() {
	// This handler references the initialized dispatch table
	// cyclically, so I'm initializing this particular handler
	// slightly later to break the cycle.  A refactoring might
	// make this unnecessary.
	dispatchTable[SPLIT_RANGE] = vbSplitRange
}

const observerBroadcastMax = 100

func newVBucket(parent bucket, vbid uint16, bs *bucketstore) (*vbucket, error) {
	rv := &vbucket{
		parent:      parent,
		meta:        VBMeta{Id: vbid, State: VBDead.String()},
		mem:         newMemStore(),
		bs:          bs,
		ch:          make(chan vbreq),
		ach:         make(chan vbapplyreq),
		observer:    newBroadcaster(observerBroadcastMax),
		collItems:   fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_ITEMS),
		collChanges: fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_CHANGES),
	}

	bs.apply(true, func(bs *bucketstore) {
		bs.coll(rv.collItems)
		bs.coll(rv.collChanges)
	})

	go rv.service()

	return rv, nil
}

func (v *vbucket) service() {
	for {
		select {
		case ar, ok := <-v.ach:
			if !ok {
				return
			}
			ar.cb(v)
			if ar.res != nil {
				close(ar.res)
			}
		case req, ok := <-v.ch:
			if !ok {
				return
			}
			res := v.dispatch(&req)
			if res != overrideResponse {
				v.respond(&req, res)
			}
		}
	}
}

func (v *vbucket) Close() error {
	close(v.ch)
	close(v.ach)
	return v.observer.Close()
}

func (v *vbucket) Dispatch(w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	resch := make(chan *gomemcached.MCResponse, 1)
	v.ch <- vbreq{w, req, resch}
	return <-resch
}

func (v *vbucket) dispatch(vbr *vbreq) *gomemcached.MCResponse {
	if vbr.w != nil {
		v.stats.Ops++
	}

	f := dispatchTable[vbr.req.Opcode]
	if f == nil {
		if vbr.w != nil {
			v.stats.Unknowns++
		}
		return &gomemcached.MCResponse{
			Status: gomemcached.UNKNOWN_COMMAND,
			Body: []byte(fmt.Sprintf("Unknown command %v",
				vbr.req.Opcode)),
		}
	}

	res, msg := f(v, vbr)
	if msg != nil {
		v.observer.Submit(*msg)
	}
	return res
}

func (v *vbucket) respond(vr *vbreq, res *gomemcached.MCResponse) {
	vr.resch <- res
}

func (v *vbucket) get(key []byte) *gomemcached.MCResponse {
	return v.Dispatch(nil, &gomemcached.MCRequest{
		Opcode:  gomemcached.GET,
		Key:     key,
		VBucket: v.meta.Id,
	})
}

func (v *vbucket) Apply(cb func(*vbucket)) {
	req := vbapplyreq{cb: cb, res: make(chan bool)}
	v.ach <- req
	<-req.res
}

func (v *vbucket) ApplyAsync(cb func(*vbucket)) {
	v.ach <- vbapplyreq{cb: cb, res: nil}
}

func (v *vbucket) ApplyBucketStore(cb func(*bucketstore)) {
	v.bs.apply(true, cb)
}

func (v *vbucket) ApplyBucketStoreAsync(cb func(*bucketstore)) {
	v.bs.apply(false, cb)
}

func (v *vbucket) GetVBState() (res VBState) {
	v.Apply(func(vbLocked *vbucket) {
		res = parseVBState(vbLocked.meta.State)
	})
	return
}

// TODO: SetVBState should also return an error to expose any storage errors.
func (v *vbucket) SetVBState(newState VBState,
	cb func(oldState VBState)) (oldState VBState) {
	v.Apply(func(vbLocked *vbucket) {
		vbLocked.ApplyBucketStore(func(bs *bucketstore) {
			oldState = parseVBState(vbLocked.meta.State)
			newMeta := &VBMeta{Id: v.meta.Id}
			newMeta.update(&v.meta)
			newMeta.State = newState.String()
			j, err := json.Marshal(newMeta)
			if err != nil {
				return
			}
			k := []byte(fmt.Sprintf("%d", newMeta.Id))
			if err := bs.coll(COLL_VBMETA).Set(k, j); err != nil {
				return
			}
			if err := bs.flush(); err != nil {
				return
			}
			vbLocked.meta.update(newMeta)
			if cb != nil {
				cb(oldState)
			}
		})
	})
	return
}

func (v *vbucket) load() (err error) {
	v.Apply(func(vbLocked *vbucket) {
		vbLocked.ApplyBucketStore(func(bs *bucketstore) {
			// TODO: Need to load changes?
			meta := &VBMeta{}
			meta.Id = v.meta.Id
			meta.update(&v.meta)

			x, err := bs.coll(COLL_VBMETA).GetItem(
				[]byte(fmt.Sprintf("%v", v.meta.Id)), true)
			if err != nil {
				return
			}
			if x != nil && x.Val != nil {
				if err = json.Unmarshal(x.Val, meta); err != nil {
					return
				}
			} // TODO: Handle else of missing COLL_VBMETA.

			visitor := func(i *item) bool {
				// TODO: What if we're loading something out of allowed range?
				// TODO: Don't want to have all values warmed into memory?
				if v.meta.LastCas < i.cas {
					v.meta.LastCas = i.cas
				}
				vbLocked.mem.set(i, nil)
				v.stats.Items++
				return true
			}
			err = bs.visit(v.collItems, nil, true, visitor)
			if err == nil {
				v.meta.update(meta)
			}
		})
	})
	return err
}

func (v *vbucket) AddStats(dest *Stats, key string) {
	v.Apply(func(vbLocked *vbucket) {
		if parseVBState(v.meta.State) == VBActive { // TODO: handle key
			dest.Add(&vbLocked.stats)
		}
	})
}

func (v *vbucket) checkRange(req *gomemcached.MCRequest) *gomemcached.MCResponse {
	if v.meta.KeyRange != nil {
		if len(v.meta.KeyRange.MinKeyInclusive) > 0 &&
			bytes.Compare(req.Key, v.meta.KeyRange.MinKeyInclusive) < 0 {
			v.stats.ErrNotMyRange++
			return &gomemcached.MCResponse{Status: NOT_MY_RANGE}
		}
		if len(v.meta.KeyRange.MaxKeyExclusive) > 0 &&
			bytes.Compare(req.Key, v.meta.KeyRange.MaxKeyExclusive) >= 0 {
			v.stats.ErrNotMyRange++
			return &gomemcached.MCResponse{Status: NOT_MY_RANGE}
		}
	}
	return nil
}

func vbSet(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	v.stats.Sets++

	req := vbr.req
	if rangeErr := v.checkRange(req); rangeErr != nil {
		return rangeErr, nil
	}

	meta, err := v.mem.getMeta(req.Key)
	if err != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.TMPFAIL,
			Body:   []byte(fmt.Sprintf("Store error %v", err)),
		}, nil
	}
	if req.Cas != 0 && (meta == nil || meta.cas != req.Cas) {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
		}, nil
	}

	v.meta.LastCas++
	itemCas := v.meta.LastCas

	itemNew := &item{
		// TODO: Extras
		key:  req.Key,
		cas:  itemCas,
		data: req.Body,
	}

	v.mem.set(itemNew, meta)
	if meta != nil {
		v.stats.Updates++
	} else {
		v.stats.Creates++
		v.stats.Items++
	}
	v.stats.ValueBytesIncoming += uint64(len(req.Body))

	itemNewBack := itemNew.clone()
	v.ApplyBucketStoreAsync(func(bs *bucketstore) {
		// TODO: Handle async error.
		bs.set(v.collItems, v.collChanges, itemNewBack, meta)
	})

	toBroadcast := &mutation{v.meta.Id, req.Key, itemCas, false}
	if req.Opcode.IsQuiet() {
		return nil, toBroadcast
	}
	return &gomemcached.MCResponse{Cas: itemCas}, toBroadcast
}

func vbGet(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	// Only update stats for requests that came from the "outside".
	if vbr.w != nil {
		v.stats.Gets++
	}

	req := vbr.req
	if rangeErr := v.checkRange(req); rangeErr != nil {
		return rangeErr, nil
	}

	i, err := v.mem.get(req.Key)
	if err != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.TMPFAIL,
			Body:   []byte(fmt.Sprintf("Store error %v", err)),
		}, nil
	}

	if i == nil {
		if vbr.w != nil {
			v.stats.GetMisses++
		}
		if req.Opcode.IsQuiet() {
			return nil, nil
		}
		return &gomemcached.MCResponse{
			Status: gomemcached.KEY_ENOENT,
		}, nil
	}

	if i.data == nil {
		vbGetBackgroundFetch(v, vbr, i.cas)
		return overrideResponse, nil
	}

	res := &gomemcached.MCResponse{
		Cas:    i.cas,
		Extras: make([]byte, 4), // TODO: Extras!
		Body:   i.data,
	}

	if vbr.w != nil {
		v.stats.ValueBytesOutgoing += uint64(len(i.data))
	}

	wantsKey := (req.Opcode == gomemcached.GETK || req.Opcode == gomemcached.GETKQ)
	if wantsKey {
		res.Key = req.Key
	}
	return res, nil
}

func vbGetBackgroundFetch(v *vbucket, vbr *vbreq, cas uint64) {
	v.ApplyBucketStoreAsync(func(bs *bucketstore) {
		fetchedItem, err := bs.get(v.collItems, vbr.req.Key)

		// After fetching, call "through the top" asynchronously (so there's
		// no deadlock) so the mem update is synchronous.
		v.ApplyAsync(func(vbLocked *vbucket) {
			if err != nil {
				v.stats.FetchedErr++
				v.respond(vbr, &gomemcached.MCResponse{
					Status: gomemcached.TMPFAIL,
					Body:   []byte(fmt.Sprintf("Fetch error %v", err)),
				})
				return
			}

			if fetchedItem != nil {
				v.stats.FetchedItems++
				currMeta, err := v.mem.getMeta(fetchedItem.key)
				if err != nil {
					v.stats.FetchedErr++
					v.respond(vbr, &gomemcached.MCResponse{
						Status: gomemcached.TMPFAIL,
						Body:   []byte(fmt.Sprintf("Fetch error %v", err)),
					})
					return
				}

				if currMeta != nil {
					if currMeta.cas == cas {
						v.mem.set(fetchedItem, nil)
					} else {
						// Item was recently modified & bucketstore is behind.
						v.stats.FetchedModified++
					}
				} else {
					// Item was recently deleted & bucketstore is behind.
					v.stats.FetchedDeleted++
				}
			} else {
				v.stats.FetchedNil++
			}

			// TODO: not sure if we should recurse here.
			res, _ := vbGet(v, vbr)
			if res != overrideResponse {
				v.respond(vbr, res)
			} else {
				v.stats.FetchedAgain++
			}
		})
	})
}

func vbDelete(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	v.stats.Deletes++

	req := vbr.req
	if rangeErr := v.checkRange(req); rangeErr != nil {
		return rangeErr, nil
	}

	meta, err := v.mem.getMeta(req.Key)
	if err != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.TMPFAIL,
			Body:   []byte(fmt.Sprintf("Store error %v", err)),
		}, nil
	}

	if req.Cas != 0 && (meta == nil || meta.cas != req.Cas) {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
		}, nil
	}

	if meta == nil {
		if req.Opcode.IsQuiet() {
			return nil, nil
		}
		return &gomemcached.MCResponse{
			Status: gomemcached.KEY_ENOENT,
		}, nil
	}

	v.meta.LastCas++
	cas := v.meta.LastCas

	v.mem.del(req.Key, cas)
	v.stats.Items--

	v.ApplyBucketStoreAsync(func(bs *bucketstore) {
		// TODO: Handle async error.
		bs.del(v.collItems, v.collChanges, req.Key, cas)
	})

	toBroadcast := &mutation{v.meta.Id, req.Key, cas, true}
	return &gomemcached.MCResponse{}, toBroadcast
}

// Responds with the changes since the req.Cas, with the last response
// in the response stream having no key.
// TODO: Support a limit on changes-since, perhaps in the req.Extras.
func vbChangesSince(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	req := vbr.req
	res := &gomemcached.MCResponse{
		Opcode: req.Opcode,
		Cas:    req.Cas,
	}

	ch, errs := transmitPackets(vbr.w)
	var err error

	visitor := func(i *item) bool {
		if i.cas > req.Cas {
			ch <- &gomemcached.MCResponse{
				Opcode: req.Opcode,
				Key:    i.key,
				Cas:    i.cas,
				// TODO: Extras.
				// TODO: Should changes-since respond with item value?
			}
			select {
			case err = <-errs:
				return false
			default:
			}
		}
		return true
	}

	v.mem.visitChanges(req.Cas, visitor)
	close(ch)
	if err == nil {
		err = <-errs
	}
	if err != nil {
		log.Printf("Error sending changes-since: %v", err)
		return &gomemcached.MCResponse{Fatal: true}, nil
	}
	return res, nil
}

func vbGetVBMeta(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	if j, err := json.Marshal(&v.meta); err == nil {
		return &gomemcached.MCResponse{Body: j}, nil
	}
	return &gomemcached.MCResponse{Body: []byte("{}")}, nil
}

func vbSetVBMeta(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	req := vbr.req
	if req.Body != nil {
		meta := &VBMeta{}
		if err := json.Unmarshal(req.Body, meta); err != nil {
			return &gomemcached.MCResponse{Status: gomemcached.EINVAL}, nil
		}
		v.meta.update(meta)
		return &gomemcached.MCResponse{}, nil
	}
	return &gomemcached.MCResponse{Status: gomemcached.EINVAL}, nil
}

func vbRGet(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	// From http://code.google.com/p/memcached/wiki/RangeOps
	// Extras field  Bits
	// ------------------
	// End key len	 16
	// Reserved       8
	// Flags          8
	// Max results	 32

	// TODO: Extras.

	v.stats.RGets++

	// Snapshot during the vbucket service() "lock".
	storeSnapshot, err := v.mem.snapshot()
	if err != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.TMPFAIL,
			Body:   []byte(fmt.Sprintf("Store error %v", err)),
		}, nil
	}

	go func() {
		req := vbr.req
		res := &gomemcached.MCResponse{
			Opcode: req.Opcode,
			Cas:    req.Cas,
		}

		visitRGetResults := uint64(0)
		visitValueBytesOutgoing := uint64(0)

		visitor := func(i *item) bool {
			if bytes.Compare(i.key, req.Key) >= 0 {
				var err error

				if i.data == nil {
					// TODO: track stats for RGET bg-fetches?
					// TODO: should we fetch from snapshot?
					v.ApplyBucketStore(func(bs *bucketstore) {
						i, err = bs.get(v.collItems, i.key)
					})
				}
				if err != nil {
					res = &gomemcached.MCResponse{
						Status: gomemcached.TMPFAIL,
						Body:   []byte(fmt.Sprintf("Store error %v", err)),
					}
					return false
				}
				if i == nil {
					return true // Item was deleted while we were iterating.
				}
				if i.data == nil {
					res = &gomemcached.MCResponse{
						Status: gomemcached.TMPFAIL,
						Body:   []byte("Missing data"),
					}
					return false
				}

				err = (&gomemcached.MCResponse{
					Opcode: req.Opcode,
					Key:    i.key,
					Cas:    i.cas,
					Extras: make([]byte, 4), // TODO: Extras.
					Body:   i.data,
				}).Transmit(vbr.w)
				if err != nil {
					res = &gomemcached.MCResponse{Fatal: true}
					return false
				}
				visitRGetResults++
				visitValueBytesOutgoing += uint64(len(i.data))
			}
			return true
		}

		storeSnapshot.visitItems(req.Key, visitor)
		v.Apply(func(vbLocked *vbucket) {
			vbLocked.stats.RGetResults += visitRGetResults
			vbLocked.stats.ValueBytesOutgoing += visitValueBytesOutgoing
		})
		v.respond(vbr, res)
	}()

	return overrideResponse, nil
}

type VBSplitRangePart struct {
	VBucketId       int   `json:"vbucketId"`
	MinKeyInclusive Bytes `json:"minKeyInclusive"`
	MaxKeyExclusive Bytes `json:"maxKeyExclusive"`
}

type VBSplitRange struct {
	Splits []VBSplitRangePart `json:"splits"`
}

type VBSplitRangeParts []VBSplitRangePart

func (sr VBSplitRangeParts) Len() int {
	return len(sr)
}

func (sr VBSplitRangeParts) Less(i, j int) bool {
	return sr[i].VBucketId < sr[j].VBucketId
}

func (sr VBSplitRangeParts) Swap(i, j int) {
	sr[i], sr[j] = sr[j], sr[i]
}

func vbSplitRange(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	req := vbr.req
	if req.Body != nil {
		sr := &VBSplitRange{}
		if err := json.Unmarshal(req.Body, sr); err != nil {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("Error decoding split-range json: %v, err: %v",
					string(req.Body), err)),
			}, nil
		}
		return v.splitRange(sr), nil
	}
	return &gomemcached.MCResponse{Status: gomemcached.EINVAL}, nil
}

func (v *vbucket) splitRange(sr *VBSplitRange) (res *gomemcached.MCResponse) {
	// Spliting to just 1 new destination vbucket is allowed.  It's
	// equivalent to re-numbering a vbucket with a different
	// vbucket-id.
	if len(sr.Splits) < 1 {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
			Body:   []byte("Error need at least 1 splits"),
		}
	}

	// Sort the splits by vbucket-id so that duplicate checks are easy
	// and so that our upcoming "lock" visits will avoid deadlocking.
	sort.Sort(VBSplitRangeParts(sr.Splits))

	// Validate the splits.
	max := -1
	for _, split := range sr.Splits {
		if split.VBucketId < 0 || split.VBucketId >= MAX_VBUCKETS {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("vbucket id %v out of range",
					split.VBucketId)),
			}
		}
		if split.VBucketId <= max || uint16(split.VBucketId) == v.meta.Id {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("vbucket id %v is duplicate",
					split.VBucketId)),
			}
		}
		max = split.VBucketId
	}

	// TODO: Validate that split ranges are non-overlapping and fully
	// covering v's existing range.

	return v.splitRangeActual(sr.Splits)
}

func (v *vbucket) splitRangeActual(splits []VBSplitRangePart) (res *gomemcached.MCResponse) {
	res = &gomemcached.MCResponse{Status: gomemcached.EINVAL}

	var transferSplits func(int)
	transferSplits = func(splitIdx int) {
		log.Printf("transferSplits %v", splitIdx)

		if splitIdx >= len(splits) {
			// We reach this recursion base-case after all our newly
			// created destination vbuckets and source v have been
			// visited (we've "locked" all their goroutines), so mark
			// success so our unwinding code can complete the split
			// transfer.
			res = &gomemcached.MCResponse{}
			return
		}

		vbid := uint16(splits[splitIdx].VBucketId)
		var vb *vbucket
		created := false
		if vbid != v.meta.Id {
			vb = v.parent.CreateVBucket(vbid)
			created = true
		}
		if vb == nil {
			vb = v.parent.getVBucket(vbid)
		}
		if vb != nil {
			vb.Apply(func(vbLocked *vbucket) {
				if parseVBState(vbLocked.meta.State) == VBDead {
					transferSplits(splitIdx + 1)
					if res.Status == gomemcached.SUCCESS {
						err := v.rangeCopyTo(vbLocked,
							splits[splitIdx].MinKeyInclusive,
							splits[splitIdx].MaxKeyExclusive)
						if err != nil {
							res = &gomemcached.MCResponse{
								Status: gomemcached.TMPFAIL,
							}
						}
						// TODO: poke observers on vbLocked's changed state.
					}
				} else {
					res = &gomemcached.MCResponse{
						Status: gomemcached.EINVAL,
						Body: []byte(fmt.Sprintf("Error split-range, vbucket: %v,"+
							" state not initially dead or was incorrect,"+
							" was: %v, req: %v", vbid, vbLocked.meta.State, splits)),
					}
				}
			})
			if res.Status != gomemcached.SUCCESS && created {
				// TODO: Cleanup the vbucket that we created.
			}
		} else {
			res = &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("Error split-range,"+
					" tried to create/get vbucket: %v, req: %v",
					vbid, splits)),
			}
		}
	}

	transferSplits(0)
	if res.Status == gomemcached.SUCCESS {
		// TODO: Need to remove collections from bs.
		v.mem = newMemStore()
		v.meta.State = VBDead.String()
		v.meta.KeyRange = &VBKeyRange{}
	}
	return
}

func (v *vbucket) rangeCopyTo(dst *vbucket,
	minKeyInclusive []byte, maxKeyExclusive []byte) error {
	s, err := v.mem.rangeCopy(minKeyInclusive, maxKeyExclusive, "")
	if err != nil {
		return err
	}
	if s == nil {
		return errors.New("rangeCopyTo got nil mem copy")
	}

	v.ApplyBucketStore(func(bs *bucketstore) {
		err = bs.rangeCopy(v.collItems, dst.bs, dst.collItems,
			minKeyInclusive, maxKeyExclusive)
		if err == nil {
			err = bs.rangeCopy(v.collChanges, dst.bs, dst.collChanges,
				minKeyInclusive, maxKeyExclusive)
		}
	})

	if err != nil {
		return err
	}

	dst.mem = s
	dst.meta.update(&v.meta)
	dst.meta.KeyRange = &VBKeyRange{
		MinKeyInclusive: minKeyInclusive,
		MaxKeyExclusive: maxKeyExclusive,
	}
	return nil
}
