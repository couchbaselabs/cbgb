package cbgb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"

	"github.com/dustin/gomemcached"
)

const (
	CHANGES_SINCE       = gomemcached.CommandCode(0x60)
	GET_VBUCKET_CONFIG  = gomemcached.CommandCode(0x61)
	SET_VBUCKET_CONFIG  = gomemcached.CommandCode(0x62)
	SPLIT_RANGE         = gomemcached.CommandCode(0x63)
	NOT_MY_RANGE        = gomemcached.Status(0x60)
	COLL_SUFFIX_ITEMS   = ".i"
	COLL_SUFFIX_CHANGES = ".c"
)

type VBState uint8

const (
	_ = VBState(iota)
	VBActive
	VBReplica
	VBPending
	VBDead
)

var vbStateNames = []string{
	VBActive:  "active",
	VBReplica: "replica",
	VBPending: "pending",
	VBDead:    "dead",
}

func (v VBState) String() string {
	if v < VBActive || v > VBDead {
		panic("Invalid vb state")
	}
	return vbStateNames[v]
}

type VBConfig struct {
	MinKeyInclusive Bytes `json:"minKeyInclusive"`
	MaxKeyExclusive Bytes `json:"maxKeyExclusive"`
}

func (t *VBConfig) Equal(u *VBConfig) bool {
	return bytes.Equal(t.MinKeyInclusive, u.MinKeyInclusive) &&
		bytes.Equal(t.MaxKeyExclusive, u.MaxKeyExclusive)
}

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
	bs          *bucketstore
	mem         *memstore
	cas         uint64
	observer    *broadcaster
	vbid        uint16
	state       VBState
	config      *VBConfig
	stats       Stats
	ch          chan vbreq
	ach         chan vbapplyreq
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
	GET_VBUCKET_CONFIG: vbGetConfig,
	SET_VBUCKET_CONFIG: vbSetConfig,
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
		bs:          bs,
		mem:         newMemStore(),
		observer:    newBroadcaster(observerBroadcastMax),
		vbid:        vbid,
		state:       VBDead,
		ch:          make(chan vbreq),
		ach:         make(chan vbapplyreq),
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
		VBucket: v.vbid,
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
		res = vbLocked.state
	})
	return
}

func (v *vbucket) SetVBState(newState VBState,
	cb func(oldState VBState)) (oldState VBState) {
	v.Apply(func(vbLocked *vbucket) {
		oldState = vbLocked.state
		vbLocked.state = newState
		if cb != nil {
			cb(oldState)
		}
	})
	return
}

func (v *vbucket) load() (err error) {
	v.Apply(func(vbLocked *vbucket) {
		vbLocked.ApplyBucketStore(func(bs *bucketstore) {
			visitor := func(i *item) bool {
				// TODO: What if we're loading something out of allowed range?
				// TODO: Don't want to have all values warmed into memory?
				if v.cas <= i.cas {
					v.cas = i.cas + 1
				}
				vbLocked.mem.set(i, nil)
				v.stats.Items++
				return true
			}
			err = bs.visit(v.collItems, nil, true, visitor)
		})
	})
	return err
}

func (v *vbucket) AddStats(dest *Stats, key string) {
	v.Apply(func(vbLocked *vbucket) {
		if v.state == VBActive { // TODO: handle key
			dest.Add(&vbLocked.stats)
		}
	})
}

func (v *vbucket) checkRange(req *gomemcached.MCRequest) *gomemcached.MCResponse {
	if v.config != nil {
		if len(v.config.MinKeyInclusive) > 0 &&
			bytes.Compare(req.Key, v.config.MinKeyInclusive) < 0 {
			v.stats.ErrNotMyRange++
			return &gomemcached.MCResponse{Status: NOT_MY_RANGE}
		}
		if len(v.config.MaxKeyExclusive) > 0 &&
			bytes.Compare(req.Key, v.config.MaxKeyExclusive) >= 0 {
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

	itemCas := v.cas
	v.cas++

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

	toBroadcast := &mutation{v.vbid, req.Key, itemCas, false}
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

		// After fetching, call "through the top" so the
		// mem update is synchronous.
		v.ApplyAsync(func(vbLocked *vbucket) {
			if err != nil {
				v.stats.StoreBackFetchedErr++
				v.respond(vbr, &gomemcached.MCResponse{
					Status: gomemcached.TMPFAIL,
					Body:   []byte(fmt.Sprintf("Store error %v", err)),
				})
				return
			}

			if fetchedItem != nil {
				v.stats.StoreBackFetchedItems++
				currMeta, err := v.mem.getMeta(fetchedItem.key)
				if err != nil {
					v.stats.StoreBackFetchedErr++
					v.respond(vbr, &gomemcached.MCResponse{
						Status: gomemcached.TMPFAIL,
						Body:   []byte(fmt.Sprintf("Store error %v", err)),
					})
					return
				}

				if currMeta != nil {
					if currMeta.cas == cas {
						v.mem.set(fetchedItem, nil)
					} else {
						// Item was recently modified & storeBack is behind.
						v.stats.StoreBackFetchedModified++
					}
				} else {
					// Item was recently deleted & storeBack is behind.
					v.stats.StoreBackFetchedDeleted++
				}
			} else {
				v.stats.StoreBackFetchedNil++
			}

			// TODO: not sure if we should recurse here.
			res, _ := vbGet(v, vbr)
			if res != overrideResponse {
				v.respond(vbr, res)
			} else {
				v.stats.StoreBackFetchedAgain++
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

	cas := v.cas
	v.cas++

	v.mem.del(req.Key, cas)
	v.stats.Items--

	v.ApplyBucketStoreAsync(func(bs *bucketstore) {
		// TODO: Handle async error.
		bs.del(v.collItems, v.collChanges, req.Key, cas)
	})

	toBroadcast := &mutation{v.vbid, req.Key, cas, true}
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

func vbGetConfig(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	if v.config != nil {
		if j, err := json.Marshal(v.config); err == nil {
			return &gomemcached.MCResponse{Body: j}, nil
		}
	}
	return &gomemcached.MCResponse{Body: []byte("{}")}, nil
}

func vbSetConfig(v *vbucket, vbr *vbreq) (*gomemcached.MCResponse, *mutation) {
	req := vbr.req
	if req.Body != nil {
		config := &VBConfig{}
		err := json.Unmarshal(req.Body, config)
		if err == nil {
			v.config = config
			return &gomemcached.MCResponse{}, nil
		} else {
			log.Printf("Error decoding vbucket config: %v, err: %v",
				string(req.Body), err)
		}
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
		err := json.Unmarshal(req.Body, sr)
		if err == nil {
			return v.splitRange(sr), nil
		} else {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("Error decoding split-range json: %v, err: %v",
					string(req.Body), err)),
			}, nil
		}
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
		if split.VBucketId <= max || uint16(split.VBucketId) == v.vbid {
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
		if vbid != v.vbid {
			vb = v.parent.CreateVBucket(vbid)
			created = true
		}
		if vb == nil {
			vb = v.parent.getVBucket(vbid)
		}
		if vb != nil {
			vb.Apply(func(vbLocked *vbucket) {
				if vbLocked.state == VBDead {
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
							" was: %v, req: %v", vbid, vbLocked.state, splits)),
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
		v.state = VBDead
		v.config = &VBConfig{}
	}
	return
}

func (v *vbucket) rangeCopyTo(dst *vbucket,
	minKeyInclusive []byte, maxKeyExclusive []byte) error {
	s, err := v.mem.rangeCopy(minKeyInclusive, maxKeyExclusive, "")
	if err == nil {
		dst.mem = s

		v.ApplyBucketStore(func(bs *bucketstore) {
			err = bs.rangeCopy(v.collItems, dst.bs, dst.collItems,
				minKeyInclusive, maxKeyExclusive)
			if err == nil {
				err = bs.rangeCopy(v.collChanges, dst.bs, dst.collChanges,
					minKeyInclusive, maxKeyExclusive)
			}
		})

		if err == nil && dst.mem != nil {
			dst.cas = v.cas
			dst.state = v.state
			dst.config = &VBConfig{
				MinKeyInclusive: minKeyInclusive,
				MaxKeyExclusive: maxKeyExclusive,
			}
		}
	}
	return err
}
