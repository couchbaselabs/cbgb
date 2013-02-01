package cbgb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/dustin/gomemcached"
	"github.com/steveyen/gkvlite"
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

type vbucket struct {
	parent      bucket
	vbid        uint16
	meta        unsafe.Pointer // *VBMeta
	bs          *bucketstore
	ach         chan *funreq // To access top-level vbucket fields & stats.
	mch         chan *funreq // To mutate the items/changes collections.
	stats       Stats
	observer    *broadcaster
	collItems   unsafe.Pointer // *gkvlite.Collection
	collChanges unsafe.Pointer // *gkvlite.Collection
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

type dispatchFun func(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) *gomemcached.MCResponse

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

	SPLIT_RANGE: vbSplitRange,
}

const observerBroadcastMax = 100

func newVBucket(parent bucket, vbid uint16, bs *bucketstore) (rv *vbucket, err error) {
	pauseSwapColls := func(cic collItemsChanges) {
		rv.Mutate(func() {
			collItems, collChanges := cic()
			atomic.StorePointer(&rv.collItems, unsafe.Pointer(collItems))
			atomic.StorePointer(&rv.collChanges, unsafe.Pointer(collChanges))
		})
	}

	collItems, collChanges := bs.collItemsChanges(vbid, pauseSwapColls)
	rv = &vbucket{
		parent:      parent,
		vbid:        vbid,
		meta:        unsafe.Pointer(&VBMeta{Id: vbid, State: VBDead.String()}),
		bs:          bs,
		ach:         make(chan *funreq),
		mch:         make(chan *funreq),
		observer:    newBroadcaster(observerBroadcastMax),
		collItems:   unsafe.Pointer(collItems),
		collChanges: unsafe.Pointer(collChanges),
	}

	go funservice(rv.ach)
	go funservice(rv.mch)

	return rv, nil
}

func (v *vbucket) Meta() *VBMeta {
	return (*VBMeta)(atomic.LoadPointer(&v.meta))
}

func (v *vbucket) Close() error {
	// TODO: Can get panics if goroutines send to closed channels.
	// Perhaps use atomic pointer CAS on channels as the way out?
	close(v.ach)
	close(v.mch)
	return v.observer.Close()
}

func (v *vbucket) Dispatch(w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	atomic.AddUint64(&v.stats.Ops, 1)
	f := dispatchTable[req.Opcode]
	if f == nil {
		atomic.AddUint64(&v.stats.Unknowns, 1)
		return &gomemcached.MCResponse{
			Status: gomemcached.UNKNOWN_COMMAND,
			Body:   []byte(fmt.Sprintf("Unknown command %v", req.Opcode)),
		}
	}
	return f(v, w, req)
}

func (v *vbucket) get(key []byte) *gomemcached.MCResponse {
	return v.Dispatch(nil, &gomemcached.MCRequest{
		Opcode:  gomemcached.GET,
		Key:     key,
		VBucket: v.vbid,
	})
}

func (v *vbucket) CollItems() *gkvlite.Collection {
	return (*gkvlite.Collection)(atomic.LoadPointer(&v.collItems))
}

func (v *vbucket) CollChanges() *gkvlite.Collection {
	return (*gkvlite.Collection)(atomic.LoadPointer(&v.collChanges))
}

func (v *vbucket) Apply(fun func()) {
	req := &funreq{fun: fun, res: make(chan bool)}
	v.ach <- req
	<-req.res
}

func (v *vbucket) Mutate(fun func()) {
	req := &funreq{fun: fun, res: make(chan bool)}
	v.mch <- req
	<-req.res
}

func (v *vbucket) GetVBState() (res VBState) {
	return parseVBState(v.Meta().State)
}

func (v *vbucket) SetVBState(newState VBState,
	cb func(prevState VBState)) (prevState VBState, err error) {
	prevState = VBDead
	// The bs.apply() ensures we're not compacting/flushing while
	// changing vbstate, which is good for atomicity and to avoid
	// deadlock when the compactor wants to swap collections.
	v.bs.apply(func() {
		v.Apply(func() {
			v.Mutate(func() {
				prevMeta := v.Meta()
				prevState = parseVBState(prevMeta.State)
				casMeta := atomic.AddUint64(&prevMeta.LastCas, 1)

				newMeta := prevMeta.Copy()
				newMeta.State = newState.String()
				newMeta.MetaCas = casMeta

				err = v.setVBMeta(newMeta)
				if err != nil {
					return
				}
				if cb != nil {
					cb(prevState)
				}
			})
		})
	})
	return prevState, err
}

func (v *vbucket) setVBMeta(newMeta *VBMeta) (err error) {
	// This should only be called when holding the bucketstore
	// service/apply "lock", to ensure a Flush between changes stream
	// update and COLL_VBMETA update is atomic.
	var j []byte
	j, err = json.Marshal(newMeta)
	if err != nil {
		return err
	}
	k := []byte(fmt.Sprintf("%d", v.vbid))
	i := &item{
		key:  nil, // A nil key means it's a VBMeta change.
		cas:  newMeta.MetaCas,
		data: j,
	}
	if err = v.bs.set(nil, v.CollChanges(), i, nil); err != nil {
		return err
	}
	if err = v.bs.coll(COLL_VBMETA).Set(k, j); err != nil {
		return err
	}
	if err = v.bs.flush(); err != nil {
		return err
	}
	atomic.StorePointer(&v.meta, unsafe.Pointer(newMeta))
	return nil
}

func (v *vbucket) load() (err error) {
	v.Apply(func() {
		v.Mutate(func() {
			meta := v.Meta().Copy()

			x, err := v.bs.coll(COLL_VBMETA).GetItem(
				[]byte(fmt.Sprintf("%v", v.vbid)), true)
			if err != nil {
				return
			}
			if x == nil || x.Val == nil {
				err = errors.New("missing COLL_VBMETA")
				return
			}
			if err = json.Unmarshal(x.Val, meta); err != nil {
				return
			}

			i, err := v.CollChanges().MaxItem(true)
			if err != nil {
				return
			}
			if i != nil {
				var lastCas uint64
				lastCas, err = casBytesParse(i.Key)
				if err != nil {
					return
				}
				if meta.LastCas < lastCas {
					meta.LastCas = lastCas
				}
			}

			atomic.StorePointer(&v.meta, unsafe.Pointer(meta))

			// TODO: Need to update v.stats.Items.
			// TODO: What if we're loading something out of allowed range?
		})
	})

	return err
}

func (v *vbucket) AddStats(dest *Stats, key string) {
	v.Apply(func() { // Need apply protection due to stats.Items.
		if parseVBState(v.Meta().State) == VBActive { // TODO: handle key
			dest.Add(&v.stats)
		}
	})
}

func (v *vbucket) checkRange(req *gomemcached.MCRequest) *gomemcached.MCResponse {
	meta := v.Meta()
	if meta.KeyRange != nil {
		if len(meta.KeyRange.MinKeyInclusive) > 0 &&
			bytes.Compare(req.Key, meta.KeyRange.MinKeyInclusive) < 0 {
			atomic.AddUint64(&v.stats.ErrNotMyRange, 1)
			return &gomemcached.MCResponse{Status: NOT_MY_RANGE}
		}
		if len(meta.KeyRange.MaxKeyExclusive) > 0 &&
			bytes.Compare(req.Key, meta.KeyRange.MaxKeyExclusive) >= 0 {
			atomic.AddUint64(&v.stats.ErrNotMyRange, 1)
			return &gomemcached.MCResponse{Status: NOT_MY_RANGE}
		}
	}
	return nil
}

func vbSet(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	atomic.AddUint64(&v.stats.Sets, 1)

	res = v.checkRange(req)
	if res != nil {
		return res
	}

	var itemCas uint64
	v.Apply(func() {
		// TODO: We have the apply to avoid races, but is there a better way?
		itemCas = atomic.AddUint64(&v.Meta().LastCas, 1)
	})

	var prevMeta *item
	var err error

	v.Mutate(func() {
		prevMeta, err = v.bs.getMeta(v.CollItems(), v.CollChanges(), req.Key)
		if err != nil {
			res = &gomemcached.MCResponse{
				Status: gomemcached.TMPFAIL,
				Body:   []byte(fmt.Sprintf("Store getMeta error %v", err)),
			}
			return
		}
		if req.Cas != 0 && (prevMeta == nil || prevMeta.cas != req.Cas) {
			err = errors.New("CAS mismatch")
			res = &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body:   []byte(fmt.Sprintf("CAS mismatch")),
			}
			return
		}

		itemNew := &item{
			// TODO: Extras
			key:  req.Key,
			cas:  itemCas,
			data: req.Body,
		}

		err = v.bs.set(v.CollItems(), v.CollChanges(), itemNew, prevMeta)
		if err != nil {
			res = &gomemcached.MCResponse{
				Status: gomemcached.TMPFAIL,
				Body:   []byte(fmt.Sprintf("Store set error %v", err)),
			}
		} else {
			if !req.Opcode.IsQuiet() {
				res = &gomemcached.MCResponse{Cas: itemCas}
			}
		}
	})

	if err != nil {
		atomic.AddUint64(&v.stats.ErrStore, 1)
	} else {
		if prevMeta != nil {
			atomic.AddUint64(&v.stats.Updates, 1)
		} else {
			atomic.AddUint64(&v.stats.Creates, 1)
			atomic.AddInt64(&v.stats.Items, 1)
		}
		atomic.AddUint64(&v.stats.ValueBytesIncoming, uint64(len(req.Body)))
	}

	if err == nil {
		v.observer.Submit(mutation{v.vbid, req.Key, itemCas, false})
	}

	return res
}

func vbGet(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	atomic.AddUint64(&v.stats.Gets, 1)

	res = v.checkRange(req)
	if res != nil {
		return res
	}

	i, err := v.bs.get(v.CollItems(), v.CollChanges(), req.Key)
	if err != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.TMPFAIL,
			Body:   []byte(fmt.Sprintf("Store get error %v", err)),
		}
	}

	if i == nil {
		atomic.AddUint64(&v.stats.GetMisses, 1)
	} else {
		atomic.AddUint64(&v.stats.ValueBytesOutgoing, uint64(len(i.data)))
	}

	if i == nil {
		if req.Opcode.IsQuiet() {
			return nil
		}
		return &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT}
	}

	res = &gomemcached.MCResponse{
		Cas:    i.cas,
		Extras: make([]byte, 4), // TODO: Extras!
		Body:   i.data,
	}
	wantsKey := (req.Opcode == gomemcached.GETK || req.Opcode == gomemcached.GETKQ)
	if wantsKey {
		res.Key = req.Key
	}
	return res
}

func vbDelete(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	atomic.AddUint64(&v.stats.Deletes, 1)

	res = v.checkRange(req)
	if res != nil {
		return res
	}

	var cas uint64
	v.Apply(func() {
		// TODO: We have the apply to avoid races, but is there a better way?
		cas = atomic.AddUint64(&v.Meta().LastCas, 1)
	})

	var prevMeta *item
	var err error

	v.Mutate(func() {
		prevMeta, err = v.bs.getMeta(v.CollItems(), v.CollChanges(), req.Key)
		if err != nil {
			res = &gomemcached.MCResponse{
				Status: gomemcached.TMPFAIL,
				Body:   []byte(fmt.Sprintf("Store getMeta error %v", err)),
			}
			return
		}
		if req.Cas != 0 && (prevMeta == nil || prevMeta.cas != req.Cas) {
			res = &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body:   []byte(fmt.Sprintf("CAS mismatch")),
			}
			return
		}

		if prevMeta == nil {
			if req.Opcode.IsQuiet() {
				return
			}
			res = &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT}
			return
		}

		err = v.bs.del(v.CollItems(), v.CollChanges(), req.Key, cas)
		if err != nil {
			res = &gomemcached.MCResponse{
				Status: gomemcached.TMPFAIL,
				Body:   []byte(fmt.Sprintf("Store del error %v", err)),
			}
		} else {
			if !req.Opcode.IsQuiet() {
				res = &gomemcached.MCResponse{Cas: cas}
			}
		}
	})

	if err != nil {
		atomic.AddUint64(&v.stats.ErrStore, 1)
	} else if prevMeta != nil {
		atomic.AddInt64(&v.stats.Items, -1)
	}

	if err == nil && prevMeta != nil {
		v.observer.Submit(mutation{v.vbid, req.Key, cas, true})
	}

	return res
}

// Responds with the changes since the req.Cas, with the last response
// in the response stream having no key.
// TODO: Support a limit on changes-since, perhaps in the req.Extras.
func vbChangesSince(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	res = &gomemcached.MCResponse{Opcode: req.Opcode, Cas: req.Cas}

	var err error

	ch, errs := transmitPackets(w)

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

	errVisit := v.bs.visitChanges(v.CollChanges(), casBytes(req.Cas), true, visitor)

	close(ch)

	if errVisit != nil {
		return &gomemcached.MCResponse{Fatal: true}
	}

	if err == nil {
		err = <-errs
	}
	if err != nil {
		log.Printf("Error sending changes-since: %v", err)
		return &gomemcached.MCResponse{Fatal: true}
	}

	// TODO: Update stats.

	return res
}

func vbGetVBMeta(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	v.Apply(func() {
		if j, err := json.Marshal(v.Meta()); err == nil {
			res = &gomemcached.MCResponse{Body: j}
		} else {
			res = &gomemcached.MCResponse{Body: []byte("{}")}
		}
	})
	return res
}

func vbSetVBMeta(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	res = &gomemcached.MCResponse{Status: gomemcached.EINVAL}
	// The bs.apply() ensures we're not compacting/flushing while
	// changing vbstate, which is good for atomicity and to avoid
	// deadlock when the compactor wants to swap collections.
	v.bs.apply(func() {
		v.Apply(func() {
			v.Mutate(func() {
				if req.Body != nil {
					newMeta := &VBMeta{}
					if err := json.Unmarshal(req.Body, newMeta); err != nil {
						return
					}

					prevMeta := v.Meta()
					casMeta := atomic.AddUint64(&prevMeta.LastCas, 1)

					newMeta = prevMeta.Copy().update(newMeta)
					newMeta.MetaCas = casMeta

					if err := v.setVBMeta(newMeta); err != nil {
						res = &gomemcached.MCResponse{
							Status: gomemcached.TMPFAIL,
							Body:   []byte(fmt.Sprintf("setVBMeta error %v", err)),
						}
						return
					}

					res = &gomemcached.MCResponse{}
				}
			})
		})
	})
	return res
}

func vbRGet(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	// From http://code.google.com/p/memcached/wiki/RangeOps
	// Extras field  Bits
	// ------------------
	// End key len	 16
	// Reserved       8
	// Flags          8
	// Max results	 32

	// TODO: Extras.

	res = &gomemcached.MCResponse{
		Opcode: req.Opcode,
		Cas:    req.Cas,
	}

	visitRGetResults := uint64(0)
	visitValueBytesOutgoing := uint64(0)

	visitor := func(i *item) bool {
		if bytes.Compare(i.key, req.Key) >= 0 {
			err := (&gomemcached.MCResponse{
				Opcode: req.Opcode,
				Key:    i.key,
				Cas:    i.cas,
				Extras: make([]byte, 4), // TODO: Extras.
				Body:   i.data,
			}).Transmit(w)
			if err != nil {
				res = &gomemcached.MCResponse{Fatal: true}
				return false
			}
			visitRGetResults++
			visitValueBytesOutgoing += uint64(len(i.data))
		}
		return true
	}

	if err := v.bs.visitItems(v.CollItems(), v.CollChanges(),
		req.Key, true, visitor); err != nil {
		res = &gomemcached.MCResponse{Fatal: true}
	}

	atomic.AddUint64(&v.stats.RGets, 1)
	atomic.AddUint64(&v.stats.RGetResults, visitRGetResults)
	atomic.AddUint64(&v.stats.ValueBytesOutgoing, visitValueBytesOutgoing)

	return res
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

func vbSplitRange(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	if req.Body != nil {
		sr := &VBSplitRange{}
		if err := json.Unmarshal(req.Body, sr); err != nil {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("Error decoding split-range json: %v, err: %v",
					string(req.Body), err)),
			}
		}
		return v.splitRange(sr)
	}
	return &gomemcached.MCResponse{Status: gomemcached.EINVAL}
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
		if vbid != v.Meta().Id {
			vb, _ = v.parent.CreateVBucket(vbid)
			created = true
		}
		if vb == nil {
			vb = v.parent.getVBucket(vbid)
		}

		// TODO: Possible race here, in-between creation and access,
		// an adversary could delete the vbucket?

		if vb != nil {
			vb.Apply(func() {
				if parseVBState(vb.Meta().State) == VBDead {
					transferSplits(splitIdx + 1)
					if res.Status == gomemcached.SUCCESS {
						err := v.rangeCopyTo(vb,
							splits[splitIdx].MinKeyInclusive,
							splits[splitIdx].MaxKeyExclusive)
						if err != nil {
							res = &gomemcached.MCResponse{
								Status: gomemcached.TMPFAIL,
							}
						}
						// TODO: poke observers on vb's changed state.
					}
				} else {
					res = &gomemcached.MCResponse{
						Status: gomemcached.EINVAL,
						Body: []byte(fmt.Sprintf("Error split-range, vbucket: %v,"+
							" state not initially dead or was incorrect,"+
							" was: %v, req: %v", vbid, vb.Meta().State, splits)),
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
		v.Apply(func() {
			meta := v.Meta()
			meta.State = VBDead.String()
			meta.KeyRange = &VBKeyRange{}
			// TODO: Need to flush?
			// TODO: Need a new 'purge vbucket' command?
		})
	}
	return
}

func (v *vbucket) rangeCopyTo(dst *vbucket,
	minKeyInclusive []byte, maxKeyExclusive []byte) error {
	// TODO: Should this be under dst.Apply()/Mutate().

	err := v.bs.rangeCopy(v.CollItems(), dst.bs, dst.CollItems(),
		minKeyInclusive, maxKeyExclusive)
	if err == nil {
		err = v.bs.rangeCopy(v.CollChanges(), dst.bs, dst.CollChanges(),
			minKeyInclusive, maxKeyExclusive)
	}
	if err != nil {
		return err
	}

	dstMeta := dst.Meta().Copy()
	dstMeta.update(v.Meta())
	dstMeta.KeyRange = &VBKeyRange{
		MinKeyInclusive: minKeyInclusive,
		MaxKeyExclusive: maxKeyExclusive,
	}
	dstMeta.MetaCas = dstMeta.LastCas
	atomic.StorePointer(&dst.meta, unsafe.Pointer(dstMeta))

	return nil
}
