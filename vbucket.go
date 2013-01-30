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

type vbapplyreq struct {
	cb  func(*vbucket)
	res chan bool
}

type vbucket struct {
	parent      bucket
	vbid        uint16
	meta        unsafe.Pointer // *VBMeta
	bs          *bucketstore
	ach         chan vbapplyreq // To access top-level vbucket fields & stats.
	mch         chan vbapplyreq // To mutate the items/changes collections.
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
		rv.Mutate(func(vbLocked *vbucket) {
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
		ach:         make(chan vbapplyreq),
		mch:         make(chan vbapplyreq),
		observer:    newBroadcaster(observerBroadcastMax),
		collItems:   unsafe.Pointer(collItems),
		collChanges: unsafe.Pointer(collChanges),
	}

	go rv.service(rv.ach)
	go rv.service(rv.mch)

	return rv, nil
}

func (v *vbucket) Meta() *VBMeta {
	return (*VBMeta)(atomic.LoadPointer(&v.meta))
}

func (v *vbucket) service(ch chan vbapplyreq) {
	for r := range ch {
		r.cb(v)
		close(r.res)
	}
}

func (v *vbucket) Close() error {
	// TODO: Can get panics if goroutines send to closed channels.
	// Perhaps use atomic pointer CAS on channels as the way out?
	close(v.ach)
	close(v.mch)
	return v.observer.Close()
}

func (v *vbucket) Dispatch(w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	f := dispatchTable[req.Opcode]
	if f == nil {
		v.Apply(func(vbLocked *vbucket) {
			vbLocked.stats.Ops++
			vbLocked.stats.Unknowns++
		})
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

func (v *vbucket) Apply(cb func(*vbucket)) {
	req := vbapplyreq{cb: cb, res: make(chan bool)}
	v.ach <- req
	<-req.res
}

func (v *vbucket) Mutate(cb func(*vbucket)) {
	req := vbapplyreq{cb: cb, res: make(chan bool)}
	v.mch <- req
	<-req.res
}

func (v *vbucket) GetVBState() (res VBState) {
	return parseVBState(v.Meta().State)
}

func (v *vbucket) SetVBState(newState VBState,
	cb func(prevState VBState)) (prevState VBState, err error) {
	prevState = VBDead
	v.Apply(func(vbLocked *vbucket) {
		vbLocked.Mutate(func(vm *vbucket) {
			prevMeta := vbLocked.Meta()
			prevState = parseVBState(prevMeta.State)

			newMeta := prevMeta.Copy()
			newMeta.State = newState.String()

			var j []byte
			j, err = json.Marshal(newMeta)
			if err != nil {
				return
			}
			k := []byte(fmt.Sprintf("%d", v.vbid))
			if err = vm.bs.coll(COLL_VBMETA).Set(k, j); err != nil {
				return
			}
			atomic.StorePointer(&vbLocked.meta, unsafe.Pointer(newMeta))
			if cb != nil {
				cb(prevState)
			}
		})
	})
	if err != nil {
		return prevState, err
	}

	// Flush outside the Apply/Mutate sections to avoid deadlock, in
	// case the bucketstore calls us back to swap collections.
	return prevState, v.bs.Flush()
}

func (v *vbucket) load() (err error) {
	v.Apply(func(vbLocked *vbucket) {
		vbLocked.Mutate(func(vm *vbucket) {
			meta := vbLocked.Meta().Copy()

			x, err := vm.bs.coll(COLL_VBMETA).GetItem(
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

			i, err := vm.CollChanges().MaxItem(true)
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

			atomic.StorePointer(&vbLocked.meta, unsafe.Pointer(meta))

			// TODO: Need to update v.stats.Items.
			// TODO: What if we're loading something out of allowed range?
		})
	})

	return err
}

func (v *vbucket) AddStats(dest *Stats, key string) {
	if parseVBState(v.Meta().State) == VBActive { // TODO: handle key
		dest.Add(&v.stats)
	}
}

func (v *vbucket) checkRange(req *gomemcached.MCRequest) *gomemcached.MCResponse {
	meta := v.Meta()
	if meta.KeyRange != nil {
		if len(meta.KeyRange.MinKeyInclusive) > 0 &&
			bytes.Compare(req.Key, meta.KeyRange.MinKeyInclusive) < 0 {
			v.stats.ErrNotMyRange++
			return &gomemcached.MCResponse{Status: NOT_MY_RANGE}
		}
		if len(meta.KeyRange.MaxKeyExclusive) > 0 &&
			bytes.Compare(req.Key, meta.KeyRange.MaxKeyExclusive) >= 0 {
			v.stats.ErrNotMyRange++
			return &gomemcached.MCResponse{Status: NOT_MY_RANGE}
		}
	}
	return nil
}

func vbSet(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	var itemCas uint64

	v.Apply(func(vbLocked *vbucket) {
		vbLocked.stats.Ops++
		vbLocked.stats.Sets++

		res = vbLocked.checkRange(req)
		if res == nil {
			itemCas = atomic.AddUint64(&vbLocked.Meta().LastCas, 1)
		}
	})
	if res != nil {
		return res
	}

	var prevMeta *item
	var err error

	v.Mutate(func(vm *vbucket) {
		prevMeta, err = vm.bs.getMeta(vm.CollItems(), vm.CollChanges(), req.Key)
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

		err = vm.bs.set(vm.CollItems(), vm.CollChanges(), itemNew, prevMeta)
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

	v.Apply(func(vbLocked *vbucket) {
		if err != nil {
			vbLocked.stats.ErrStore++
		} else {
			if prevMeta != nil {
				vbLocked.stats.Updates++
			} else {
				vbLocked.stats.Creates++
				vbLocked.stats.Items++
			}
			vbLocked.stats.ValueBytesIncoming += uint64(len(req.Body))
		}
	})

	if err == nil {
		v.observer.Submit(mutation{v.vbid, req.Key, itemCas, false})
	}

	return res
}

func vbGet(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	v.Apply(func(vbLocked *vbucket) {
		vbLocked.stats.Ops++
		vbLocked.stats.Gets++

		res = vbLocked.checkRange(req)
	})
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

	v.Apply(func(vbLocked *vbucket) {
		if i == nil {
			vbLocked.stats.GetMisses++
		} else {
			vbLocked.stats.ValueBytesOutgoing += uint64(len(i.data))
		}
	})

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
	var cas uint64

	v.Apply(func(vbLocked *vbucket) {
		vbLocked.stats.Ops++
		vbLocked.stats.Deletes++

		res = vbLocked.checkRange(req)
		if res == nil {
			cas = atomic.AddUint64(&vbLocked.Meta().LastCas, 1)
		}
	})
	if res != nil {
		return res
	}

	var prevMeta *item
	var err error

	v.Mutate(func(vm *vbucket) {
		prevMeta, err = vm.bs.getMeta(vm.CollItems(), vm.CollChanges(), req.Key)
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

		err = vm.bs.del(vm.CollItems(), vm.CollChanges(), req.Key, cas)
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

	v.Apply(func(vbLocked *vbucket) {
		if err != nil {
			vbLocked.stats.ErrStore++
		} else if prevMeta != nil {
			vbLocked.stats.Items--
		}
	})

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

	// TODO: Update stats including Ops counter.

	return res
}

func vbGetVBMeta(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	v.Apply(func(vbLocked *vbucket) {
		// TODO: Might also need to Mutate() for vbGetMeta/vbSetVBMeta race avoidance.
		vbLocked.stats.Ops++
		if j, err := json.Marshal(vbLocked.Meta()); err == nil {
			res = &gomemcached.MCResponse{Body: j}
		} else {
			res = &gomemcached.MCResponse{Body: []byte("{}")}
		}
	})
	return res
}

func vbSetVBMeta(v *vbucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	res = &gomemcached.MCResponse{Status: gomemcached.EINVAL}
	v.Apply(func(vbLocked *vbucket) {
		// TODO: Might also need to Mutate() for vbGetMeta/vbSetVBMeta race avoidance.
		vbLocked.stats.Ops++
		if req.Body != nil {
			meta := &VBMeta{}
			if err := json.Unmarshal(req.Body, meta); err != nil {
				return
			}
			atomic.StorePointer(&vbLocked.meta,
				unsafe.Pointer(vbLocked.Meta().Copy().update(meta)))
			// TODO: record the meta change in the changes stream.
			// TODO: flush.
			res = &gomemcached.MCResponse{}
		}
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

	v.Apply(func(vbLocked *vbucket) {
		vbLocked.stats.Ops++
		vbLocked.stats.RGets++
		vbLocked.stats.RGetResults += visitRGetResults
		vbLocked.stats.ValueBytesOutgoing += visitValueBytesOutgoing
		// TODO: Track errors.
	})

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
			vb.Apply(func(vbLocked *vbucket) {
				if parseVBState(vbLocked.Meta().State) == VBDead {
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
							" was: %v, req: %v", vbid, vbLocked.Meta().State, splits)),
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
		v.Apply(func(vbLocked *vbucket) {
			meta := vbLocked.Meta()
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
	atomic.StorePointer(&dst.meta, unsafe.Pointer(dstMeta))

	return nil
}
