package cbgb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"

	"github.com/dustin/gomemcached"
	"github.com/petar/GoLLRB/llrb"
)

const (
	CHANGES_SINCE      = gomemcached.CommandCode(0x60)
	GET_VBUCKET_CONFIG = gomemcached.CommandCode(0x61)
	SET_VBUCKET_CONFIG = gomemcached.CommandCode(0x62)
	SPLIT_RANGE        = gomemcached.CommandCode(0x63)
	NOT_MY_RANGE       = gomemcached.Status(0x60)
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

type item struct {
	key       []byte
	exp, flag uint32
	cas       uint64
	data      []byte
}

func KeyLess(p, q interface{}) bool {
	return bytes.Compare(p.(*item).key, q.(*item).key) < 0
}

func CASLess(p, q interface{}) bool {
	return p.(*item).cas < q.(*item).cas
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

type vbvisitreq struct {
	cb  func(*vbucket)
	res chan bool
}

type vbucket struct {
	parent    *bucket
	items     *llrb.Tree
	changes   *llrb.Tree
	cas       uint64
	observer  *broadcaster
	vbid      uint16
	state     VBState
	config    *VBConfig
	stats     Stats
	suspended bool
	ch        chan vbreq
	vch       chan vbvisitreq
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

const dataBroadcastBufLen = 100

type dispatchFun func(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (*gomemcached.MCResponse, *mutation)

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

func newVBucket(parent *bucket, vbid uint16) *vbucket {
	rv := &vbucket{
		parent:   parent,
		items:    llrb.New(KeyLess),
		changes:  llrb.New(CASLess),
		observer: newBroadcaster(dataBroadcastBufLen),
		vbid:     vbid,
		state:    VBDead,
		ch:       make(chan vbreq),
		vch:      make(chan vbvisitreq),
	}

	go rv.service()

	return rv
}

func (v *vbucket) Close() error {
	close(v.vch)
	return v.observer.Close()
}

func (v *vbucket) get(key []byte) *gomemcached.MCResponse {
	return v.Dispatch(nil, &gomemcached.MCRequest{
		Opcode:  gomemcached.GET,
		Key:     key,
		VBucket: v.vbid,
	})
}

func (v *vbucket) GetVBState() (res VBState) {
	v.Visit(func(vbLocked *vbucket) {
		res = vbLocked.state
	})
	return
}

func (v *vbucket) SetVBState(newState VBState,
	cb func(oldState VBState)) (oldState VBState) {
	v.Visit(func(vbLocked *vbucket) {
		oldState = vbLocked.state
		vbLocked.state = newState
		if cb != nil {
			cb(oldState)
		}
	})
	return
}

func (v *vbucket) AddStats(dest *Stats, key string) {
	v.Visit(func(vbLocked *vbucket) {
		if v.state == VBActive { // TODO: handle key
			dest.Add(&vbLocked.stats)
		}
	})
}

func (v *vbucket) Dispatch(w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	resch := make(chan *gomemcached.MCResponse, 1)
	v.ch <- vbreq{w, req, resch}
	return <-resch
}

func (v *vbucket) dispatch(w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	f := dispatchTable[req.Opcode]
	if f == nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.UNKNOWN_COMMAND,
			Body: []byte(fmt.Sprintf("Unknown command %v",
				req.Opcode)),
		}
	}

	if w != nil {
		v.stats.Ops++
	}
	res, msg := f(v, w, req)
	if msg != nil {
		v.observer.Submit(*msg)
	}
	return res
}

func (v *vbucket) Suspend() {
	v.Visit(func(vbLocked *vbucket) {
		vbLocked.suspended = true
	})
}

func (v *vbucket) Resume() {
	v.Visit(func(vbLocked *vbucket) {
		vbLocked.suspended = false
	})
}

func (v *vbucket) Visit(cb func(*vbucket)) {
	req := vbvisitreq{cb: cb, res: make(chan bool)}
	v.vch <- req
	<-req.res
}

func (v *vbucket) service() {
	for {
		select {
		case vr, ok := <-v.vch:
			if !ok {
				return
			}
			vr.cb(v)
			close(vr.res)
			if v.suspended {
				v.serviceSuspended()
			}

		case req := <-v.ch:
			res := v.dispatch(req.w, req.req)
			req.resch <- res
		}
	}

	// TODO: shall we unregister from bucket if we reach here?
}

func (v *vbucket) serviceSuspended() {
	for vr := range v.vch {
		vr.cb(v)
		close(vr.res)
		if !v.suspended {
			return
		}
	}
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

func vbSet(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (*gomemcached.MCResponse, *mutation) {
	v.stats.Sets++

	if rangeErr := v.checkRange(req); rangeErr != nil {
		return rangeErr, nil
	}

	old := v.items.Get(&item{key: req.Key})

	if req.Cas != 0 {
		var oldcas uint64
		if old != nil {
			oldcas = old.(*item).cas
		}

		if oldcas != req.Cas {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
			}, nil
		}
	}

	itemCas := v.cas
	v.cas++

	itemNew := &item{
		// TODO: Extras
		key:  req.Key,
		cas:  itemCas,
		data: req.Body,
	}

	v.stats.ValueBytesIncoming += uint64(len(req.Body))

	v.items.ReplaceOrInsert(itemNew)

	v.changes.ReplaceOrInsert(itemNew)
	if old != nil {
		v.changes.Delete(old)
		v.stats.Updates++
	} else {
		v.stats.Creates++
		v.stats.Items++
	}

	toBroadcast := &mutation{v.vbid, req.Key, itemCas, false}

	if req.Opcode.IsQuiet() {
		return nil, toBroadcast
	}

	return &gomemcached.MCResponse{
		Cas: itemCas,
	}, toBroadcast
}

func vbGet(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (*gomemcached.MCResponse, *mutation) {
	// Only update stats for requests that came from the "outside".
	if w != nil {
		v.stats.Gets++
	}

	if rangeErr := v.checkRange(req); rangeErr != nil {
		return rangeErr, nil
	}

	x := v.items.Get(&item{key: req.Key})
	if x == nil {
		if w != nil {
			v.stats.GetMisses++
		}
		if req.Opcode.IsQuiet() {
			return nil, nil
		}
		return &gomemcached.MCResponse{
			Status: gomemcached.KEY_ENOENT,
		}, nil
	}

	i := x.(*item)
	res := &gomemcached.MCResponse{
		Cas:    i.cas,
		Extras: make([]byte, 4),
		Body:   i.data,
	}
	// TODO: Extras!

	if w != nil {
		v.stats.ValueBytesOutgoing += uint64(len(i.data))
	}

	wantsKey := (req.Opcode == gomemcached.GETK || req.Opcode == gomemcached.GETKQ)
	if wantsKey {
		res.Key = req.Key
	}

	return res, nil
}

func vbDelete(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (*gomemcached.MCResponse, *mutation) {
	v.stats.Deletes++

	if rangeErr := v.checkRange(req); rangeErr != nil {
		return rangeErr, nil
	}

	t := &item{key: req.Key}
	x := v.items.Get(t)
	if x != nil {
		v.stats.Items--
		i := x.(*item)
		if req.Cas != 0 {
			if req.Cas != i.cas {
				return &gomemcached.MCResponse{
					Status: gomemcached.EINVAL,
				}, nil
			}
		}
	} else {
		if req.Opcode.IsQuiet() {
			return nil, nil
		}
		return &gomemcached.MCResponse{
			Status: gomemcached.KEY_ENOENT,
		}, nil
	}

	cas := v.cas
	v.cas++

	v.items.Delete(t)

	v.changes.ReplaceOrInsert(&item{
		key:  req.Key,
		cas:  cas,
		data: nil, // A nil data represents a delete mutation.
	})

	toBroadcast := &mutation{v.vbid, req.Key, cas, true}

	return &gomemcached.MCResponse{}, toBroadcast
}

// Responds with the changes since the req.Cas, with the last response
// in the response stream having no key.
// TODO: Support a limit on changes-since, perhaps in the req.Extras.
func vbChangesSince(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (res *gomemcached.MCResponse, m *mutation) {
	res = &gomemcached.MCResponse{
		Opcode: req.Opcode,
		Cas:    req.Cas,
	}

	ch, errs := transmitPackets(w)
	var err error

	visitor := func(x llrb.Item) bool {
		i := x.(*item)
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

	v.changes.AscendGreaterOrEqual(&item{cas: req.Cas}, visitor)
	close(ch)
	if err == nil {
		err = <-errs
	}
	if err != nil {
		log.Printf("Error sending changes-since: %v", err)
		res = &gomemcached.MCResponse{Fatal: true}
	}

	return
}

func vbGetConfig(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (*gomemcached.MCResponse, *mutation) {
	if v.config != nil {
		if j, err := json.Marshal(v.config); err == nil {
			return &gomemcached.MCResponse{Body: j}, nil
		}
	}
	return &gomemcached.MCResponse{Body: []byte("{}")}, nil
}

func vbSetConfig(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (*gomemcached.MCResponse, *mutation) {
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

func vbRGet(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (*gomemcached.MCResponse, *mutation) {
	// From http://code.google.com/p/memcached/wiki/RangeOps
	// Extras field  Bits
	// ------------------
	// End key len	 16
	// Reserved       8
	// Flags          8
	// Max results	 32

	// TODO: Extras.

	v.stats.RGets++

	res := &gomemcached.MCResponse{
		Opcode: req.Opcode,
		Cas:    req.Cas,
	}

	visitor := func(x llrb.Item) bool {
		i := x.(*item)
		if bytes.Compare(i.key, req.Key) >= 0 {
			err := (&gomemcached.MCResponse{
				Opcode: req.Opcode,
				Key:    i.key,
				Cas:    i.cas,
				Body:   i.data,
				// TODO: Extras.
			}).Transmit(w)
			if err != nil {
				log.Printf("Error sending RGET values: %v", err)
				res = &gomemcached.MCResponse{Fatal: true}
				return false
			}
			v.stats.RGetResults++
			v.stats.ValueBytesOutgoing += uint64(len(i.data))
		}
		return true
	}

	v.items.AscendGreaterOrEqual(&item{key: req.Key}, visitor)
	return res, nil
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

func vbSplitRange(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (*gomemcached.MCResponse, *mutation) {
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
		if split.VBucketId < 0 || split.VBucketId >= MAX_VBUCKET {
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
			vb.Visit(func(vbLocked *vbucket) {
				if vbLocked.state == VBDead {
					transferSplits(splitIdx + 1)
					if res.Status == gomemcached.SUCCESS {
						v.rangeCopyTo(vbLocked,
							splits[splitIdx].MinKeyInclusive,
							splits[splitIdx].MaxKeyExclusive)
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
		v.items = llrb.New(KeyLess)
		v.changes = llrb.New(CASLess)
		v.state = VBDead
		v.config = &VBConfig{}
	}
	return
}

func (v *vbucket) rangeCopyTo(dst *vbucket, minKeyInclusive []byte, maxKeyExclusive []byte) {
	dst.items = treeRangeCopy(v.items, llrb.New(KeyLess),
		v.items.Min(), // TODO: inefficient.
		minKeyInclusive,
		maxKeyExclusive)
	dst.changes = treeRangeCopy(v.changes, llrb.New(CASLess),
		v.changes.Min(), // TODO: inefficient.
		minKeyInclusive,
		maxKeyExclusive)
	dst.cas = v.cas
	dst.state = v.state
	dst.config = &VBConfig{
		MinKeyInclusive: minKeyInclusive,
		MaxKeyExclusive: maxKeyExclusive,
	}
}

func treeRangeCopy(src *llrb.Tree, dst *llrb.Tree, minItem llrb.Item,
	minKeyInclusive []byte, maxKeyExclusive []byte) *llrb.Tree {
	visitor := func(x llrb.Item) bool {
		i := x.(*item)
		if len(minKeyInclusive) > 0 &&
			bytes.Compare(i.key, minKeyInclusive) < 0 {
			return true
		}
		if len(maxKeyExclusive) > 0 &&
			bytes.Compare(i.key, maxKeyExclusive) >= 0 {
			return true
		}
		dst.ReplaceOrInsert(x)
		return true
	}
	src.AscendGreaterOrEqual(minItem, visitor)
	return dst
}
