package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dustin/go-broadcast"
	"github.com/dustin/gomemcached"
)

const (
	CHANGES_SINCE        = gomemcached.CommandCode(0x60)
	GET_VBMETA           = gomemcached.CommandCode(0x61)
	SET_VBMETA           = gomemcached.CommandCode(0x62)
	COLL_SUFFIX_KEYS     = ".k" // This suffix sorts before CHANGES suffix.
	COLL_SUFFIX_CHANGES  = ".s" // The changes is like a "sequence" stream.
	COLL_VBMETA          = "vbm"
	MAX_VBID             = 0x0000ffff // Due to uint16.
	MAX_ITEM_KEY_LENGTH  = 250
	MAX_ITEM_DATA_LENGTH = 1024 * 1024
	MAX_ITEM_EXP         = 0x7fffffff
	DELETION_EXP         = 0x80000000 // Deletion sentinel exp.
	DELETION_FLAG        = 0xffffffff // Deletion sentinel flag.
)

var ignore = errors.New("not-an-error/sentinel")

func VBucketIdForKey(key []byte, numVBuckets int) uint16 {
	return uint16((crc32.ChecksumIEEE(key) >> uint32(16)) & uint32(numVBuckets-1))
}

type VBucket struct {
	meta     unsafe.Pointer // *VBMeta
	parent   Bucket
	bs       *bucketstore
	ps       *partitionstore
	lock     sync.Mutex
	observer broadcast.Broadcaster

	bucketItemBytes *int64
	staleness       int64 // To track view freshness.

	stats BucketStats

	viewsStore *bucketstore
	viewsLock  sync.Mutex

	available chan bool
	vbid      uint16
}

func (v VBucket) String() string {
	return fmt.Sprintf("{vbucket %v}", v.vbid)
}

type dispatchFun func(v *VBucket, w io.Writer,
	req *gomemcached.MCRequest) *gomemcached.MCResponse

var dispatchTable = [256]dispatchFun{
	gomemcached.GET:   vbGet,
	gomemcached.GETK:  vbGet,
	gomemcached.GETQ:  vbGet,
	gomemcached.GETKQ: vbGet,

	gomemcached.SET:  vbMutate,
	gomemcached.SETQ: vbMutate,

	gomemcached.DELETE:  vbDelete,
	gomemcached.DELETEQ: vbDelete,

	gomemcached.ADD:      vbMutate,
	gomemcached.ADDQ:     vbMutate,
	gomemcached.REPLACE:  vbMutate,
	gomemcached.REPLACEQ: vbMutate,
	gomemcached.APPEND:   vbMutate,
	gomemcached.APPENDQ:  vbMutate,
	gomemcached.PREPEND:  vbMutate,
	gomemcached.PREPENDQ: vbMutate,

	gomemcached.INCREMENT:  vbMutate,
	gomemcached.INCREMENTQ: vbMutate,
	gomemcached.DECREMENT:  vbMutate,
	gomemcached.DECREMENTQ: vbMutate,

	gomemcached.RGET: vbRGet,

	// TODO: Replace CHANGES_SINCE with enhanced TAP.
	CHANGES_SINCE: vbChangesSince,

	// TODO: Move new command codes to gomemcached one day.
	GET_VBMETA: vbGetVBMeta,
	SET_VBMETA: vbSetVBMeta,
}

func newVBucket(parent Bucket, vbid uint16, bs *bucketstore,
	bucketItemBytes *int64) (rv *VBucket, err error) {
	rv = &VBucket{
		parent:          parent,
		vbid:            vbid,
		meta:            unsafe.Pointer(&VBMeta{Id: vbid, State: VBDead.String()}),
		bs:              bs,
		ps:              bs.getPartitionStore(vbid),
		observer:        broadcastMux.Sub(),
		available:       make(chan bool),
		bucketItemBytes: bucketItemBytes,
	}

	return rv, nil
}

func (v *VBucket) Meta() *VBMeta {
	return (*VBMeta)(atomic.LoadPointer(&v.meta))
}

func (v *VBucket) Close() error {
	if v == nil {
		return nil
	}
	close(v.available)
	return v.observer.Close()
}

func (v *VBucket) Dispatch(w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	atomic.AddInt64(&v.stats.Ops, 1)
	f := dispatchTable[req.Opcode]
	if f == nil {
		atomic.AddInt64(&v.stats.Unknowns, 1)
		return &gomemcached.MCResponse{
			Status: gomemcached.UNKNOWN_COMMAND,
			Body:   []byte(fmt.Sprintf("Unknown command %v", req.Opcode)),
		}
	}
	return f(v, w, req)
}

func (v *VBucket) get(key []byte) *gomemcached.MCResponse {
	return v.Dispatch(nil, &gomemcached.MCRequest{
		Opcode:  gomemcached.GET,
		Key:     key,
		VBucket: v.vbid,
	})
}

func (v *VBucket) Apply(fun func()) {
	v.lock.Lock()
	defer v.lock.Unlock()
	fun()
}

func (v *VBucket) GetVBState() (res VBState) {
	return parseVBState(v.Meta().State)
}

func (v *VBucket) SetVBState(newState VBState,
	cb func(prevState VBState)) (prevState VBState, err error) {
	prevState = VBDead
	// The bs.apply() ensures we're not compacting/flushing while
	// changing vbstate, which is good for atomicity and to avoid
	// deadlock when the compactor wants to swap collections.
	v.bs.apply(func() {
		v.Apply(func() {
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
	return prevState, err
}

func (v *VBucket) setVBMeta(newMeta *VBMeta) (err error) {
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

	deltaItemBytes, err := v.ps.set(i, nil)
	if err != nil {
		return err
	}
	if err = v.bs.collMeta(COLL_VBMETA).Set(k, j); err != nil {
		return err
	}
	atomic.StorePointer(&v.meta, unsafe.Pointer(newMeta))

	atomic.AddInt64(&v.stats.ItemBytes, deltaItemBytes)
	atomic.AddInt64(v.bucketItemBytes, deltaItemBytes)

	return nil
}

func (v *VBucket) load() (err error) {
	v.Apply(func() {
		meta := v.Meta().Copy()

		x, err := v.bs.collMeta(COLL_VBMETA).GetItem(
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

		_, changes := v.ps.colls()
		i, err := changes.MaxItem(true)
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

		numItems, numItemBytes, err := v.ps.getTotals()
		if err == nil {
			atomic.StoreInt64(&v.stats.Items, int64(numItems))
			atomic.StoreInt64(&v.stats.ItemBytes, int64(numItemBytes))
			atomic.AddInt64(v.bucketItemBytes, int64(numItemBytes))
		}

		// TODO: What if we're loading something out of allowed range?
	})

	return err
}

func (v *VBucket) AddStatsTo(dest *BucketStats, key string) {
	if parseVBState(v.Meta().State) == VBActive { // TODO: handle stats sub-key.
		dest.Add(&v.stats)
	}
}

func vbGet(v *VBucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	atomic.AddInt64(&v.stats.Gets, 1)

	i, err := v.getUnexpired(req.Key, time.Now())
	if err != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.TMPFAIL,
			Body:   []byte(fmt.Sprintf("Store get error %v", err)),
		}
	}
	if i == nil {
		atomic.AddInt64(&v.stats.GetMisses, 1)
		if req.Opcode.IsQuiet() {
			return nil
		}
		return &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT}
	}

	res = &gomemcached.MCResponse{
		Cas:    i.cas,
		Extras: make([]byte, 4),
		Body:   i.data,
	}
	binary.BigEndian.PutUint32(res.Extras, i.flag)
	wantsKey := (req.Opcode == gomemcached.GETK || req.Opcode == gomemcached.GETKQ)
	if wantsKey {
		res.Key = req.Key
	}

	atomic.AddInt64(&v.stats.OutgoingValueBytes, int64(len(i.data)))

	return res
}

// Responds with the changes since the req.Cas, with the last response
// in the response stream having no key.
// TODO: Support a limit on changes-since, perhaps in the req.Extras.
func vbChangesSince(v *VBucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
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

	errVisit := v.ps.visitChanges(casBytes(req.Cas), true, visitor)

	close(ch)

	if errVisit != nil {
		return &gomemcached.MCResponse{Fatal: true}
	}

	if err == nil {
		err = <-errs
	}
	if err != nil {
		return &gomemcached.MCResponse{Fatal: true}
	}

	// TODO: Update stats.

	return res
}

func vbGetVBMeta(v *VBucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	v.Apply(func() {
		if j, err := json.Marshal(v.Meta()); err == nil {
			res = &gomemcached.MCResponse{Body: j}
		} else {
			res = &gomemcached.MCResponse{Body: []byte("{}")}
		}
	})
	return res
}

// TODO: Don't allow clients to change vb state/meta for initial cbgb
// version, until (perhaps) one day when we have auth checking.
var allow_vbSetVBMeta bool = false

func vbSetVBMeta(v *VBucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	res = &gomemcached.MCResponse{Status: gomemcached.EINVAL}
	if !allow_vbSetVBMeta {
		return
	}
	if req.Body == nil {
		return
	}
	newMeta := &VBMeta{}
	if err := json.Unmarshal(req.Body, newMeta); err != nil {
		// XXX:  The test requires that we don't report what's wrong
		// res.Body = []byte(err.Error())
		return
	}

	// The bs.apply() ensures we're not compacting/flushing while
	// changing vbstate, which is good for atomicity and to avoid
	// deadlock when the compactor wants to swap collections.
	v.bs.apply(func() {
		v.Apply(func() {
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

			// TODO: Unlike couchbase, we flush before returning
			// to client; which might not be what some management
			// use cases want (ability to switch vbstate even if
			// dirty queues are huge).
			if _, err := v.bs.flush_unlocked(); err != nil {
				res = &gomemcached.MCResponse{
					Status: gomemcached.TMPFAIL,
					Body:   []byte(fmt.Sprintf("setVBMeta flush error %v", err)),
				}
				return
			}

			res = &gomemcached.MCResponse{}
		})
	})
	return res
}

func vbRGet(v *VBucket, w io.Writer, req *gomemcached.MCRequest) (
	res *gomemcached.MCResponse) {
	// From http://code.google.com/p/memcached/wiki/RangeOps
	// Extras field  Bits
	// ------------------
	// End key len	 16
	// Reserved       8
	// Flags          8
	// Max results	 32

	res = &gomemcached.MCResponse{
		Opcode: req.Opcode,
		Cas:    req.Cas,
	}

	extras := make([]byte, 4)

	visitRGetResults := int64(0)
	visitOutgoingValueBytes := int64(0)

	visitor := func(i *item) bool {
		if bytes.Compare(i.key, req.Key) >= 0 {
			// TODO: Need to hide expired items from range scan.
			binary.BigEndian.PutUint32(extras, i.flag)
			r := gomemcached.MCResponse{
				Opcode: req.Opcode,
				Key:    i.key,
				Cas:    i.cas,
				Extras: extras,
				Body:   i.data,
			}
			err := r.Transmit(w)
			if err != nil {
				res = &gomemcached.MCResponse{Fatal: true}
				return false
			}
			visitRGetResults++
			visitOutgoingValueBytes += int64(len(i.data))
		}
		return true
	}

	if err := v.ps.visitItems(req.Key, true, visitor); err != nil {
		res = &gomemcached.MCResponse{Fatal: true}
	}

	atomic.AddInt64(&v.stats.RGets, 1)
	atomic.AddInt64(&v.stats.RGetResults, visitRGetResults)
	atomic.AddInt64(&v.stats.OutgoingValueBytes, visitOutgoingValueBytes)

	return res
}

func (v *VBucket) Visit(start []byte, visitor func(key []byte, data []byte) bool) error {
	return v.ps.visitItems(start, true, func(i *item) bool {
		return visitor(i.key, i.data)
	})
}
