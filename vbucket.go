package cbgb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/dustin/gomemcached"
)

const (
	CHANGES_SINCE        = gomemcached.CommandCode(0x60)
	GET_VBMETA           = gomemcached.CommandCode(0x61)
	SET_VBMETA           = gomemcached.CommandCode(0x62)
	SPLIT_RANGE          = gomemcached.CommandCode(0x63)
	NOT_MY_RANGE         = gomemcached.Status(0x60)
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

type vbucket struct {
	parent   Bucket
	vbid     uint16
	meta     unsafe.Pointer // *VBMeta
	bs       *bucketstore
	ps       *partitionstore
	ach      chan *funreq // To access top-level vbucket fields & stats.
	mch      chan *funreq // To mutate the keys/changes collections.
	stats    Stats
	observer *broadcaster
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

func newVBucket(parent Bucket, vbid uint16, bs *bucketstore) (rv *vbucket, err error) {
	rv = &vbucket{
		parent:   parent,
		vbid:     vbid,
		meta:     unsafe.Pointer(&VBMeta{Id: vbid, State: VBDead.String()}),
		bs:       bs,
		ps:       bs.getPartitionStore(vbid),
		ach:      make(chan *funreq),
		mch:      make(chan *funreq),
		observer: newBroadcaster(observerBroadcastMax),
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
	if err = v.ps.set(i, nil, 0); err != nil {
		return err
	}
	if err = v.bs.coll(COLL_VBMETA).Set(k, j); err != nil {
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

			// TODO: Need to update v.stats.Items.
			// TODO: What if we're loading something out of allowed range?
		})
	})

	return err
}

func (v *vbucket) AddStatsTo(dest *Stats, key string) {
	v.Apply(func() { // Need apply() protection due to stats.Items.
		if parseVBState(v.Meta().State) == VBActive { // TODO: handle key
			dest.Add(&v.stats)
		}
	})
}

func (v *vbucket) checkRange(req *gomemcached.MCRequest) *gomemcached.MCResponse {
	if len(req.Key) > MAX_ITEM_KEY_LENGTH {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
			Body:   []byte(fmt.Sprintf("key length too long: %v", len(req.Key))),
		}
	}

	meta := v.Meta()
	if meta.KeyRange != nil {
		if len(meta.KeyRange.MinKeyInclusive) > 0 &&
			bytes.Compare(req.Key, meta.KeyRange.MinKeyInclusive) < 0 {
			atomic.AddUint64(&v.stats.NotMyRangeErrors, 1)
			return &gomemcached.MCResponse{Status: NOT_MY_RANGE}
		}
		if len(meta.KeyRange.MaxKeyExclusive) > 0 &&
			bytes.Compare(req.Key, meta.KeyRange.MaxKeyExclusive) >= 0 {
			atomic.AddUint64(&v.stats.NotMyRangeErrors, 1)
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

	if len(req.Body) > MAX_ITEM_DATA_LENGTH {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
			Body: []byte(fmt.Sprintf("data too big: %v, key: %v",
				len(req.Body), req.Key)),
		}
	}

	var itemCas uint64
	v.Apply(func() {
		// TODO: We have the apply to avoid races, but is there a better way?
		itemCas = atomic.AddUint64(&v.Meta().LastCas, 1)
	})

	var prevMeta *item
	var err error

	v.Mutate(func() {
		prevMeta, err = v.ps.getMeta(req.Key)
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

		var flag, exp uint32
		if req.Extras != nil {
			flag = binary.BigEndian.Uint32(req.Extras)
			exp = binary.BigEndian.Uint32(req.Extras[4:])
		}

		itemNew := &item{
			key:  req.Key,
			flag: flag,
			exp:  exp, // TODO: Handle expirations.
			cas:  itemCas,
			data: req.Body,
		}

		err = v.ps.set(itemNew, prevMeta,
			atomic.LoadInt64(&v.stats.Items)+1)
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
		atomic.AddUint64(&v.stats.StoreErrors, 1)
	} else {
		if prevMeta != nil {
			atomic.AddUint64(&v.stats.Updates, 1)
		} else {
			atomic.AddUint64(&v.stats.Creates, 1)
			atomic.AddInt64(&v.stats.Items, 1)
		}
		atomic.AddUint64(&v.stats.IncomingValueBytes, uint64(len(req.Body)))
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

	i, err := v.ps.get(req.Key)
	if err != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.TMPFAIL,
			Body:   []byte(fmt.Sprintf("Store get error %v", err)),
		}
	}

	if i == nil {
		atomic.AddUint64(&v.stats.GetMisses, 1)
	} else {
		atomic.AddUint64(&v.stats.OutgoingValueBytes, uint64(len(i.data)))
	}

	if i == nil {
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
		prevMeta, err = v.ps.getMeta(req.Key)
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

		err = v.ps.del(req.Key, cas, prevMeta,
			atomic.LoadInt64(&v.stats.Items)-1)
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
		atomic.AddUint64(&v.stats.StoreErrors, 1)
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

	errVisit := v.ps.visitChanges(casBytes(req.Cas), true, visitor)

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

					if err := v.bs.flush(); err != nil {
						res = &gomemcached.MCResponse{
							Status: gomemcached.TMPFAIL,
							Body:   []byte(fmt.Sprintf("setVBMeta flush error %v", err)),
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

	res = &gomemcached.MCResponse{
		Opcode: req.Opcode,
		Cas:    req.Cas,
	}

	extras := make([]byte, 4)

	visitRGetResults := uint64(0)
	visitOutgoingValueBytes := uint64(0)

	visitor := func(i *item) bool {
		if bytes.Compare(i.key, req.Key) >= 0 {
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
			visitOutgoingValueBytes += uint64(len(i.data))
		}
		return true
	}

	if err := v.ps.visitItems(req.Key, true, visitor); err != nil {
		res = &gomemcached.MCResponse{Fatal: true}
	}

	atomic.AddUint64(&v.stats.RGets, 1)
	atomic.AddUint64(&v.stats.RGetResults, visitRGetResults)
	atomic.AddUint64(&v.stats.OutgoingValueBytes, visitOutgoingValueBytes)

	return res
}
