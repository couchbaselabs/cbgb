package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/dustin/gomemcached"
	"github.com/petar/GoLLRB/llrb"
)

const (
	CHANGES_SINCE = gomemcached.CommandCode(0x60)
)

type vbState uint8

const (
	_ = vbState(iota)
	vbActive
	vbReplica
	vbPending
	vbDead
)

var vbStateNames = []string{
	vbActive:  "active",
	vbReplica: "replica",
	vbPending: "pending",
	vbDead:    "dead",
}

func (v vbState) String() string {
	if v < vbActive || v > vbDead {
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

type vbucket struct {
	items    *llrb.Tree
	changes  *llrb.Tree
	cas      uint64
	observer *broadcaster
	vbid     uint16
	state    vbState
	lock     sync.Mutex
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

type dispatchFun func(v *vbucket, w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse

var dispatchTable = [256]dispatchFun{
	gomemcached.GET:   vbGet,
	gomemcached.GETK:  vbGet,
	gomemcached.GETQ:  vbGet,
	gomemcached.GETKQ: vbGet,

	gomemcached.SET:  vbSet,
	gomemcached.SETQ: vbSet,

	gomemcached.DELETE:  vbDel,
	gomemcached.DELETEQ: vbDel,

	// TODO: Move CHANGES_SINCE to gomemcached one day.
	CHANGES_SINCE: vbChangesSince,
}

func newVbucket(vbid uint16) *vbucket {
	return &vbucket{
		items:    llrb.New(KeyLess),
		changes:  llrb.New(CASLess),
		observer: newBroadcaster(dataBroadcastBufLen),
		vbid:     vbid,
		state:    vbDead,
	}
}

func (v *vbucket) Close() error {
	return v.observer.Close()
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

	v.lock.Lock()
	defer v.lock.Unlock()
	return f(v, w, req)
}

func vbSet(v *vbucket, w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	old := v.items.Get(&item{key: req.Key})

	if req.Cas != 0 {
		var oldcas uint64
		if old != nil {
			oldcas = old.(*item).cas
		}

		if oldcas != req.Cas {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
			}
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

	v.items.ReplaceOrInsert(itemNew)

	v.changes.ReplaceOrInsert(itemNew)
	if old != nil {
		v.changes.Delete(old)
	}

	v.observer.Submit(mutation{v.vbid, req.Key, itemCas, false})

	if req.Opcode.IsQuiet() {
		return nil
	}

	return &gomemcached.MCResponse{
		Cas: itemCas,
	}
}

func vbGet(v *vbucket, w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	x := v.items.Get(&item{key: req.Key})
	if x == nil {
		if req.Opcode.IsQuiet() {
			return nil
		}
		return &gomemcached.MCResponse{
			Status: gomemcached.KEY_ENOENT,
		}
	}

	i := x.(*item)
	res := &gomemcached.MCResponse{
		Cas:    i.cas,
		Extras: make([]byte, 4),
		Body:   i.data,
	}
	// TODO: Extras!

	wantsKey := (req.Opcode == gomemcached.GETK || req.Opcode == gomemcached.GETKQ)
	if wantsKey {
		res.Key = req.Key
	}

	return res
}

func vbDel(v *vbucket, w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	t := &item{key: req.Key}
	x := v.items.Get(t)
	if x != nil {
		i := x.(*item)
		if req.Cas != 0 {
			if req.Cas != i.cas {
				return &gomemcached.MCResponse{
					Status: gomemcached.EINVAL,
				}
			}
		}
	} else {
		if req.Opcode.IsQuiet() {
			return nil
		}
		return &gomemcached.MCResponse{
			Status: gomemcached.KEY_ENOENT,
		}
	}

	cas := v.cas
	v.cas++

	v.items.Delete(t)

	v.changes.ReplaceOrInsert(&item{
		key:  req.Key,
		cas:  cas,
		data: nil, // A nil data represents a delete mutation.
	})

	v.observer.broadcast(mutation{v.vbid, req.Key, cas, true})

	return &gomemcached.MCResponse{}
}

// Responds with the changes since the req.Cas, with the last response
// in the response stream having no key.
// TODO: Support a limit on changes-since, perhaps in the req.Extras.
func vbChangesSince(v *vbucket, w io.Writer,
	req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	res = &gomemcached.MCResponse{
		Opcode: req.Opcode,
		Cas:    req.Cas,
	}

	visitor := func(x llrb.Item) bool {
		i := x.(*item)
		if i.cas > req.Cas {
			err := (&gomemcached.MCResponse{
				Opcode: req.Opcode,
				Key:    i.key,
				Cas:    i.cas,
				// TODO: Extras.
				// TODO: Should changes-since respond with item value?
			}).Transmit(w)
			if err != nil {
				log.Printf("Error sending changes-since: %v", err)
				res = &gomemcached.MCResponse{Fatal: true}
				return false
			}
		}
		return true
	}

	v.changes.AscendGreaterOrEqual(&item{cas: req.Cas}, visitor)
	return
}
