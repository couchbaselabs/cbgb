package main

import (
	"fmt"
	"io"
	"sync"

	"github.com/dustin/gomemcached"
)

type item struct {
	exp, flag uint32
	cas       uint64
	data      []byte
}

type vbucket struct {
	data     map[string]*item
	cas      uint64
	observer *broadcaster
	vbid     uint16
	lock     sync.Mutex
}

type mutation struct {
	key     []byte
	cas     uint64
	deleted bool
}

func (m mutation) String() string {
	sym := "M"
	if m.deleted {
		sym = "D"
	}
	return fmt.Sprintf("%v: %s -> %v", sym, m.key, m.cas)
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
}

func newVbucket(vbid uint16) *vbucket {
	return &vbucket{
		data:     make(map[string]*item),
		observer: newBroadcaster(dataBroadcastBufLen),
		vbid:     vbid,
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
	if req.Cas != 0 {
		var oldcas uint64
		if old, ok := v.data[string(req.Key)]; ok {
			oldcas = old.cas
		}

		if oldcas != req.Cas {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
			}
		}
	}

	itemCas := v.cas
	v.cas++

	v.data[string(req.Key)] = &item{
		// TODO: Extras
		cas:  itemCas,
		data: req.Body,
	}

	v.observer.Submit(mutation{req.Key, itemCas, false})

	if req.Opcode.IsQuiet() {
		return nil
	}

	return &gomemcached.MCResponse{
		Cas: itemCas,
	}
}

func vbGet(v *vbucket, w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	data, ok := v.data[string(req.Key)]
	if !ok {
		if req.Opcode.IsQuiet() {
			return nil
		}
		return &gomemcached.MCResponse{
			Status: gomemcached.KEY_ENOENT,
		}
	}

	res := &gomemcached.MCResponse{
		Cas:    data.cas,
		Extras: make([]byte, 4),
		Body:   data.data,
	}
	// TODO:  Extras!

	wantsKey := (req.Opcode == gomemcached.GETK || req.Opcode == gomemcached.GETKQ)
	if wantsKey {
		res.Key = req.Key
	}

	return res
}

func vbDel(v *vbucket, w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {

	k := string(req.Key)
	i, ok := v.data[k]
	if ok {
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
	delete(v.data, k)

	v.observer.broadcast(mutation{req.Key, 0, true})

	return &gomemcached.MCResponse{}
}
