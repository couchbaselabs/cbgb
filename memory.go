package main

import (
	"fmt"
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
	lock     sync.Mutex
}

var notFound = &gomemcached.MCResponse{
	Status: gomemcached.KEY_ENOENT,
}

var eInval = &gomemcached.MCResponse{
	Status: gomemcached.EINVAL,
}

var emptyResponse = &gomemcached.MCResponse{}

type dispatchFun func(v *vbucket, req *gomemcached.MCRequest) *gomemcached.MCResponse

var dispatchTable = [256]dispatchFun{
	gomemcached.GET:   vbGet,
	gomemcached.GETK:  vbGet,
	gomemcached.GETQ:  vbGet,
	gomemcached.GETKQ: vbGet,

	gomemcached.SET: vbSet,

	gomemcached.DELETE:  vbDel,
	gomemcached.DELETEQ: vbDel,
}

func newVbucket() *vbucket {
	return &vbucket{
		data:     make(map[string]*item),
		observer: newBroadcaster(),
	}
}

func (v *vbucket) dispatch(req *gomemcached.MCRequest) *gomemcached.MCResponse {
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
	return f(v, req)
}

func vbSet(v *vbucket, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	// TODO: CAS
	if req.Cas != 0 {
		return eInval
	}

	itemCas := v.cas
	v.cas++

	v.data[string(req.Key)] = &item{
		// TODO: Extras
		cas:  itemCas,
		data: req.Body,
	}

	v.observer.broadcast(mutation{req.Key, itemCas, false})

	if req.Opcode.IsQuiet() {
		return nil
	}

	return &gomemcached.MCResponse{
		Cas: itemCas,
	}
}

func vbGet(v *vbucket, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	data, ok := v.data[string(req.Key)]
	if !ok {
		if req.Opcode.IsQuiet() {
			return nil
		}
		return notFound
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

func vbDel(v *vbucket, req *gomemcached.MCRequest) *gomemcached.MCResponse {
	k := string(req.Key)
	_, ok := v.data[k]
	if !ok {
		if req.Opcode.IsQuiet() {
			return nil
		}
		return notFound
	}
	delete(v.data, k)

	v.observer.broadcast(mutation{req.Key, 0, true})

	return emptyResponse
}
