package main

import (
	"flag"
	"log"
	"net"
	"sync/atomic"
	"unsafe"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/server"
)

const VERSION = "0.0.0"

const MAX_VBUCKET = 1024

// TODO:  Make a collection of vbuckets
var vbuckets [MAX_VBUCKET]unsafe.Pointer

type reqHandler struct {
}

var notMyVbucket = &gomemcached.MCResponse{
	Status: gomemcached.NOT_MY_VBUCKET,
}

func getVBucket(vbid uint16) *vbucket {
	vbp := atomic.LoadPointer(&vbuckets[vbid])
	return (*vbucket)(vbp)
}

func setVBucket(vbid uint16, vb *vbucket) {
	atomic.StorePointer(&vbuckets[vbid], unsafe.Pointer(vb))
}

// TODO:  Make this work with stats and other multi-response things
func (rh *reqHandler) HandleMessage(req *gomemcached.MCRequest) *gomemcached.MCResponse {

	switch req.Opcode {
	case gomemcached.QUIT:
		return &gomemcached.MCResponse{
			Fatal: true,
		}
	case gomemcached.VERSION:
		return &gomemcached.MCResponse{
			Body: []byte(VERSION),
		}
	case gomemcached.NOOP:
		return emptyResponse
	case gomemcached.FLUSH, gomemcached.STAT:
		panic("OMG")
	}

	vb := getVBucket(req.VBucket)
	if vb == nil {
		return notMyVbucket
	}

	return vb.dispatch(req)
}

func sessionSomeCrap(s net.Conn, handler *reqHandler) {
	log.Printf("Started session with %v", s.RemoteAddr())
	defer func() {
		log.Printf("Finished session with %v", s.RemoteAddr())
	}()
	memcached.HandleIO(s, handler)
}

func waitForConnections(ls net.Listener) {
	handler := &reqHandler{}

	for {
		s, e := ls.Accept()
		if e == nil {
			log.Printf("Got a connection from %v", s.RemoteAddr())
			go sessionSomeCrap(s, handler)
		} else {
			log.Printf("Error accepting from %s", ls)
		}
	}
}

func main() {
	addr := flag.String("bind", ":11211", "memcached listen port")

	flag.Parse()

	setVBucket(0, newVbucket())

	ls, e := net.Listen("tcp", *addr)
	if e != nil {
		log.Fatalf("Got an error:  %s", e)
	}

	waitForConnections(ls)
}
