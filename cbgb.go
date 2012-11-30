package main

import (
	"flag"
	"io"
	"log"
	"net"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/server"
)

const (
	VERSION     = "0.0.0"
	MAX_VBUCKET = 1024
)

// TODO:  Have more than one of these
var defaultBucket bucket

var mutationLogCh = make(chan mutation)

type reqHandler struct {
}

var notMyVbucket = &gomemcached.MCResponse{
	Status: gomemcached.NOT_MY_VBUCKET,
}

// TODO:  Make this work with stats and other multi-response things
func (rh *reqHandler) HandleMessage(w io.Writer, req *gomemcached.MCRequest) *gomemcached.MCResponse {

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

	vb := defaultBucket.getVBucket(req.VBucket)
	if vb == nil {
		return notMyVbucket
	}

	return vb.dispatch(w, req)
}

func sessionLoop(s net.Conn, handler *reqHandler) {
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
			go sessionLoop(s, handler)
		} else {
			log.Printf("Error accepting from %s", ls)
		}
	}
}

func mutationLogger() {
	for m := range mutationLogCh {
		sym := "M"
		if m.deleted {
			sym = "D"
		}
		log.Printf("%v: %s -> %v", sym, m.key, m.cas)
	}
}

func main() {
	addr := flag.String("bind", ":11211", "memcached listen port")

	flag.Parse()

	go mutationLogger()

	defaultBucket.createVBucket(0).observer.Register(mutationLogCh)

	ls, e := net.Listen("tcp", *addr)
	if e != nil {
		log.Fatalf("Got an error:  %s", e)
	}

	waitForConnections(ls)
}
