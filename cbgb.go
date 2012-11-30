package main

import (
	"flag"
	"io"
	"log"
	"net"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/server"
)

const (
	VERSION     = "0.0.0"
	MAX_VBUCKET = 1024
)

var mutationLogCh = make(chan mutation)

var serverStart = time.Now()

type reqHandler struct {
	currentBucket *bucket
}

var notMyVbucket = &gomemcached.MCResponse{
	Status: gomemcached.NOT_MY_VBUCKET,
}

type statItem struct {
	key, val string
}

// This is slightly more complicated than it would generally need to
// be, but as a generator, it's self-terminating based on an input
// stream.  I may do this a bit differently for stats in the future,
// but the model is quite helpful for a tap stream or similar.
func transmitStats(w io.Writer) (chan<- statItem, <-chan error) {
	ch := make(chan statItem)
	errs := make(chan error)
	go func() {
		for res := range ch {
			err := (&gomemcached.MCResponse{
				Opcode: gomemcached.STAT,
				Key:    []byte(res.key),
				Body:   []byte(res.val),
			}).Transmit(w)
			if err != nil {
				for _ = range ch {
					// Eat the input
				}
				errs <- err
				return
			}
		}
		errs <- (&gomemcached.MCResponse{Opcode: gomemcached.STAT}).Transmit(w)
	}()
	return ch, errs
}

func doStats(w io.Writer, key string) error {
	log.Printf("Doing stats for %#v", key)
	ch, errs := transmitStats(w)
	ch <- statItem{"uptime", time.Since(serverStart).String()}
	ch <- statItem{"version", VERSION}
	close(ch)
	return <-errs
}

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
	case gomemcached.FLUSH:
		panic("OMG")
	case gomemcached.STAT:
		err := doStats(w, string(req.Key))
		if err != nil {
			log.Printf("Error sending stats: %v", err)
			return &gomemcached.MCResponse{Fatal: true}
		}
		return nil
	case gomemcached.SASL_AUTH:
		panic("OMG need to implement SASL_AUTH")
	}

	if rh.currentBucket == nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
			Body:   []byte("No current bucket; please SASL_AUTH first"),
		}
	}

	vb := rh.currentBucket.getVBucket(req.VBucket)
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

func waitForConnections(ls net.Listener, defaultBucket *bucket) {
	handler := &reqHandler{
		currentBucket: defaultBucket,
	}

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

	defaultBucket := bucket{}
	defaultBucket.createVBucket(0).observer.Register(mutationLogCh)

	ls, e := net.Listen("tcp", *addr)
	if e != nil {
		log.Fatalf("Got an error:  %s", e)
	}

	waitForConnections(ls, &defaultBucket)
}
