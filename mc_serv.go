package main

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/server"
)

type reqHandler struct {
	currentBucket *bucket
}

type transmissible interface {
	Transmit(io.Writer) error
}

// How often to send opaque "heartbeats" on tap streams.
var tapTickFreq = time.Second

// Given an io.Writer, return a channel that can be fed things that
// can write themselves to an io.Writer and a channel that will return
// any encountered error.
//
// The user of this transmitter *MUST* close the input channel to
// indicate no more messages will be sent.
//
// There will be exactly one error written to the error channel either
// after successfully transmitting the entire stream (in which case it
// will be nil) or on any transmission error.
//
// Unless your transmissible stream is finite, it's recommended to
// perform a non-blocking receive of the error stream to check for
// brokenness so you know to stop transmitting.
func transmitPackets(w io.Writer) (chan<- transmissible, <-chan error) {
	ch := make(chan transmissible)
	errs := make(chan error, 1)
	go func() {
		for pkt := range ch {
			err := pkt.Transmit(w)
			if err != nil {
				errs <- err
				for _ = range ch {
					// Eat the input
				}
				return
			}
		}
		errs <- nil
	}()
	return ch, errs
}

func (rh *reqHandler) doTap(req *gomemcached.MCRequest,
	chpkt chan<- transmissible, cherr <-chan error) error {

	bch := make(chan interface{})
	mch := make(chan interface{}, 1000)

	rh.currentBucket.Subscribe(bch)
	defer rh.currentBucket.observer.Unregister(bch)

	ticker := time.NewTicker(tapTickFreq)
	defer ticker.Stop()

	// defer cleanup vbucket mchs
	registered := map[uint16]bool{}
	defer func() {
		for vbid := range registered {
			vb := rh.currentBucket.getVBucket(vbid)
			if vb != nil {
				vb.observer.Unregister(mch)
			}
		}
	}()

	for {
		select {
		case ci := <-bch:
			// Bucket state change.  Register
			c := ci.(bucketChange)
			vb := c.getVBucket()
			if c.newState == vbActive {
				vb.observer.Register(mch)
				registered[vb.vbid] = true
			} else if vb != nil {
				vb.observer.Unregister(mch)
				delete(registered, vb.vbid)
			}
		case mi := <-mch:
			// Send a change
			m := mi.(mutation)
			pkt := &gomemcached.MCRequest{
				Opcode:  gomemcached.TAP_MUTATION,
				Key:     m.key,
				VBucket: m.vb,
			}
			if m.deleted {
				pkt.Opcode = gomemcached.TAP_DELETE
				pkt.Extras = make([]byte, 8) // TODO: fill
			} else {
				pkt.Extras = make([]byte, 16) // TODO: fill

				vb := rh.currentBucket.getVBucket(m.vb)
				if vb != nil {
					i := vb.Get(m.key)
					if i == nil {
						log.Printf("Tapped a missing item, skipping: %s",
							m.key)
						continue
					}

					pkt.Body = i.data
				} else {
					log.Printf("Change on missing vbucket? %v",
						m.vb)
					continue
				}

			}
			chpkt <- pkt
		case <-ticker.C:
			// Send a noop
			chpkt <- &gomemcached.MCRequest{
				Opcode: gomemcached.TAP_OPAQUE,
				Extras: make([]byte, 8),
			}
		case err := <-cherr:
			return err
		}
	}
	panic("unreachable")
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
	case gomemcached.TAP_CONNECT:
		chpkt, cherr := transmitPackets(w)
		err := rh.doTap(req, chpkt, cherr)
		// Currently no way for this to return without failing
		log.Printf("Error tapping: %v", err)
		return &gomemcached.MCResponse{Fatal: true}
	case gomemcached.NOOP:
		return &gomemcached.MCResponse{}
	case gomemcached.STAT:
		err := doStats(rh.currentBucket, w, string(req.Key))
		if err != nil {
			log.Printf("Error sending stats: %v", err)
			return &gomemcached.MCResponse{Fatal: true}
		}
		return nil
	}

	vb := rh.currentBucket.getVBucket(req.VBucket)
	if vb == nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.NOT_MY_VBUCKET,
		}
	}

	return vb.dispatch(w, req)
}

func sessionLoop(s io.ReadWriteCloser, addr string, handler *reqHandler) {
	log.Printf("Started session with %v", addr)
	defer func() {
		log.Printf("Finished session with %v", addr)
	}()
	memcached.HandleIO(s, handler)
}

func waitForConnections(ls net.Listener, defaultBucket *bucket) {
	for {
		s, e := ls.Accept()
		if e == nil {
			log.Printf("Got a connection from %v", s.RemoteAddr())
			handler := &reqHandler{
				currentBucket: defaultBucket,
			}
			go sessionLoop(s, s.RemoteAddr().String(), handler)
		} else {
			log.Printf("Error accepting from %s: %v", ls, e)
			// TODO:  Figure out if this is recoverable.
			// It probably is most of the time, but not during tests.
			return
		}
	}
}

func startMCServer(addr string, defaultBucket *bucket) (net.Listener, error) {
	ls, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	go waitForConnections(ls, defaultBucket)
	return ls, nil
}
