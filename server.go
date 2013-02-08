package cbgb

import (
	"io"
	"log"
	"net"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/server"
)

const (
	VERSION = "0.0.0"
)

type reqHandler struct {
	currentBucket Bucket
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
		err := doTap(rh.currentBucket, req, chpkt, cherr)
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

	vb := rh.currentBucket.GetVBucket(req.VBucket)
	if vb == nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.NOT_MY_VBUCKET,
		}
	}

	return vb.Dispatch(w, req)
}

func sessionLoop(s io.ReadWriteCloser, addr string, handler *reqHandler) {
	log.Printf("Started session with %v", addr)
	defer func() {
		log.Printf("Finished session with %v", addr)
	}()
	memcached.HandleIO(s, handler)
}

func waitForConnections(ls net.Listener, buckets *Buckets, defaultBucketName string) {
	for {
		s, e := ls.Accept()
		if e == nil {
			log.Printf("Got a connection from %v", s.RemoteAddr())
			bucket := buckets.Get(defaultBucketName)
			handler := &reqHandler{
				currentBucket: bucket,
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

func StartServer(addr string, buckets *Buckets, defaultBucketName string) (net.Listener, error) {
	ls, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	go waitForConnections(ls, buckets, defaultBucketName)
	return ls, nil
}
