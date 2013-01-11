package cbgb

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
)

type transmissible interface {
	Transmit(io.Writer) error
}

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

type Bytes []byte

func (a *Bytes) MarshalJSON() ([]byte, error) {
	s := url.QueryEscape(string(*a))
	return json.Marshal(s)
}

func (a *Bytes) UnmarshalJSON(d []byte) error {
	var s string
	err := json.Unmarshal(d, &s)
	if err != nil {
		return err
	}
	x, err := url.QueryUnescape(s)
	if err == nil {
		*a = Bytes(x)
	}
	return err
}

func (a *Bytes) String() string {
	return string(*a)
}

func isDir(path string) bool {
	if finfo, err := os.Stat(path); err != nil || !finfo.IsDir() {
		return false
	}
	return true // TODO: check for writability.
}

func MutationLogger(ch chan interface{}) {
	for i := range ch {
		switch o := i.(type) {
		case mutation:
			log.Printf("Mutation: %v", o)
		case vbucketChange:
			log.Printf("VBucket change: %v", o)
			if o.newState == VBActive {
				vb := o.getVBucket()
				if vb != nil {
					// Watch state changes
					vb.observer.Register(ch)
				}
			}
		default:
			panic(fmt.Sprintf("Unhandled item to log %T: %v", i, i))
		}
	}
}
