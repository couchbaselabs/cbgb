package cbgb

import (
	"fmt"
	"log"
	"time"

	"github.com/dustin/gomemcached"
)

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

// How often to send opaque "heartbeats" on tap streams.
var tapTickFreq = time.Second

func doTap(b Bucket, req *gomemcached.MCRequest,
	chpkt chan<- transmissible, cherr <-chan error) *gomemcached.MCResponse {
	tc, err := req.ParseTapCommands()
	if err != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
			Body:   []byte(fmt.Sprintf("ParseTapCommands err: %v", err)),
		}
	}

	res, yesDump := isTapDump(tc)
	if yesDump {
		return doTapDump(b, req, chpkt, cherr, tc)
	}
	if res != nil {
		return res
	}

	// TODO: TAP_BACKFILL, but mind the gap between backfill and tap-forward.

	return doTapForward(b, req, chpkt, cherr, tc)
}

func doTapForward(b Bucket, req *gomemcached.MCRequest,
	chpkt chan<- transmissible, cherr <-chan error,
	tc gomemcached.TapConnect) *gomemcached.MCResponse {
	bch := make(chan interface{})
	mch := make(chan interface{}, 1000)

	b.Subscribe(bch)
	defer b.Unsubscribe(bch)

	ticker := time.NewTicker(tapTickFreq)
	defer ticker.Stop()

	// defer cleanup vbucket mchs
	registered := map[uint16]bool{}
	defer func() {
		for vbid := range registered {
			vb := b.GetVBucket(vbid)
			if vb != nil {
				vb.observer.Unregister(mch)
			}
		}
	}()

	for {
		select {
		case ci := <-bch:
			// VBucket state change, so update registrations
			c := ci.(vbucketChange)
			if vb := c.getVBucket(); vb != nil {
				if c.newState == VBActive {
					vb.observer.Register(mch)
					registered[vb.vbid] = true
				} else if vb != nil {
					vb.observer.Unregister(mch)
					delete(registered, vb.vbid)
				}
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

				vb := b.GetVBucket(m.vb)
				if vb != nil {
					// TODO: if vb is suspended, the get() will freeze
					// the TAP stream until vb is resumed; that may be
					// or may not be what we want.
					res := vb.get(m.key)
					if res.Status != gomemcached.SUCCESS {
						log.Printf("Tapped a missing item, skipping: %s",
							m.key)
						continue
					}
					pkt.Body = res.Body
				} else {
					log.Printf("Change on missing partition? %v", m.vb)
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
		case <-cherr:
			return &gomemcached.MCResponse{Fatal: true}
		}
	}

	return &gomemcached.MCResponse{Fatal: true} // Unreachable.
}

func isTapDump(tc gomemcached.TapConnect) (*gomemcached.MCResponse, bool) {
	v, ok := tc.Flags[gomemcached.DUMP]
	if !ok {
		return nil, false
	}
	switch vx := v.(type) {
	case bool:
		if !vx {
			return nil, false
		}
	default:
		return &gomemcached.MCResponse{Fatal: true}, false
	}
	return nil, true
}

func doTapDump(b Bucket, req *gomemcached.MCRequest,
	chpkt chan<- transmissible, cherr <-chan error,
	tc gomemcached.TapConnect) *gomemcached.MCResponse {
	var res *gomemcached.MCResponse
	var err error

	np := b.GetBucketSettings().NumPartitions
	for i := 0; i < np; i++ {
		vb := b.GetVBucket(uint16(i))
		if vb == nil {
			continue
		}
		if vb.GetVBState() != VBActive {
			continue
		}

		errVisit := vb.ps.visitItems(nil, true, func(i *item) bool {
			// TODO: Need to occasionally send TAP_ACK's.
			chpkt <- &gomemcached.MCRequest{
				Opcode:  gomemcached.TAP_MUTATION,
				VBucket: vb.vbid,
				Key:     i.key,
				Cas:     i.cas,
				Extras:  make([]byte, 16),
				Body:    i.data,
			}
			select {
			case err = <-cherr:
				return false
			default:
			}
			return true
		})

		// TODO: Determine if we should close this now.
		// close(chpkt)

		if errVisit != nil {
			return &gomemcached.MCResponse{Fatal: true}
		}

		if err == nil {
			err = <-cherr
		}
		if err != nil {
			return &gomemcached.MCResponse{Fatal: true}
		}
	}

	return res
}

func MutationLogger(ch chan interface{}) {
	for i := range ch {
		switch o := i.(type) {
		case mutation:
			log.Printf("mutation: %v", o)
		case vbucketChange:
			log.Printf("partition change: %v", o)
			if o.newState == VBActive {
				if vb := o.getVBucket(); vb != nil {
					// Watch state changes
					vb.observer.Register(ch)
				}
			}
		default:
			panic(fmt.Sprintf("Unhandled item to log %T: %v", i, i))
		}
	}
}
