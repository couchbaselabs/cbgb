package cbgb

import (
	"fmt"
	"log"
	"time"

	"github.com/dustin/gomemcached"
)

// How often to send opaque "heartbeats" on tap streams.
var tapTickFreq = time.Second

func doTap(b bucket, req *gomemcached.MCRequest,
	chpkt chan<- transmissible, cherr <-chan error) error {

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
			vb := b.getVBucket(vbid)
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

				vb := b.getVBucket(m.vb)
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
					log.Printf("Change on missing vbucket? %v", m.vb)
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

func MutationLogger(ch chan interface{}) {
	for i := range ch {
		switch o := i.(type) {
		case mutation:
			log.Printf("Mutation: %v", o)
		case vbucketChange:
			log.Printf("VBucket change: %v", o)
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
