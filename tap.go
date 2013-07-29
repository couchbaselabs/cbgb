package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/server"
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

func doTap(b Bucket, req *gomemcached.MCRequest, r io.Reader,
	chpkt chan<- transmissible, cherr <-chan error) *gomemcached.MCResponse {
	tc, err := req.ParseTapCommands()
	if err != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
			Body:   []byte(fmt.Sprintf("ParseTapCommands err: %v", err)),
		}
	}

	// TODO: TAP of a vbucket list.

	res, yesDump := tapFlagBool(&tc, gomemcached.DUMP)
	if res != nil {
		return res
	}
	if yesDump || tapFlagExists(&tc, gomemcached.BACKFILL) {
		res := doTapBackFill(b, req, r, chpkt, cherr, tc)
		if res != nil {
			return res
		}
		if yesDump {
			close(chpkt)
			return &gomemcached.MCResponse{Fatal: true}
		}
	}

	// TODO: There's probably a mutation gap between backfill and tap-forward.

	return doTapForward(b, req, r, chpkt, cherr, tc)
}

func tapFlagBool(tc *gomemcached.TapConnect, flag gomemcached.TapConnectFlag) (
	*gomemcached.MCResponse, bool) {
	v, ok := tc.Flags[flag]
	if !ok {
		return nil, false
	}
	if vx, ok := v.(bool); ok {
		return nil, vx
	}
	return &gomemcached.MCResponse{Fatal: true}, false
}

func tapFlagExists(tc *gomemcached.TapConnect, flag gomemcached.TapConnectFlag) bool {
	_, ok := tc.Flags[flag]
	return ok
}

func doTapForward(b Bucket, req *gomemcached.MCRequest, r io.Reader,
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
			vb, _ := b.GetVBucket(vbid)
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

				vb, _ := b.GetVBucket(m.vb)
				if vb != nil {
					// TODO: if vb is suspended, the get() will freeze
					// the TAP stream until vb is resumed; that may be
					// or may not be what we want.
					res := vb.get(m.key)
					if res.Status != gomemcached.SUCCESS {
						log.Printf("tapped a missing item, skipping key: %s, res: %#v",
							m.key, res)
						continue
					}
					pkt.Body = res.Body
				} else {
					log.Printf("tapping a missing partition: %v", m.vb)
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
}

func doTapBackFill(b Bucket, req *gomemcached.MCRequest, r io.Reader,
	chpkt chan<- transmissible, cherr <-chan error,
	tc gomemcached.TapConnect) *gomemcached.MCResponse {
	var err error

	np := b.GetBucketSettings().NumPartitions
	for vbid := 0; vbid < np; vbid++ {
		vb, _ := b.GetVBucket(uint16(vbid))
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
				VBucket: uint16(vbid),
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
		if errVisit != nil {
			close(chpkt)
			return &gomemcached.MCResponse{Fatal: true}
		}
		if err != nil {
			close(chpkt)
			return &gomemcached.MCResponse{Fatal: true}
		}
	}

	// TODO: Skipping error handling for now, as it always errors
	// since the memcached.ReadPacket() is expecting a REQ instead of
	// RES for the ACK's header magic.  The fix involves exposing more
	// helper functions in gomemcached to help us flip the REQ/RES
	// stream direction.
	doTapAck(r, chpkt, cherr)

	return nil
}

func doTapAck(r io.Reader, chpkt chan<- transmissible, cherr <-chan error) error {
	ackReq := &gomemcached.MCRequest{
		Opcode: gomemcached.TAP_OPAQUE,
		Extras: make([]byte, 8),
	}
	TAP_FLAG_ACK := uint16(0x01)
	binary.BigEndian.PutUint16(ackReq.Extras[2:], TAP_FLAG_ACK)

	chpkt <- ackReq
	select {
	case err := <-cherr:
		return err
	default:
	}

	// TODO: Validate that the response matches the ACK that we expect.
	_, err := memcached.ReadPacket(r)
	return err
}

func MutationLogger(ch chan interface{}) {
	for i := range ch {
		switch o := i.(type) {
		case mutation:
			log.Printf("tap mutation: %v", o)
		case vbucketChange:
			log.Printf("tap partition change: %v", o)
			if o.newState == VBActive {
				if vb := o.getVBucket(); vb != nil {
					// Watch state changes
					vb.observer.Register(ch)
				}
			}
		default:
			panic(fmt.Sprintf("unhandled mutation logger type %T: %v", i, i))
		}
	}
}
