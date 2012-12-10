package main

import (
	"io"
	"log"
	"strconv"
	"time"

	"github.com/dustin/gomemcached"
)

type statItem struct {
	key, val string
}

type Stats struct {
	Items uint64

	Ops         uint64
	Gets        uint64
	GetMisses   uint64
	Sets        uint64
	Deletes     uint64
	Creates     uint64
	Updates     uint64
	RGets       uint64
	RGetResults uint64

	IncomingValueBytes uint64
	OutgoingValueBytes uint64

	ErrNotMyRange uint64
}

func (s *Stats) Add(in *Stats) {
	s.Items += in.Items
	s.Ops += in.Ops
	s.Gets += in.Gets
	s.GetMisses += in.GetMisses
	s.Sets += in.Sets
	s.Deletes += in.Deletes
	s.Creates += in.Creates
	s.Updates += in.Updates
	s.RGets += in.RGets
	s.RGetResults += in.RGetResults
	s.IncomingValueBytes += in.IncomingValueBytes
	s.OutgoingValueBytes += in.OutgoingValueBytes
	s.ErrNotMyRange += in.ErrNotMyRange
}

func (s *Stats) Send(ch chan<- statItem) {
	ch <- statItem{"items", strconv.FormatUint(s.Items, 10)}
	ch <- statItem{"ops", strconv.FormatUint(s.Ops, 10)}
	ch <- statItem{"gets", strconv.FormatUint(s.Gets, 10)}
	ch <- statItem{"get_misses", strconv.FormatUint(s.GetMisses, 10)}
	ch <- statItem{"sets", strconv.FormatUint(s.Sets, 10)}
	ch <- statItem{"deletes", strconv.FormatUint(s.Deletes, 10)}
	ch <- statItem{"creates", strconv.FormatUint(s.Creates, 10)}
	ch <- statItem{"updates", strconv.FormatUint(s.Updates, 10)}
	ch <- statItem{"rgets", strconv.FormatUint(s.RGets, 10)}
	ch <- statItem{"rget_results", strconv.FormatUint(s.RGetResults, 10)}
	ch <- statItem{"incoming_value_bytes", strconv.FormatUint(s.IncomingValueBytes, 10)}
	ch <- statItem{"outgoing_value_bytes", strconv.FormatUint(s.OutgoingValueBytes, 10)}
	ch <- statItem{"err_not_my_range", strconv.FormatUint(s.ErrNotMyRange, 10)}
}

func aggregateStats(b *bucket, key string) (agg *Stats) {
	agg = &Stats{}
	for i := uint16(0); i < uint16(MAX_VBUCKET); i++ {
		vb := b.getVBucket(i)
		if vb != nil {
			vb.lock.Lock()
			if vb.state == vbActive {
				agg.Add(&vb.stats)
			}
			vb.lock.Unlock()
		}
	}
	return
}

// This is slightly more complicated than it would generally need to
// be, but as a generator, it's self-terminating based on an input
// stream.  I may do this a bit differently for stats in the future,
// but the model is quite helpful for a tap stream or similar.
func transmitStats(w io.Writer) (chan<- statItem, <-chan error) {
	ch := make(chan statItem)
	pktch, errs := transmitPackets(w)
	go func() {
		for res := range ch {
			pktch <- &gomemcached.MCResponse{
				Opcode: gomemcached.STAT,
				Key:    []byte(res.key),
				Body:   []byte(res.val),
			}
		}
		pktch <- &gomemcached.MCResponse{Opcode: gomemcached.STAT}
		close(pktch)
	}()
	return ch, errs
}

func doStats(b *bucket, w io.Writer, key string) error {
	log.Printf("Doing stats for %#v", key)

	ch, errs := transmitStats(w)
	ch <- statItem{"uptime", time.Since(serverStart).String()}
	ch <- statItem{"version", VERSION}

	agg := aggregateStats(b, key)
	agg.Send(ch)

	close(ch)
	return <-errs
}
