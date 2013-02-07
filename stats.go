package cbgb

import (
	"io"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dustin/gomemcached"
)

var serverStart = time.Now()

type statItem struct {
	key, val string
}

type Stats struct {
	Items int64

	Ops         uint64
	Gets        uint64
	GetMisses   uint64
	Sets        uint64
	Deletes     uint64
	Creates     uint64
	Updates     uint64
	RGets       uint64
	RGetResults uint64
	Unknowns    uint64

	IncomingValueBytes uint64
	OutgoingValueBytes uint64

	StoreErrors      uint64
	NotMyRangeErrors uint64
}

func (s *Stats) Add(in *Stats) {
	s.Items += atomic.LoadInt64(&in.Items)
	s.Ops += atomic.LoadUint64(&in.Ops)
	s.Gets += atomic.LoadUint64(&in.Gets)
	s.GetMisses += atomic.LoadUint64(&in.GetMisses)
	s.Sets += atomic.LoadUint64(&in.Sets)
	s.Deletes += atomic.LoadUint64(&in.Deletes)
	s.Creates += atomic.LoadUint64(&in.Creates)
	s.Updates += atomic.LoadUint64(&in.Updates)
	s.RGets += atomic.LoadUint64(&in.RGets)
	s.RGetResults += atomic.LoadUint64(&in.RGetResults)
	s.Unknowns += atomic.LoadUint64(&in.Unknowns)
	s.IncomingValueBytes += atomic.LoadUint64(&in.IncomingValueBytes)
	s.OutgoingValueBytes += atomic.LoadUint64(&in.OutgoingValueBytes)
	s.StoreErrors += atomic.LoadUint64(&in.StoreErrors)
	s.NotMyRangeErrors += atomic.LoadUint64(&in.NotMyRangeErrors)
}

func (s *Stats) Send(ch chan<- statItem) {
	ch <- statItem{"items", strconv.FormatInt(s.Items, 10)}
	ch <- statItem{"ops", strconv.FormatUint(s.Ops, 10)}
	ch <- statItem{"gets", strconv.FormatUint(s.Gets, 10)}
	ch <- statItem{"get_misses", strconv.FormatUint(s.GetMisses, 10)}
	ch <- statItem{"sets", strconv.FormatUint(s.Sets, 10)}
	ch <- statItem{"deletes", strconv.FormatUint(s.Deletes, 10)}
	ch <- statItem{"creates", strconv.FormatUint(s.Creates, 10)}
	ch <- statItem{"updates", strconv.FormatUint(s.Updates, 10)}
	ch <- statItem{"rgets", strconv.FormatUint(s.RGets, 10)}
	ch <- statItem{"rget_results", strconv.FormatUint(s.RGetResults, 10)}
	ch <- statItem{"unknowns", strconv.FormatUint(s.Unknowns, 10)}
	ch <- statItem{"incoming_value_bytes", strconv.FormatUint(s.IncomingValueBytes, 10)}
	ch <- statItem{"outgoing_value_bytes", strconv.FormatUint(s.OutgoingValueBytes, 10)}
	ch <- statItem{"store_errors", strconv.FormatUint(s.StoreErrors, 10)}
	ch <- statItem{"not_my_range_errors", strconv.FormatUint(s.NotMyRangeErrors, 10)}
}

func AggregateStats(b bucket, key string) (agg *Stats) {
	agg = &Stats{}
	for i := uint16(0); i < uint16(MAX_VBUCKETS); i++ {
		vb := b.GetVBucket(i)
		if vb != nil {
			vb.AddStatsTo(agg, key)
		}
	}
	return
}

func AggregateBucketStoreStats(b bucket, key string) *BucketStoreStats {
	agg := &BucketStoreStats{}
	i := 0
	for {
		bs := b.GetBucketStore(i)
		if bs == nil {
			break
		}
		agg.Add(bs.Stats())
		i++
	}
	return agg
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

func doStats(b bucket, w io.Writer, key string) error {
	log.Printf("Doing stats for %#v", key)

	ch, errs := transmitStats(w)
	ch <- statItem{"uptime", time.Since(serverStart).String()}
	ch <- statItem{"version", VERSION}

	agg := AggregateStats(b, key)
	agg.Send(ch)

	close(ch)
	return <-errs
}
