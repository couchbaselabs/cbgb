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
	Items int64 `json:"items"`

	Ops         uint64 `json:"ops"`
	Gets        uint64 `json:"gets"`
	GetMisses   uint64 `json:"getMisses"`
	Sets        uint64 `json:"sets"`
	Deletes     uint64 `json:"deletes"`
	Creates     uint64 `json:"creates"`
	Updates     uint64 `json:"updates"`
	RGets       uint64 `json:"rGets"`
	RGetResults uint64 `json:"rGetResults"`
	Unknowns    uint64 `json:"unknowns"`

	IncomingValueBytes uint64 `json:"incomingValueBytes"`
	OutgoingValueBytes uint64 `json:"outgoingValueBytes"`

	StoreErrors      uint64 `json:"storeErrors"`
	NotMyRangeErrors uint64 `json:"notMyRangeErrors"`
}

func (s *Stats) Add(in *Stats) {
	s.Op(in, addUint64)
}

func (s *Stats) Sub(in *Stats) {
	s.Op(in, subUint64)
}

func (s *Stats) Op(in *Stats, op func(uint64, uint64) uint64) {
	s.Items = int64(op(uint64(s.Items), uint64(atomic.LoadInt64(&in.Items))))
	s.Ops = op(s.Ops, atomic.LoadUint64(&in.Ops))
	s.Gets = op(s.Gets, atomic.LoadUint64(&in.Gets))
	s.GetMisses = op(s.GetMisses, atomic.LoadUint64(&in.GetMisses))
	s.Sets = op(s.Sets, atomic.LoadUint64(&in.Sets))
	s.Deletes = op(s.Deletes, atomic.LoadUint64(&in.Deletes))
	s.Creates = op(s.Creates, atomic.LoadUint64(&in.Creates))
	s.Updates = op(s.Updates, atomic.LoadUint64(&in.Updates))
	s.RGets = op(s.RGets, atomic.LoadUint64(&in.RGets))
	s.RGetResults = op(s.RGetResults, atomic.LoadUint64(&in.RGetResults))
	s.Unknowns = op(s.Unknowns, atomic.LoadUint64(&in.Unknowns))
	s.IncomingValueBytes = op(s.IncomingValueBytes, atomic.LoadUint64(&in.IncomingValueBytes))
	s.OutgoingValueBytes = op(s.OutgoingValueBytes, atomic.LoadUint64(&in.OutgoingValueBytes))
	s.StoreErrors = op(s.StoreErrors, atomic.LoadUint64(&in.StoreErrors))
	s.NotMyRangeErrors = op(s.NotMyRangeErrors, atomic.LoadUint64(&in.NotMyRangeErrors))
}

func (s *Stats) Aggregate(in Aggregatable) {
	if in == nil {
		return
	}
	s.Add(in.(*Stats))
}

func (s *Stats) Equal(in *Stats) bool {
	return s.Items == atomic.LoadInt64(&in.Items) &&
		s.Ops == atomic.LoadUint64(&in.Ops) &&
		s.Gets == atomic.LoadUint64(&in.Gets) &&
		s.GetMisses == atomic.LoadUint64(&in.GetMisses) &&
		s.Sets == atomic.LoadUint64(&in.Sets) &&
		s.Deletes == atomic.LoadUint64(&in.Deletes) &&
		s.Creates == atomic.LoadUint64(&in.Creates) &&
		s.Updates == atomic.LoadUint64(&in.Updates) &&
		s.RGets == atomic.LoadUint64(&in.RGets) &&
		s.RGetResults == atomic.LoadUint64(&in.RGetResults) &&
		s.Unknowns == atomic.LoadUint64(&in.Unknowns) &&
		s.IncomingValueBytes == atomic.LoadUint64(&in.IncomingValueBytes) &&
		s.OutgoingValueBytes == atomic.LoadUint64(&in.OutgoingValueBytes) &&
		s.StoreErrors == atomic.LoadUint64(&in.StoreErrors) &&
		s.NotMyRangeErrors == atomic.LoadUint64(&in.NotMyRangeErrors)
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

func doStats(b Bucket, w io.Writer, key string) error {
	log.Printf("Doing stats for %#v", key)

	ch, errs := transmitStats(w)
	ch <- statItem{"uptime", time.Since(serverStart).String()}
	ch <- statItem{"version", VERSION}

	agg := AggregateStats(b, key)
	agg.Send(ch)

	close(ch)
	return <-errs
}

// ------------------------------------------------

type Aggregatable interface {
	Aggregate(input Aggregatable)
}

var aggStatsLevels = []struct {
	name       string
	numSamples int // Number of historical samples to keep at this level.
}{
	{"second", 60}, // 60 seconds in a minute.
	{"minute", 60}, // 60 minutes in an hour.
	{"hour", 24},   // 24 hours in a day.
	{"day", 1},     // Just track 1 day's worth of aggregate for now.
}

type AggStats struct {
	creator func() Aggregatable
	Levels  []*AggStatsSample
	Counts  []uint64 // Total number of samples at corresponding level.
}

type AggStatsSample struct {
	next  *AggStatsSample
	stats Aggregatable
}

func NewAggStats(creator func() Aggregatable) *AggStats {
	res := &AggStats{
		creator: creator,
		Levels:  make([]*AggStatsSample, len(aggStatsLevels)),
		Counts:  make([]uint64, len(aggStatsLevels)),
	}

	// Initialize ring at each level.
	for i, level := range aggStatsLevels {
		var first *AggStatsSample
		var last *AggStatsSample
		for j := 0; j < level.numSamples; j++ {
			last = &AggStatsSample{next: last}
			if j == 0 {
				first = last
			}
		}
		first.next = last
		res.Levels[i] = last
	}

	return res
}

func (a *AggStats) addSample(s Aggregatable) {
	a.Levels[0].stats = s
	a.Levels[0] = a.Levels[0].next
	a.Counts[0]++

	// Propagate aggregate samples up to higher granularity levels.
	for i, level := range aggStatsLevels {
		if level.numSamples <= 1 {
			break
		}
		if a.Counts[i]%uint64(level.numSamples) != uint64(0) {
			break
		}
		a.Levels[i+1].stats = AggregateSamples(a.creator(), a.Levels[i])
		a.Levels[i+1] = a.Levels[i+1].next
		a.Counts[i+1]++
	}
}

func AggregateSamples(agg Aggregatable, start *AggStatsSample) Aggregatable {
	c := start
	for {
		agg.Aggregate(c.stats)
		c = c.next
		if c == start {
			break
		}
	}
	return agg
}

func AggregateStats(b Bucket, key string) (agg *Stats) {
	agg = &Stats{}
	for i := uint16(0); i < uint16(MAX_VBUCKETS); i++ {
		vb := b.GetVBucket(i)
		if vb != nil {
			vb.AddStatsTo(agg, key)
		}
	}
	return agg
}

func AggregateBucketStoreStats(b Bucket, key string) *BucketStoreStats {
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
