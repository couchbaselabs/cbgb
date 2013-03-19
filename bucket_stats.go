package cbgb

import (
	"io"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dustin/gomemcached"
)

type BucketStatsSnapshot struct {
	Current        *Stats
	BucketStore    *BucketStoreStats
	Agg            *AggStats
	AggBucketStore *AggStats
	LatestUpdate   time.Time

	requests int
}

func (bss *BucketStatsSnapshot) LatestUpdateTime() time.Time {
	return bss.LatestUpdate
}

func (bss *BucketStatsSnapshot) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"totals": map[string]interface{}{
			"bucketStats":      bss.Current,
			"bucketStoreStats": bss.BucketStore,
		},
		"diffs": map[string]interface{}{
			"bucketStats":      bss.Agg,
			"bucketStoreStats": bss.AggBucketStore,
		},
		"levels": AggStatsLevels,
	}
}

func (b *BucketStatsSnapshot) Copy() *BucketStatsSnapshot {
	return &BucketStatsSnapshot{
		&(*b.Current),
		&(*b.BucketStore),
		&(*b.Agg),
		&(*b.AggBucketStore),
		b.LatestUpdate,
		0,
	}
}

type statItem struct {
	key, val string
}

type Stats struct {
	Time int64 `json:"time"`

	Items int64 `json:"items"`

	Ops         int64 `json:"ops"`
	Gets        int64 `json:"gets"`
	GetMisses   int64 `json:"getMisses"`
	Mutations   int64 `json:"mutations"`
	Sets        int64 `json:"sets"`
	Adds        int64 `json:"adds"`
	Replaces    int64 `json:"replaces"`
	Appends     int64 `json:"appends"`
	Prepends    int64 `json:"prepends"`
	Incrs       int64 `json:"incrs"`
	Decrs       int64 `json:"decrs"`
	Deletes     int64 `json:"deletes"`
	Creates     int64 `json:"creates"`
	Updates     int64 `json:"updates"`
	Expirable   int64 `json:"expirable"`
	RGets       int64 `json:"rGets"`
	RGetResults int64 `json:"rGetResults"`
	Unknowns    int64 `json:"unknowns"`

	IncomingValueBytes int64 `json:"incomingValueBytes"`
	OutgoingValueBytes int64 `json:"outgoingValueBytes"`
	ItemBytes          int64 `json:"itemBytes"`

	StoreErrors      int64 `json:"storeErrors"`
	NotMyRangeErrors int64 `json:"notMyRangeErrors"`
}

func (s *Stats) Add(in *Stats) {
	s.Op(in, addInt64)
}

func (s *Stats) Sub(in *Stats) {
	s.Op(in, subInt64)
}

func (s *Stats) Op(in *Stats, op func(int64, int64) int64) {
	s.Items = op(s.Items, atomic.LoadInt64(&in.Items))
	s.Ops = op(s.Ops, atomic.LoadInt64(&in.Ops))
	s.Gets = op(s.Gets, atomic.LoadInt64(&in.Gets))
	s.GetMisses = op(s.GetMisses, atomic.LoadInt64(&in.GetMisses))
	s.Mutations = op(s.Mutations, atomic.LoadInt64(&in.Mutations))
	s.Sets = op(s.Sets, atomic.LoadInt64(&in.Sets))
	s.Adds = op(s.Adds, atomic.LoadInt64(&in.Adds))
	s.Replaces = op(s.Replaces, atomic.LoadInt64(&in.Replaces))
	s.Appends = op(s.Appends, atomic.LoadInt64(&in.Appends))
	s.Prepends = op(s.Prepends, atomic.LoadInt64(&in.Prepends))
	s.Incrs = op(s.Incrs, atomic.LoadInt64(&in.Incrs))
	s.Decrs = op(s.Decrs, atomic.LoadInt64(&in.Decrs))
	s.Deletes = op(s.Deletes, atomic.LoadInt64(&in.Deletes))
	s.Creates = op(s.Creates, atomic.LoadInt64(&in.Creates))
	s.Updates = op(s.Updates, atomic.LoadInt64(&in.Updates))
	s.RGets = op(s.RGets, atomic.LoadInt64(&in.RGets))
	s.RGetResults = op(s.RGetResults, atomic.LoadInt64(&in.RGetResults))
	s.Unknowns = op(s.Unknowns, atomic.LoadInt64(&in.Unknowns))
	s.IncomingValueBytes = op(s.IncomingValueBytes, atomic.LoadInt64(&in.IncomingValueBytes))
	s.OutgoingValueBytes = op(s.OutgoingValueBytes, atomic.LoadInt64(&in.OutgoingValueBytes))
	s.ItemBytes = int64(op(s.ItemBytes, atomic.LoadInt64(&in.ItemBytes)))
	s.StoreErrors = op(s.StoreErrors, atomic.LoadInt64(&in.StoreErrors))
	s.NotMyRangeErrors = op(s.NotMyRangeErrors, atomic.LoadInt64(&in.NotMyRangeErrors))
}

func (s *Stats) Aggregate(in Aggregatable) {
	if in == nil {
		return
	}
	s.Add(in.(*Stats))
}

func (s *Stats) Equal(in *Stats) bool {
	return s.Items == atomic.LoadInt64(&in.Items) &&
		s.Ops == atomic.LoadInt64(&in.Ops) &&
		s.Gets == atomic.LoadInt64(&in.Gets) &&
		s.GetMisses == atomic.LoadInt64(&in.GetMisses) &&
		s.Mutations == atomic.LoadInt64(&in.Mutations) &&
		s.Sets == atomic.LoadInt64(&in.Sets) &&
		s.Adds == atomic.LoadInt64(&in.Adds) &&
		s.Replaces == atomic.LoadInt64(&in.Replaces) &&
		s.Appends == atomic.LoadInt64(&in.Appends) &&
		s.Prepends == atomic.LoadInt64(&in.Prepends) &&
		s.Incrs == atomic.LoadInt64(&in.Incrs) &&
		s.Decrs == atomic.LoadInt64(&in.Decrs) &&
		s.Deletes == atomic.LoadInt64(&in.Deletes) &&
		s.Creates == atomic.LoadInt64(&in.Creates) &&
		s.Updates == atomic.LoadInt64(&in.Updates) &&
		s.RGets == atomic.LoadInt64(&in.RGets) &&
		s.RGetResults == atomic.LoadInt64(&in.RGetResults) &&
		s.Unknowns == atomic.LoadInt64(&in.Unknowns) &&
		s.IncomingValueBytes == atomic.LoadInt64(&in.IncomingValueBytes) &&
		s.OutgoingValueBytes == atomic.LoadInt64(&in.OutgoingValueBytes) &&
		s.ItemBytes == atomic.LoadInt64(&in.ItemBytes) &&
		s.StoreErrors == atomic.LoadInt64(&in.StoreErrors) &&
		s.NotMyRangeErrors == atomic.LoadInt64(&in.NotMyRangeErrors)
}

func (s *Stats) Send(ch chan<- statItem) {
	ch <- statItem{"items", strconv.FormatInt(s.Items, 10)}
	ch <- statItem{"ops", strconv.FormatInt(s.Ops, 10)}
	ch <- statItem{"gets", strconv.FormatInt(s.Gets, 10)}
	ch <- statItem{"get_misses", strconv.FormatInt(s.GetMisses, 10)}
	ch <- statItem{"mutations", strconv.FormatInt(s.Mutations, 10)}
	ch <- statItem{"sets", strconv.FormatInt(s.Sets, 10)}
	ch <- statItem{"adds", strconv.FormatInt(s.Adds, 10)}
	ch <- statItem{"replaces", strconv.FormatInt(s.Replaces, 10)}
	ch <- statItem{"appends", strconv.FormatInt(s.Appends, 10)}
	ch <- statItem{"prepends", strconv.FormatInt(s.Prepends, 10)}
	ch <- statItem{"incrs", strconv.FormatInt(s.Incrs, 10)}
	ch <- statItem{"decrs", strconv.FormatInt(s.Decrs, 10)}
	ch <- statItem{"deletes", strconv.FormatInt(s.Deletes, 10)}
	ch <- statItem{"creates", strconv.FormatInt(s.Creates, 10)}
	ch <- statItem{"updates", strconv.FormatInt(s.Updates, 10)}
	ch <- statItem{"rgets", strconv.FormatInt(s.RGets, 10)}
	ch <- statItem{"rget_results", strconv.FormatInt(s.RGetResults, 10)}
	ch <- statItem{"unknowns", strconv.FormatInt(s.Unknowns, 10)}
	ch <- statItem{"incoming_value_bytes", strconv.FormatInt(s.IncomingValueBytes, 10)}
	ch <- statItem{"outgoing_value_bytes", strconv.FormatInt(s.OutgoingValueBytes, 10)}
	ch <- statItem{"item_bytes", strconv.FormatInt(s.ItemBytes, 10)}
	ch <- statItem{"store_errors", strconv.FormatInt(s.StoreErrors, 10)}
	ch <- statItem{"not_my_range_errors", strconv.FormatInt(s.NotMyRangeErrors, 10)}
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

	statAge := b.StatAge()
	ch <- statItem{"stateAge", statAge.String()}

	if statAge > time.Second*30 {
		log.Printf("Stats are too old.  Starting them up.")
		b.StartStats(time.Second)
	} else {
		agg := AggregateStats(b, key)
		agg.Send(ch)
	}

	close(ch)
	return <-errs
}

func updateMutationStats(cmdIn gomemcached.CommandCode, stats *Stats) (cmd gomemcached.CommandCode) {
	switch cmdIn {
	case gomemcached.SET:
		cmd = gomemcached.SET
		atomic.AddInt64(&stats.Sets, 1)
	case gomemcached.SETQ:
		cmd = gomemcached.SET
		atomic.AddInt64(&stats.Sets, 1)
	case gomemcached.ADD:
		cmd = gomemcached.ADD
		atomic.AddInt64(&stats.Adds, 1)
	case gomemcached.ADDQ:
		cmd = gomemcached.ADD
		atomic.AddInt64(&stats.Adds, 1)
	case gomemcached.REPLACE:
		cmd = gomemcached.REPLACE
		atomic.AddInt64(&stats.Replaces, 1)
	case gomemcached.REPLACEQ:
		cmd = gomemcached.REPLACE
		atomic.AddInt64(&stats.Replaces, 1)
	case gomemcached.APPEND:
		cmd = gomemcached.APPEND
		atomic.AddInt64(&stats.Appends, 1)
	case gomemcached.APPENDQ:
		cmd = gomemcached.APPEND
		atomic.AddInt64(&stats.Appends, 1)
	case gomemcached.PREPEND:
		cmd = gomemcached.PREPEND
		atomic.AddInt64(&stats.Prepends, 1)
	case gomemcached.PREPENDQ:
		cmd = gomemcached.PREPEND
		atomic.AddInt64(&stats.Prepends, 1)
	case gomemcached.INCREMENT:
		cmd = gomemcached.INCREMENT
		atomic.AddInt64(&stats.Incrs, 1)
	case gomemcached.INCREMENTQ:
		cmd = gomemcached.INCREMENT
		atomic.AddInt64(&stats.Incrs, 1)
	case gomemcached.DECREMENT:
		cmd = gomemcached.DECREMENT
		atomic.AddInt64(&stats.Decrs, 1)
	case gomemcached.DECREMENTQ:
		cmd = gomemcached.DECREMENT
		atomic.AddInt64(&stats.Decrs, 1)
	}
	return cmd // Return the non-quiet CommandCode equivalent.
}

func AggregateStats(b Bucket, key string) (agg *Stats) {
	agg = &Stats{}
	for i := uint16(0); i < uint16(MAX_VBUCKETS); i++ {
		vb, _ := b.GetVBucket(i)
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
