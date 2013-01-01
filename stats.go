package cbgb

import (
	"io"
	"log"
	"strconv"
	"time"

	"github.com/dustin/gomemcached"
)

var serverStart = time.Now()

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
	Unknowns    uint64

	StoreBackFetchedItems    uint64
	StoreBackFetchedModified uint64
	StoreBackFetchedDeleted  uint64
	StoreBackFetchedNil      uint64
	StoreBackFetchedAgain    uint64

	ValueBytesIncoming uint64
	ValueBytesOutgoing uint64

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
	s.Unknowns += in.Unknowns
	s.StoreBackFetchedItems += in.StoreBackFetchedItems
	s.StoreBackFetchedModified += in.StoreBackFetchedModified
	s.StoreBackFetchedDeleted += in.StoreBackFetchedDeleted
	s.StoreBackFetchedNil += in.StoreBackFetchedNil
	s.StoreBackFetchedAgain += in.StoreBackFetchedAgain
	s.ValueBytesIncoming += in.ValueBytesIncoming
	s.ValueBytesOutgoing += in.ValueBytesOutgoing
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
	ch <- statItem{"unknowns", strconv.FormatUint(s.Unknowns, 10)}
	ch <- statItem{"store_back_fetched_items", strconv.FormatUint(s.StoreBackFetchedItems, 10)}
	ch <- statItem{"store_back_fetched_modified", strconv.FormatUint(s.StoreBackFetchedModified, 10)}
	ch <- statItem{"store_back_fetched_deleted", strconv.FormatUint(s.StoreBackFetchedDeleted, 10)}
	ch <- statItem{"store_back_fetched_nil", strconv.FormatUint(s.StoreBackFetchedNil, 10)}
	ch <- statItem{"store_back_fetched_again", strconv.FormatUint(s.StoreBackFetchedAgain, 10)}
	ch <- statItem{"value_bytes_incoming", strconv.FormatUint(s.ValueBytesIncoming, 10)}
	ch <- statItem{"value_bytes_outgoing", strconv.FormatUint(s.ValueBytesOutgoing, 10)}
	ch <- statItem{"err_not_my_range", strconv.FormatUint(s.ErrNotMyRange, 10)}
}

func aggregateStats(b bucket, key string) (agg *Stats) {
	agg = &Stats{}
	for i := uint16(0); i < uint16(MAX_VBUCKET); i++ {
		vb := b.getVBucket(i)
		if vb != nil {
			vb.AddStats(agg, key)
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

func doStats(b bucket, w io.Writer, key string) error {
	log.Printf("Doing stats for %#v", key)

	ch, errs := transmitStats(w)
	ch <- statItem{"uptime", time.Since(serverStart).String()}
	ch <- statItem{"version", VERSION}

	agg := aggregateStats(b, key)
	agg.Send(ch)

	close(ch)
	return <-errs
}
