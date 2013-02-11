package cbgb

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
	mcclient "github.com/dustin/gomemcached/client"
)

type rwCloser struct{ io.ReadWriter }

func (rwCloser) Close() error { return nil }

type errWriter struct {
	e error
}

func (e errWriter) Write([]byte) (int, error) {
	return 0, e.e
}

func failErr(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("%v: %v", msg, err)
	}
}

// given a slice of bytes, return all of the responses decoded and the
// error that stopped us from decoding further.
func decodeResponses(t *testing.T, b []byte) []*gomemcached.MCResponse {
	r := bytes.NewBuffer(b)
	c, err := mcclient.Wrap(&rwCloser{r})
	failErr(t, err, "wrap")

	rv := []*gomemcached.MCResponse{}
	for err == nil {
		var res *gomemcached.MCResponse
		res, err = c.Receive()
		if err == nil {
			rv = append(rv, res)
		}
	}
	if err != io.EOF {
		t.Fatalf("Expected EOF.  Got something else: %v", err)
	}

	return rv
}

// Verify a stats call that produces no stats does the right thing.
func TestStatsEmpty(t *testing.T) {
	w := &bytes.Buffer{}
	ch, errs := transmitStats(w)
	close(ch)
	err := <-errs

	failErr(t, err, "Error sending stats")

	res := decodeResponses(t, w.Bytes())
	if len(res) != 1 {
		t.Fatalf("Expected one response, got %v", res)
	}

	r1 := res[0]
	if r1.Status != gomemcached.SUCCESS || len(r1.Key)+len(r1.Body) > 0 {
		t.Fatalf("Invalid response received: %v", res)
	}
}

// Test successfully delivering and retrieving and verifying three
// stats.
func TestStatsThree(t *testing.T) {
	tests := []statItem{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	}

	w := &bytes.Buffer{}
	ch, errs := transmitStats(w)
	for _, i := range tests {
		ch <- i
	}
	close(ch)
	err := <-errs

	failErr(t, err, "Error sending stats")

	res := decodeResponses(t, w.Bytes())
	if len(res) != len(tests)+1 {
		t.Fatalf("Expected %d responses, got %v", len(tests)+1, res)
	}

	for i := range tests {
		r := res[i]
		if r.Status != gomemcached.SUCCESS || r.Opcode != gomemcached.STAT {
			t.Errorf("Invalid response received: %v", res)
		}
		if string(r.Key) != tests[i].key {
			t.Errorf("Expected key %s, got %s", tests[i].key, r.Key)
		}
		if string(r.Body) != tests[i].val {
			t.Errorf("Expected value %s, got %s", tests[i].val, r.Body)
		}
	}

	r := res[len(tests)]
	if r.Status != gomemcached.SUCCESS ||
		r.Opcode != gomemcached.STAT || len(r.Key)+len(r.Body) > 0 {

		t.Fatalf("Invalid response received: %v", res)
	}
}

// This test primarily ensures the stats writer doesn't hang or
// deadlock when the writer can't complete (e.g. if the input eater on
// error case is removed, this test fails)
func TestStatsError(t *testing.T) {
	w := errWriter{io.EOF}
	ch, errs := transmitStats(w)
	ch <- statItem{"not", "deliverable"}
	ch <- statItem{"also", "not deliverable"}
	close(ch)
	err := <-errs

	if err != io.EOF {
		t.Fatalf("Expected EOF writing to the error writer, %v", err)
	}
}

func TestNewBucketAggregateStats(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, _ := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Millisecond,
			SleepInterval:   time.Millisecond,
			CompactInterval: 10 * time.Second,
		})

	s := AggregateStats(b0, "")
	if s == nil {
		t.Errorf("Expected non-nil aggregatestats()")
	}
	if !s.Equal(&Stats{}) {
		t.Errorf("Expected stats to be empty.")
	}

	bss := AggregateBucketStoreStats(b0, "")
	if bss.Flushes != 0 {
		t.Errorf("Unexpected bss value: %v", bss)
	}
	if !bss.Equal(&BucketStoreStats{Stats: 4}) {
		t.Errorf("Expected new bss to equal, got: %v",
			bss)
	}
}

func TestBasicAggStats(t *testing.T) {
	a := NewAggStats(func() Aggregatable {
		return &Stats{}
	})
	if a == nil {
		t.Errorf("Expected NewAggStats() to work")
	}

	if len(a.Levels) != len(aggStatsLevels) {
		t.Errorf("Expected levels to match")
	}

	for i := 0; i < 59; i++ {
		a.addSample(&Stats{Ops: uint64(i)})
	}
	if a.Counts[0] != uint64(59) {
		t.Errorf("Expected 59 level-0 samples, got %v",
			a.Counts[0])
	}
	if a.Counts[1] != uint64(0) {
		t.Errorf("Expected 0 level-1 samples, got %v",
			a.Counts[1])
	}
	if a.Counts[2] != uint64(0) {
		t.Errorf("Expected 0 level-2 samples, got %v",
			a.Counts[2])
	}

	var s *Stats

	s = AggregateSamples(&Stats{}, a.Levels[0]).(*Stats)
	if s.Ops != uint64(1711) {
		t.Errorf("Expected level[0] s.ops 1711, got %v",
			s.Ops)
	}
	s = AggregateSamples(&Stats{}, a.Levels[1]).(*Stats)
	if s.Ops != uint64(0) {
		t.Errorf("Expected level[1] s.ops 0, got %v",
			s.Ops)
	}
	s = AggregateSamples(&Stats{}, a.Levels[2]).(*Stats)
	if s.Ops != uint64(0) {
		t.Errorf("Expected level[2] s.ops 0, got %v",
			s.Ops)
	}

	a.addSample(&Stats{Ops: uint64(60)})
	if a.Counts[0] != uint64(60) {
		t.Errorf("Expected 60 level-0 samples, got %v",
			a.Counts[0])
	}
	if a.Counts[1] != uint64(1) {
		t.Errorf("Expected 1 level-1 samples, got %v",
			a.Counts[1])
	}
	if a.Counts[2] != uint64(0) {
		t.Errorf("Expected 0 level-2 samples, got %v",
			a.Counts[2])
	}

	s = AggregateSamples(&Stats{}, a.Levels[0]).(*Stats)
	if s.Ops != uint64(1771) {
		t.Errorf("Expected level[0] s.ops 1771, got %v",
			s.Ops)
	}
	s = AggregateSamples(&Stats{}, a.Levels[1]).(*Stats)
	if s.Ops != uint64(1771) {
		t.Errorf("Expected level[1] s.ops 1771, got %v",
			s.Ops)
	}
	s = AggregateSamples(&Stats{}, a.Levels[2]).(*Stats)
	if s.Ops != uint64(0) {
		t.Errorf("Expected level[2] s.ops 0, got %v",
			s.Ops)
	}
}

func TestMultiDayAggStats(t *testing.T) {
	a := NewAggStats(func() Aggregatable {
		return &Stats{}
	})
	if a == nil {
		t.Errorf("Expected NewAggStats() to work")
	}

	s := &Stats{Ops: uint64(10)}
	n := 60 * 60 * 24 * 10 // 10 days worth

	for i := 0; i < n; i++ {
		a.addSample(s)
	}

	s = AggregateSamples(&Stats{}, a.Levels[0]).(*Stats)
	if s.Ops != uint64(60*10) {
		t.Errorf("Expected level[0] s.ops %v, got %v",
			60*10, s.Ops)
	}
	s = AggregateSamples(&Stats{}, a.Levels[1]).(*Stats)
	if s.Ops != uint64(60*60*10) {
		t.Errorf("Expected level[1] s.ops %v, got %v",
			60*60*10, s.Ops)
	}
	s = AggregateSamples(&Stats{}, a.Levels[2]).(*Stats)
	if s.Ops != uint64(24*60*60*10) {
		t.Errorf("Expected level[2] s.ops %v, got %v",
			24*60*60*10, s.Ops)
	}
	s = AggregateSamples(&Stats{}, a.Levels[3]).(*Stats)
	if s.Ops != uint64(24*60*60*10) {
		t.Errorf("Expected level[3] s.ops %v, got %v",
			24*60*60*10, s.Ops)
	}
}
