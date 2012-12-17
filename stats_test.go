package cbgb

import (
	"bytes"
	"io"
	"testing"

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
