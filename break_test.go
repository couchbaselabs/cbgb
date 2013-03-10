package cbgb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/dustin/gomemcached"
)

const tmpdirName = "break-tmp"
const testKey = "somekey"
const expTime = 3600

type testItem struct {
	Op    string
	Val   *string
	Error bool
}

type testDef map[string][]testItem

/*
  "addaddaddget": [
      {"op": "add", "val": "0", "error": false},
      {"op": "add", "val": "0", "error": true},
      {"op": "add", "val": "0", "error": true},
      {"op": "get", "val": "0", "error": false},
      {"op": "assert", "val": "0"}
  ]
*/

type op func(b *vbucket, memo interface{}) (interface{}, error)

func dispatchCounter(v *vbucket, initial uint64,
	cmd gomemcached.CommandCode) error {

	req := &gomemcached.MCRequest{
		Opcode: cmd,
		Key:    []byte(testKey),
		Extras: make([]byte, 20),
		Body:   []byte{'0'},
	}
	binary.BigEndian.PutUint64(req.Extras, 1)
	binary.BigEndian.PutUint64(req.Extras[8:], initial)
	res := v.Dispatch(ioutil.Discard, req)
	var err error
	if res.Status != 0 {
		err = res
	}
	return err

}

func dispatchTestCommand(v *vbucket, cas uint64,
	cmd gomemcached.CommandCode) (*gomemcached.MCResponse, error) {

	req := &gomemcached.MCRequest{
		Opcode: cmd,
		Key:    []byte(testKey),
		Cas:    cas,
		Body:   []byte{'0'},
	}
	switch cmd {
	case gomemcached.ADD, gomemcached.SET:
		req.Extras = make([]byte, 8)
		binary.BigEndian.PutUint64(req.Extras, uint64(0)<<32|uint64(expTime))
	}
	res := v.Dispatch(ioutil.Discard, req)
	var err error
	if res.Status != 0 {
		err = res
	}
	return res, err
}

func shortTestDispatch(v *vbucket, cmd gomemcached.CommandCode) error {
	_, err := dispatchTestCommand(v, 0, cmd)
	return err
}

// Not handled:
//   delay - inject sleep past expiration date of items
//   append
//   prepend
//   appendUsingCAS
//   prependUsingCAS
var opMap = map[string]op{
	"add": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, shortTestDispatch(v, gomemcached.ADD)
	},
	"set": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, shortTestDispatch(v, gomemcached.SET)
	},
	"setRetainCAS": func(v *vbucket, memo interface{}) (interface{}, error) {
		res, err := dispatchTestCommand(v, 0, gomemcached.SET)
		if err != nil {
			return nil, err
		}
		return res.Cas, err
	},
	"setUsingCAS": func(v *vbucket, memo interface{}) (interface{}, error) {
		casid, ok := memo.(uint64)
		if !ok {
			return nil, fmt.Errorf("Memo doesn't contain a CAS: %+v", memo)
		}
		_, err := dispatchTestCommand(v, casid, gomemcached.SET)
		if err != nil {
			return nil, err
		}
		return 0, err
	},
	"incr": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, dispatchCounter(v, ^uint64(0), gomemcached.INCREMENT)
	},
	"incrWithDefault": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, dispatchCounter(v, 0, gomemcached.INCREMENT)
	},
	"decr": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, dispatchCounter(v, ^uint64(0), gomemcached.DECREMENT)
	},
	"decrWithDefault": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, dispatchCounter(v, 0, gomemcached.DECREMENT)
	},
	"get": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, shortTestDispatch(v, gomemcached.GET)
	},
	"del": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, shortTestDispatch(v, gomemcached.DELETE)
	},
	"deleteUsingCAS": func(v *vbucket, memo interface{}) (interface{}, error) {
		casid, ok := memo.(uint64)
		if !ok {
			return nil, fmt.Errorf("Memo doesn't contain a CAS: %+v", memo)
		}
		_, err := dispatchTestCommand(v, casid, gomemcached.DELETE)
		if err != nil {
			return nil, err
		}
		return 0, err
	},
	"assert": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, nil
	},
	"assertMissing": func(v *vbucket, memo interface{}) (interface{}, error) {
		return nil, nil
	},
}

func runTest(t *testing.T, buckets *Buckets, name string, items []testItem) {
	for _, i := range items {
		if _, ok := opMap[i.Op]; !ok {
			// t.Logf("Skipping %v because of %v", name, i.Op)
			return
		}
	}

	b, err := buckets.New(name, &BucketSettings{MemoryOnly: 2})
	if err != nil {
		t.Errorf("Error making bucket: %v", err)
		return
	}
	defer b.Close()

	vb, err := b.CreateVBucket(0)
	if err != nil {
		t.Errorf("Error making vbucket: %v", err)
		return
	}

	var memo interface{}
	for n, i := range items {
		mtmp, err := opMap[i.Op](vb, memo)
		if mtmp != nil {
			memo = mtmp
		}
		if (err != nil) != i.Error {
			t.Errorf("Unexpected error state in %v on op %v: %+v: %v",
				name, n, i, err)
			return
		}
		res := vb.get([]byte(testKey))
		switch {
		case i.Val == nil && res.Status == 0:
			t.Errorf("Expected missing value after op %v in %v, got %s",
				n, name, res.Body)
			return
		case i.Val != nil && res.Status == 0:
			if *i.Val != string(res.Body) {
				t.Errorf("Expected body=%v after op %v in %v, got %s",
					*i.Val, n, name, res.Body)
				return
			}
		}
	}

	t.Logf("PASS: %v", name)
}

func testRunner(t *testing.T, buckets *Buckets,
	wg *sync.WaitGroup, ch <-chan testDef) {

	defer wg.Done()
	for td := range ch {
		for k, seq := range td {
			runTest(t, buckets, k, seq)
		}
	}
}

func TestAllTheThings(t *testing.T) {
	os.RemoveAll(tmpdirName)
	os.Mkdir(tmpdirName, 0777)
	defer os.RemoveAll(tmpdirName)

	f, err := os.Open("generated_suite_test.json")
	if err != nil {
		t.Logf("Error opening test inputs: %v -- skipping", err)
		return
	}
	defer f.Close()

	d := json.NewDecoder(f)

	ch := make(chan testDef)
	wg := &sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)

		buckets, err := NewBuckets(tmpdirName, &BucketSettings{
			NumPartitions: 1,
			MemoryOnly:    2,
		})
		if err != nil {
			t.Fatalf("Error making buckets: %v", err)
		}
		defer buckets.CloseAll()

		go testRunner(t, buckets, wg, ch)
	}

	ran := 0
	for {
		aTest := testDef{}
		err = d.Decode(&aTest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error decoding things: %v", err)
		}

		ch <- aTest
		ran++
	}
	close(ch)
	wg.Wait()

	t.Logf("Ran %v tests", ran)
}
