package cbgb

import (
	"bytes"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/dustin/gomemcached"
)

func testLoadInts(t *testing.T, rh *reqHandler, vbid int, numItems int) {
	for i := 0; i < numItems; i++ {
		req := &gomemcached.MCRequest{
			Opcode:  gomemcached.SET,
			Key:     []byte(strconv.Itoa(i)),
			Body:    []byte(strconv.Itoa(i)),
			VBucket: uint16(vbid),
		}
		res := rh.HandleMessage(nil, req)
		if res.Status != gomemcached.SUCCESS {
			t.Errorf("expected SET of %v to work, got: %v", i, res)
		}
	}
}

func testExpectInts(t *testing.T, rh *reqHandler, vbid int, expectedInts []int,
	desc string) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.RGET,
		Key:     []byte("0"),
		VBucket: uint16(vbid),
	}
	w := &bytes.Buffer{}
	res := rh.HandleMessage(w, req)
	if res.Status != gomemcached.SUCCESS {
		t.Errorf("testExpectInts: %v - expected RGET success, got: %v",
			desc, res)
	}
	results := decodeResponses(t, w.Bytes())
	if len(results) != len(expectedInts) {
		t.Errorf("testExpectInts: %v - expected to see %v results, got: %v, len(w): %v",
			desc, len(expectedInts), len(results), len(w.Bytes()))
	}
	for i, expectedInt := range expectedInts {
		if !bytes.Equal(results[i].Key, []byte(strconv.Itoa(expectedInt))) {
			t.Errorf("testExpectInts: %v - expected rget result key: %v, got: %v",
				desc, expectedInt, string(results[i].Key))
		}
		if !bytes.Equal(results[i].Body, []byte(strconv.Itoa(expectedInt))) {
			t.Errorf("testExpectInts: %v - expected rget result val: %v, got: %v",
				desc, expectedInt, string(results[i].Body))
		}
	}
}

func TestSaveLoadEmptyBucket(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir)
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b0.Close()

	r0 := &reqHandler{b0}
	b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)

	testLoadInts(t, r0, 2, 0)
	testExpectInts(t, r0, 2, []int{}, "initial data load")

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testExpectInts(t, r0, 2, []int{}, "after flush")

	b1, err := NewBucket(testBucketDir)
	if err != nil {
		t.Errorf("expected NewBucket re-open to work, err: %v", err)
	}
	defer b1.Close()
	r1 := &reqHandler{b1}
	err = b1.Load()
	if err != nil {
		t.Errorf("expected Load to work, err: %v", err)
	}
	testExpectInts(t, r1, 2, []int{}, "reload")
}

func TestSaveLoadBasic(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	// defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir)
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	// defer b0.Close()

	r0 := &reqHandler{b0}
	b0.CreateVBucket(2)
	if b0.SetVBState(2, VBActive) == nil {
		t.Errorf("expected SetVBState to work")
	}

	testLoadInts(t, r0, 2, 5)
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "initial data load")

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "after flush")

	b1, err := NewBucket(testBucketDir)
	if err != nil {
		t.Errorf("expected NewBucket re-open to work, err: %v", err)
	}
	defer b1.Close()
	r1 := &reqHandler{b1}
	err = b1.Load()
	if err != nil {
		t.Errorf("expected Load to work, err: %v", err)
	}
	testExpectInts(t, r1, 2, []int{0, 1, 2, 3, 4}, "reload")
}
