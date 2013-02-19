package cbgb

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
)

func TestCompaction(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{b0}
	b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)
	for i := 0; i < 100; i++ {
		testLoadInts(t, r0, 2, 5)
		if err = b0.Flush(); err != nil {
			t.Errorf("expected Flush (loop) to work, got: %v", err)
		}
	}
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4},
		"initial data load")

	if err = b0.Compact(); err != nil {
		t.Errorf("expected Compact to work, got: %v", err)
	}
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4},
		"after compaction")
	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testLoadInts(t, r0, 2, 7)
	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4, 5, 6},
		"mutation after compaction")
	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	active := uint16(2)
	tests := []struct {
		op  gomemcached.CommandCode
		vb  uint16
		key string
		val string
	}{
		{gomemcached.DELETE, active, "0", ""},
		{gomemcached.DELETE, active, "2", ""},
		{gomemcached.DELETE, active, "4", ""},
		{gomemcached.SET, active, "2", "2"},
		{gomemcached.SET, active, "5", "5"},
	}

	for _, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: x.vb,
			Key:     []byte(x.key),
			Body:    []byte(x.val),
		}
		res := r0.HandleMessage(ioutil.Discard, req)
		if res.Status != gomemcached.SUCCESS {
			t.Errorf("Expected %v for %v:%v/%v, got %v",
				gomemcached.SUCCESS, x.op, x.vb, x.key, res.Status)
		}
	}
	testExpectInts(t, r0, 2, []int{1, 2, 3, 5, 6}, "after delete")

	if err = b0.Compact(); err != nil {
		t.Errorf("expected Compact to work, got: %v", err)
	}
	testExpectInts(t, r0, 2, []int{1, 2, 3, 5, 6}, "after delete, compact")

	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}
	testExpectInts(t, r0, 2, []int{1, 2, 3, 5, 6}, "after delete, compact, flush")

	b0.Close()

	b1, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   11 * time.Second,
			SleepInterval:   11 * time.Second,
			CompactInterval: 11 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r1 := &reqHandler{b1}
	err = b1.Load()
	if err != nil {
		t.Errorf("expected Load to work, err: %v", err)
	}
	testExpectInts(t, r1, 2, []int{1, 2, 3, 5, 6}, "after reload")
}

func TestEmptyFileCompaction(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{b0}
	b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)
	testExpectInts(t, r0, 2, []int{},
		"initially empty collection")

	if err = b0.Compact(); err != nil {
		t.Errorf("expected Compact to work, got: %v", err)
	}
	testExpectInts(t, r0, 2, []int{},
		"after compaction")
	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testLoadInts(t, r0, 2, 7)
	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4, 5, 6},
		"mutation after compaction")
	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}
}

func TestCompactionNumFiles(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Second,
			CompactInterval: 10 * time.Second,
			PurgeTimeout:    20 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{b0}
	b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)
	for i := 0; i < 100; i++ {
		testLoadInts(t, r0, 2, 5)
		if err = b0.Flush(); err != nil {
			t.Errorf("expected Flush (loop) to work, got: %v", err)
		}
	}
	preCompactFiles, err := ioutil.ReadDir(testBucketDir)
	if len(preCompactFiles) != 5 {
		t.Errorf("expected 5 preCompactFiles, got: %v",
			len(preCompactFiles))
	}
	if err = b0.Compact(); err != nil {
		t.Errorf("expected Compact to work, got: %v", err)
	}
	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}
	b0.Close()
	postCompactFiles, err := ioutil.ReadDir(testBucketDir)
	if len(preCompactFiles) != 5 {
		t.Errorf("expected 9 postCompactFiles, got: %v",
			len(postCompactFiles))
	}
}

func TestCompactionPurgeTimeout(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   10 * time.Second,
			SleepInterval:   time.Millisecond,
			CompactInterval: 10 * time.Second,
			PurgeTimeout:    time.Millisecond,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{b0}
	b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)
	for i := 0; i < 100; i++ {
		testLoadInts(t, r0, 2, 5)
		if err = b0.Flush(); err != nil {
			t.Errorf("expected Flush (loop) to work, got: %v", err)
		}
	}
	preCompactFiles, err := ioutil.ReadDir(testBucketDir)
	if err = b0.Compact(); err != nil {
		t.Errorf("expected Compact to work, got: %v", err)
	}
	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	b0.Close()
	postCompactFiles, err := ioutil.ReadDir(testBucketDir)
	if len(postCompactFiles) != len(preCompactFiles) {
		t.Errorf("expected purged postCompactFiles == preCompactFiles with, got: %v vs %v",
			len(postCompactFiles), len(preCompactFiles))
	}
}
