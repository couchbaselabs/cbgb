package cbgb

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
)

func TestCompaction(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions:   MAX_VBUCKETS,
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
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
			NumPartitions:   MAX_VBUCKETS,
			FlushInterval:   11 * time.Second,
			SleepInterval:   11 * time.Second,
			CompactInterval: 11 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r1 := &reqHandler{currentBucket: b1}
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
			NumPartitions:   MAX_VBUCKETS,
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
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
			NumPartitions:   MAX_VBUCKETS,
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Second,
			CompactInterval: 10 * time.Second,
			PurgeTimeout:    20 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
	b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)
	for i := 0; i < 100; i++ {
		testLoadInts(t, r0, 2, 5)
		if err = b0.Flush(); err != nil {
			t.Errorf("expected Flush (loop) to work, got: %v", err)
		}
	}
	preCompactFiles, err := ioutil.ReadDir(testBucketDir)
	if len(preCompactFiles) != STORES_PER_BUCKET+1 {
		t.Errorf("expected %v preCompactFiles, got: %v",
			STORES_PER_BUCKET+1, len(preCompactFiles))
	}
	if err = b0.Compact(); err != nil {
		t.Errorf("expected Compact to work, got: %v", err)
	}
	if err = b0.Flush(); err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}
	b0.Close()
	postCompactFiles, err := ioutil.ReadDir(testBucketDir)
	if len(postCompactFiles) != STORES_PER_BUCKET+2 {
		t.Errorf("expected %v postCompactFiles, got: %v",
			STORES_PER_BUCKET+2, len(postCompactFiles))
	}
}

func TestCompactionPurgeTimeout(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions:   MAX_VBUCKETS,
			FlushInterval:   10 * time.Second,
			SleepInterval:   time.Millisecond,
			CompactInterval: 10 * time.Second,
			PurgeTimeout:    time.Millisecond,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
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

func TestCopyDelta(t *testing.T) {
	testCopyDelta(t, 1)
	testCopyDelta(t, 0x0800000)
}

func testCopyDelta(t *testing.T, writeEvery int) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions:   MAX_VBUCKETS,
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Millisecond,
			CompactInterval: 10 * time.Second,
			PurgeTimeout:    10 * time.Millisecond,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
	v0, _ := b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)
	for i := 0; i < 100; i++ {
		testLoadInts(t, r0, 2, 5)
		if err = b0.Flush(); err != nil {
			t.Errorf("expected Flush (loop) to work, got: %v", err)
		}
	}

	// Also exercise deletion codepaths.
	r0.HandleMessage(ioutil.Discard, &gomemcached.MCRequest{
		Opcode:  gomemcached.DELETE,
		VBucket: uint16(2),
		Key:     []byte(strconv.Itoa(3)),
	})
	r0.HandleMessage(ioutil.Discard, &gomemcached.MCRequest{
		Opcode:  gomemcached.DELETE,
		VBucket: uint16(2),
		Key:     []byte(strconv.Itoa(4)),
	})

	b0.SetVBState(2, VBReplica)
	b0.SetVBState(2, VBActive)

	testLoadInts(t, r0, 2, 5)

	vbid := uint16(2)

	cName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_CHANGES)
	kName := fmt.Sprintf("%v%s", vbid, COLL_SUFFIX_KEYS)

	numVisits, err := copyDelta(nil, cName, kName, v0.bs.BSF().store, v0.bs.BSF().store, writeEvery)
	if err != nil {
		t.Errorf("expected copyDelta to work, got: %v", err)
	}
	if numVisits <= 0 {
		t.Errorf("expected copyDelta numVisits to be > 0")
	}
}

func TestCopyDeltaBadNames(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions:   MAX_VBUCKETS,
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Millisecond,
			CompactInterval: 10 * time.Second,
			PurgeTimeout:    10 * time.Millisecond,
		})
	v0, _ := b0.CreateVBucket(2)

	_, err = copyDelta(nil, "foo", "bar", v0.bs.BSF().store, v0.bs.BSF().store, 0)
	if err == nil {
		t.Errorf("expected copyDelta to fail on bad coll names")
	}
}

func TestBadCompactSwapFile(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions:   MAX_VBUCKETS,
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Millisecond,
			CompactInterval: 10 * time.Second,
			PurgeTimeout:    10 * time.Millisecond,
		})
	v0, _ := b0.CreateVBucket(2)

	err = v0.bs.compactSwapFile(v0.bs.BSF(), "/thisIsABadPath")
	if err == nil {
		t.Errorf("expected compactSwapFile to fail on bad compactPath")
	}

	v0.bs.BSF().path = "/whoaAnUnexpectedPath"

	err = v0.bs.compactSwapFile(v0.bs.BSF(), testBucketDir+"foo")
	if err == nil {
		t.Errorf("expected compactSwapFile to fail on bad compactPath")
	}
}
