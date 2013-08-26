package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strconv"
	"testing"

	"github.com/dustin/gomemcached"
)

func TestCompaction(t *testing.T) {
	t.Parallel()

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
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
		res := r0.HandleMessage(ioutil.Discard, nil, req)
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

	b1, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
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

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
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
	t.Parallel()
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
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

func SKIP_TestCompactionPurgeTimeout(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
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
	b0.Close()

	// TODO: Even though there's a finalzer hook for the old
	// bucketstorefile, the old file still seems to exist after the
	// GC; so perhaps there are still references to the old
	// bucketstorefile, or it's something else.
	runtime.GC()

	postCompactFiles, err := ioutil.ReadDir(testBucketDir)
	if len(postCompactFiles) != len(preCompactFiles) {
		t.Errorf("expected purged postCompactFiles == preCompactFiles with, got: %v vs %v",
			len(postCompactFiles), len(preCompactFiles))
	}
}

func TestCopyDelta(t *testing.T) {
	t.Parallel()

	testCopyDelta(t, 1)
	testCopyDelta(t, 0x0800000)
}

func testCopyDelta(t *testing.T, writeEvery int) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
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
	r0.HandleMessage(ioutil.Discard, nil, &gomemcached.MCRequest{
		Opcode:  gomemcached.DELETE,
		VBucket: uint16(2),
		Key:     []byte(strconv.Itoa(3)),
	})

	r0.HandleMessage(ioutil.Discard, nil, &gomemcached.MCRequest{
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

	b1, err := NewBucket("test1", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	v1, _ := b1.CreateVBucket(2)
	b1.SetVBState(2, VBActive)

	numVisits, err := copyDelta(nil, cName, kName,
		v0.bs.BSF().store, v1.bs.BSF().store, writeEvery)
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

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
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

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
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

func TestRemoveOldFiles(t *testing.T) {
	d, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(d)

	var err error

	check := func(exp []string) {
		got := []string{}
		finfos, err := ioutil.ReadDir(d)
		if err != nil {
			t.Errorf("expected ReadDir(%v) to work, err: %v", d, err)
		}
		for _, finfo := range finfos {
			if finfo.IsDir() {
				continue
			}
			got = append(got, finfo.Name())
		}
		sort.Strings(got)
		if len(got) != len(exp) {
			t.Errorf("expected len(got) == len(exp), %v vs %v", got, exp)
		}
		for i, x := range exp {
			if x != got[i] {
				t.Errorf("expected got[i] == exp[i], %v, %v vs %v",
					i, got[i], exp[i])
			}
		}
	}

	ioutil.WriteFile(d+"/prefix-9.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/prefix-10.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/prefix-12.store", []byte("hi"), 0600)
	err = removeOldFiles(d, "prefix-12.store", "store")
	if err != nil {
		t.Errorf("expected removeOldFiles to work, err: %v", err)
	}
	check([]string{"prefix-12.store"})

	ioutil.WriteFile(d+"/prefix-9.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/prefix-10.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/prefix-12.store", []byte("hi"), 0600)
	err = removeOldFiles(d, "prefix-10.store", "store")
	if err != nil {
		t.Errorf("expected removeOldFiles to work, err: %v", err)
	}
	check([]string{"prefix-10.store", "prefix-12.store"})

	ioutil.WriteFile(d+"/prefix-9.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/prefix-10.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/prefix-12.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/not-9.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/prefix-9.not", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/settings.json", []byte("hi"), 0600)
	err = removeOldFiles(d, "prefix-10.store", "store")
	if err != nil {
		t.Errorf("expected removeOldFiles to work, err: %v", err)
	}
	check([]string{
		"not-9.store",
		"prefix-10.store",
		"prefix-12.store",
		"prefix-9.not",
		"settings.json"})
}

func TestCompactionViews(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	d, _, bucket := testSetupDefaultBucket(t, 1, uint16(0))
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	t.Logf("TestCompactionViews dir: %s", d)

	testSetupDDoc(t, bucket, `{
		"_id":"_design/d0",
		"language": "javascript",
		"views": {
			"v0": {
				"map": "function(doc) { emit(doc.amount, null) }"
			}
		}
    }`, nil)

	rowCount := func(startKey, endKey int) (int, string) {
		rr := httptest.NewRecorder()
		r, _ := http.NewRequest("GET",
			fmt.Sprintf("http://127.0.0.1/default/_design/d0/_view/v0?startkey=%d"+
				"&endkey=%d&stale=false", startKey, endKey), nil)
		mr.ServeHTTP(rr, r)
		if rr.Code != 200 {
			t.Errorf("expected req to 200, got: %#v, %v",
				rr, rr.Body.String())
		}
		dd := &ViewResult{}
		err := jsonUnmarshal(rr.Body.Bytes(), dd)
		if err != nil {
			t.Errorf("expected good view result, got: %v", err)
		}
		return dd.TotalRows, rr.Body.String()
	}

	count, _ := rowCount(0, 100000)

	added := 10000 // Add even more items.

	docFmt := func(i int) string {
		return fmt.Sprintf(`{"amount":%d}`, i)
	}

	for i := 10; i < added; i++ {
		a := (i%10)*(added*10) + i      // So that amount is not in order.
		x := (10-(i%10))*(added*10) + i // So that the key is not in order.
		res := SetItem(bucket, []byte(fmt.Sprintf("x-%d", x)), []byte(docFmt(a)), VBActive)
		if res == nil || res.Status != gomemcached.SUCCESS {
			t.Errorf("expected SetItem to work, got: %v", res)
		}
		count++
	}

	done := make(chan bool)

	for i := 0; i < 5; i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				c, s := rowCount(0, 10000000)
				if c != count {
					t.Errorf("expected count: %v, got: %v, rcstring: %v", count, c, s)
				}
				t.Logf("expected count: %v, got: %v", count, c)
			}
		}()
	}

	if err := bucket.Compact(); err != nil {
		t.Errorf("expected Compact to work, got: %v", err)
	}

	c, s := rowCount(0, 10000000)
	if c != count {
		t.Errorf("at end expected count: %v, got: %v, rcstring: %v\n", count, c, s)
	}

	close(done)
}
