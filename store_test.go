package cbgb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
)

type brokenFile struct {
	error
}

func (b brokenFile) ReadAt([]byte, int64) (int, error) {
	return 0, b.error
}

func (b brokenFile) WriteAt([]byte, int64) (int, error) {
	return 0, b.error
}

func (b brokenFile) Close() error {
	return b.error
}

func (b brokenFile) Stat() (os.FileInfo, error) {
	return nil, b.error
}

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
		if i >= len(results) {
			return
		}
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

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b0.Close()

	r0 := &reqHandler{currentBucket: b0}
	b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)

	testLoadInts(t, r0, 2, 0)
	testExpectInts(t, r0, 2, []int{}, "initial data load")

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testExpectInts(t, r0, 2, []int{}, "after flush")

	b1, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket re-open to work, err: %v", err)
	}
	defer b1.Close()
	r1 := &reqHandler{currentBucket: b1}
	err = b1.Load()
	if err != nil {
		t.Errorf("expected Load to work, err: %v", err)
	}
	testExpectInts(t, r1, 2, []int{}, "reload")
}

func TestSaveLoadBasic(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
	b0.CreateVBucket(2)
	if b0.SetVBState(2, VBActive) != nil {
		t.Errorf("expected SetVBState to work")
	}

	testLoadInts(t, r0, 2, 5)
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "initial data load")

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "after flush")

	b1, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket re-open to work, err: %v", err)
	}
	defer b1.Close()
	r1 := &reqHandler{currentBucket: b1}
	err = b1.Load()
	if err != nil {
		t.Errorf("expected Load to work, err: %v", err)
	}
	testExpectInts(t, r1, 2, []int{0, 1, 2, 3, 4}, "reload")
}

func TestSaveLoadMutations(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
	b0.CreateVBucket(2)
	if b0.SetVBState(2, VBActive) != nil {
		t.Errorf("expected SetVBState to work")
	}

	testLoadInts(t, r0, 2, 5)
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "initial data load")

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "after flush")

	b0.Close()

	b1, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket re-open to work, err: %v", err)
	}
	r1 := &reqHandler{currentBucket: b1}
	err = b1.Load()
	if err != nil {
		t.Errorf("expected Load to work, err: %v", err)
	}

	vb1 := b1.GetVBucket(2)
	if vb1.Meta().LastCas != 6 {
		t.Errorf("expected reloaded LastCas to be 6, got %v", vb1.Meta().LastCas)
	}

	testExpectInts(t, r1, 2, []int{0, 1, 2, 3, 4}, "reload")

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
		res := r1.HandleMessage(ioutil.Discard, req)
		if res.Status != gomemcached.SUCCESS {
			t.Errorf("Expected %v for %v:%v/%v, got %v",
				gomemcached.SUCCESS, x.op, x.vb, x.key, res.Status)
		}
	}

	testExpectInts(t, r1, 2, []int{1, 2, 3, 5}, "before flush")

	err = b1.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	testExpectInts(t, r1, 2, []int{1, 2, 3, 5}, "after flush")

	bss1 := vb1.bs.Stats()
	if bss1 == nil {
		t.Errorf("expected bucket store to have Stats()")
	}
	if bss1.Flushes != 1 {
		t.Errorf("expected bss1 to have 1 Flushes")
	}
	if bss1.Reads == 0 {
		t.Errorf("expected bss1 to have >0 Reads")
	}
	if bss1.Writes == 0 {
		t.Errorf("expected bss1 to have >0 Writes")
	}
	if bss1.Stats == 0 {
		t.Errorf("expected bss1 to have >0 Stats")
	}
	if bss1.FlushErrors != 0 {
		t.Errorf("expected bss1 to have 0 FlushErrors")
	}
	if bss1.ReadErrors != 0 {
		t.Errorf("expected bss1 to have 0 ReadErrors")
	}
	if bss1.WriteErrors != 0 {
		t.Errorf("expected bss1 to have 0 WriteErrors")
	}
	if bss1.StatErrors != 0 {
		t.Errorf("expected bss1 to have 0 StatErrors")
	}
	if bss1.ReadBytes == 0 {
		t.Errorf("expected bss1 to have >0 ReadBytes")
	}
	if bss1.WriteBytes == 0 {
		t.Errorf("expected bss1 to have >0 WriteBytes")
	}

	b1.Close()

	b2, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket re-open to work, err: %v", err)
	}
	defer b2.Close()
	r2 := &reqHandler{currentBucket: b2}
	err = b2.Load()
	if err != nil {
		t.Errorf("expected Load to work, err: %v", err)
	}

	testExpectInts(t, r2, 2, []int{1, 2, 3, 5}, "reload2")

	vb2 := b2.GetVBucket(2)
	if vb2.Meta().LastCas != 11 {
		t.Errorf("expected reloaded LastCas to be 11, got %v", vb2.Meta().LastCas)
	}

	bss2 := vb2.bs.Stats()
	if bss2 == nil {
		t.Errorf("expected bucket store to have Stats()")
	}
	if bss2.Flushes != 0 {
		t.Errorf("expected bss2 to have 0 Flushes")
	}
	if bss2.Reads == 0 {
		t.Errorf("expected bss2 to have >0 Reads")
	}
	if bss2.Writes != 0 {
		t.Errorf("expected bss2 to have 0 Writes")
	}
	if bss2.Stats == 0 {
		t.Errorf("expected bss2 to have >0 Stats")
	}
	if bss2.FlushErrors != 0 {
		t.Errorf("expected bss2 to have 0 FlushErrors")
	}
	if bss2.ReadErrors != 0 {
		t.Errorf("expected bss2 to have 0 ReadErrors")
	}
	if bss2.WriteErrors != 0 {
		t.Errorf("expected bss2 to have 0 WriteErrors")
	}
	if bss2.StatErrors != 0 {
		t.Errorf("expected bss2 to have 0 StatErrors")
	}
	if bss2.ReadBytes == 0 {
		t.Errorf("expected bss2 to have >0 ReadBytes")
	}
	if bss2.WriteBytes != 0 {
		t.Errorf("expected bss2 to have 0 WriteBytes")
	}
}

func TestSaveLoadVBState(t *testing.T) {
	testSaveLoadVBState(t, false)
	testSaveLoadVBState(t, true)
}

func testSaveLoadVBState(t *testing.T, withData bool) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
	b0.CreateVBucket(2)
	if b0.SetVBState(2, VBActive) != nil {
		t.Errorf("expected SetVBState to work")
	}

	if withData {
		testLoadInts(t, r0, 2, 5)
	}

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	b0.Close()

	tests := []struct {
		currState VBState
		nextState VBState
	}{
		{VBActive, VBReplica},
		{VBReplica, VBPending},
		{VBPending, VBDead},
		{VBDead, VBActive},
	}

	for _, test := range tests {
		b1, err := NewBucket(testBucketDir,
			&BucketSettings{
				FlushInterval:   time.Second,
				SleepInterval:   time.Second,
				CompactInterval: 10 * time.Second,
			})
		if err != nil {
			t.Errorf("expected NewBucket re-open to work, err: %v", err)
		}
		r1 := &reqHandler{currentBucket: b1}
		err = b1.Load()
		if err != nil {
			t.Errorf("expected Load to work, err: %v", err)
		}
		vb := b1.GetVBucket(2)
		if vb == nil {
			t.Errorf("expected vbucket")
		}
		vbs := vb.GetVBState()
		if vbs != test.currState {
			t.Errorf("expected vbstate %v, got %v", test.currState, vbs)
		}
		if b1.SetVBState(2, test.nextState) != nil {
			t.Errorf("expected SetVBState to work")
		}
		if err = b1.Flush(); err != nil {
			t.Errorf("expected flush to work, got: %v", err)
		}
		if withData {
			testExpectInts(t, r1, 2, []int{0, 1, 2, 3, 4}, "reload2")
		}
	}
}

func TestFlushCloseInterval(t *testing.T) {
	testFlushCloseInterval(t, time.Millisecond, time.Second)
	testFlushCloseInterval(t, time.Millisecond, time.Millisecond)
	testFlushCloseInterval(t, 2*time.Millisecond, time.Millisecond)
	testFlushCloseInterval(t, time.Millisecond, 2*time.Millisecond)
}

func testFlushCloseInterval(t *testing.T,
	flushInterval time.Duration,
	sleepInterval time.Duration) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   flushInterval,
			SleepInterval:   sleepInterval,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
	b0.CreateVBucket(2)
	if b0.SetVBState(2, VBActive) != nil {
		t.Errorf("expected SetVBState to work")
	}

	testLoadInts(t, r0, 2, 5)
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "initial data load")

	// We don't do an explicit Flush() here.  Instead, we sleep longer
	// than the flushInterval so there's a background Flush() for us.
	time.Sleep(10 * time.Millisecond)

	b0.Close()

	b1, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket re-open to work, err: %v", err)
	}
	r1 := &reqHandler{currentBucket: b1}
	err = b1.Load()
	if err != nil {
		t.Errorf("expected Load to work, err: %v", err)
	}
	testExpectInts(t, r1, 2, []int{0, 1, 2, 3, 4}, "reload")
}

func TestSleepInterval(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Millisecond,
			SleepInterval:   time.Millisecond,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
	b0.CreateVBucket(2)
	if b0.SetVBState(2, VBActive) != nil {
		t.Errorf("expected SetVBState to work")
	}

	testLoadInts(t, r0, 2, 5)
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "initial data load")

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	testLoadInts(t, r0, 2, 1)
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "data")

	b0.Close()

	testWithFlushInterval := func(flushInterval time.Duration) {
		b1, err := NewBucket(testBucketDir,
			&BucketSettings{
				FlushInterval:   flushInterval,
				SleepInterval:   time.Millisecond,
				CompactInterval: 10 * time.Second,
			})
		if err != nil {
			t.Errorf("expected NewBucket re-open to work, err: %v", err)
		}
		r1 := &reqHandler{currentBucket: b1}

		err = b1.Load()
		if err != nil {
			t.Errorf("expected Load to work, err: %v", err)
		}

		vb1 := b1.GetVBucket(2)

		bss1before := vb1.bs.Stats()
		if bss1before == nil {
			t.Errorf("expected bucket store to have before Stats()")
		}

		time.Sleep(10 * time.Millisecond)

		testExpectInts(t, r1, 2, []int{0, 1, 2, 3, 4}, "reload")

		bss1 := vb1.bs.Stats()
		if bss1 == nil {
			t.Errorf("expected bucket store to have Stats()")
		}
		if bss1.Sleeps != 1+bss1before.Sleeps {
			t.Errorf("expected bss1 to have %v Sleeps, got %v",
				1+bss1before.Sleeps, bss1.Sleeps)
		}
		if bss1.Wakes != 1+bss1before.Wakes {
			t.Errorf("expected bss1 to have %v Wakes, got %v",
				1+bss1before.Wakes, bss1.Wakes)
		}
		if bss1.WakeErrors != bss1before.WakeErrors {
			t.Errorf("expected bss1 to have %v WakeErrors, got %v",
				bss1before.Wakes, bss1.Wakes)
		}

		b1.Close()
	}

	testWithFlushInterval(1 * time.Millisecond)
	testWithFlushInterval(20 * time.Millisecond)
}

func TestLatestStoreFiles(t *testing.T) {
	d, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(d)

	checkNames := func(msg string, got []string, exp []string) {
		if len(got) != len(exp) {
			t.Errorf("%v - expected %v, got %v", msg, exp, got)
		}
		for i, v := range exp {
			if got[i] != v {
				t.Errorf("%v - expected %v, got %v", msg, v, got[i])
			}
		}
	}

	f, err := latestStoreFileNames(d, 4)
	if err != nil {
		t.Errorf("expected latestStoreFileNames to work, err: %v", err)
	}
	checkNames("empty", f,
		[]string{"0-0.store", "1-0.store", "2-0.store", "3-0.store"})

	ioutil.WriteFile(d+"/0-1234.store", []byte("hi"), 0600)
	f, err = latestStoreFileNames(d, 4)
	if err != nil {
		t.Errorf("expected latestStoreFileNames to work, err: %v", err)
	}
	checkNames("one file", f,
		[]string{"0-1234.store", "1-0.store", "2-0.store", "3-0.store"})

	ioutil.WriteFile(d+"/0-234.store", []byte("hi"), 0600)
	f, err = latestStoreFileNames(d, 4)
	if err != nil {
		t.Errorf("expected latestStoreFileNames to work, err: %v", err)
	}
	checkNames("one shadowed file", f,
		[]string{"0-1234.store", "1-0.store", "2-0.store", "3-0.store"})

	ioutil.WriteFile(d+"/1-1.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/2-0.store.not", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/2-.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/-.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/-100.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/3-1-1.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/4-0.store", []byte("hi"), 0600)
	ioutil.WriteFile(d+"/4-0.store", []byte("hi"), 0600)
	f, err = latestStoreFileNames(d, 4)
	if err != nil {
		t.Errorf("expected latestStoreFileNames to work, err: %v", err)
	}
	checkNames("many files", f,
		[]string{"0-1234.store", "1-1.store", "2-0.store", "3-0.store"})
}

func TestStoreFiles(t *testing.T) {
	f := makeStoreFileName(0, 0)
	if f != "0-0.store" {
		t.Errorf("expected 0-0.store, got %v", f)
	}

	idx, ver, err := parseStoreFileName("0-0.store")
	if err != nil {
		t.Errorf("expected no err, got %v", err)
	}
	if idx != 0 || ver != 0 {
		t.Errorf("expected 0-0, got %v-%v", idx, ver)
	}

	idx, ver, err = parseStoreFileName("0-0.store.bak")
	if err == nil {
		t.Errorf("expected err")
	}

	idx, ver, err = parseStoreFileName("-.store")
	if err == nil {
		t.Errorf("expected err")
	}

	idx, ver, err = parseStoreFileName("0-0-.store")
	if err == nil {
		t.Errorf("expected err")
	}
}

func TestFlushError(t *testing.T) {
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
	defer b0.Close()

	r0 := &reqHandler{currentBucket: b0}
	v0, _ := b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)

	testLoadInts(t, r0, 2, 0)
	testExpectInts(t, r0, 2, []int{}, "initial data load")

	if v0.bs.dirtiness == 0 {
		t.Errorf("expected dirtiness")
	}

	v0.bs.BSF().file = brokenFile{fmt.Errorf("I'm broken")}

	err = b0.Flush()
	if err == nil {
		t.Errorf("expected flush error")
	}
	if v0.bs.Stats().FlushErrors != 1 {
		t.Errorf("expected 1 flusherrors")
	}
	if v0.bs.Stats().Flushes != 0 {
		t.Errorf("expected 0 flushes")
	}
	if v0.bs.dirtiness == 0 {
		t.Errorf("expected still be dirty")
	}
}

func TestStatError(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b0.Close()

	v0, _ := b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)

	v0.bs.BSF().file = brokenFile{fmt.Errorf("I'm broken")}

	prevStats := v0.bs.Stats().Stats
	prevStatErrors := v0.bs.Stats().StatErrors

	fi, err := v0.bs.BSF().Stat()
	if err == nil {
		t.Errorf("expected Stat() to fail on a closed file")
	}
	if fi != nil {
		t.Errorf("expected Stat() to give nil fileinfo on a closed file")
	}
	if v0.bs.Stats().Stats != prevStats+1 {
		t.Errorf("expected stats to be higher")
	}
	if v0.bs.Stats().StatErrors != prevStatErrors+1 {
		t.Errorf("expected staterrors to be higher")
	}
}

func TestReadAtError(t *testing.T) {
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
	defer b0.Close()

	v0, _ := b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)

	prevReads := v0.bs.Stats().Reads
	prevReadErrors := v0.bs.Stats().ReadErrors

	n, err := v0.bs.BSF().ReadAt(make([]byte, 10), -10000)
	if err == nil {
		t.Errorf("expected ReadAt() to fail")
	}
	if n != 0 {
		t.Errorf("expected ReadAt() to read nothing")
	}
	if v0.bs.Stats().Reads != prevReads+1 {
		t.Errorf("expected reads to be higher")
	}
	if v0.bs.Stats().ReadErrors != prevReadErrors+1 {
		t.Errorf("expected readerrors to be higher")
	}

	v0.bs.BSF().file = brokenFile{fmt.Errorf("I'm broken")}

	prevReads = v0.bs.Stats().Reads
	prevReadErrors = v0.bs.Stats().ReadErrors

	n, err = v0.bs.BSF().ReadAt(make([]byte, 10), 0)
	if err == nil {
		t.Errorf("expected ReadAt() to fail")
	}
	if n != 0 {
		t.Errorf("expected ReadAt() to read nothing")
	}
	if v0.bs.Stats().Reads != prevReads+1 {
		t.Errorf("expected reads to be higher")
	}
	if v0.bs.Stats().ReadErrors != prevReadErrors+1 {
		t.Errorf("expected readerrors to be higher")
	}
}
