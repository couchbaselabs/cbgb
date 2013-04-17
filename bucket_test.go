package main

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
)

var aNonDirectory = path.Join("tmp", "notadir")

func init() {
	bdir := "tmp"
	if err := os.MkdirAll(bdir, 0777); err != nil {
		panic("Can't make tmp dir")
	}
	f, err := os.Create(aNonDirectory)
	if err != nil {
		panic("Error creating file: " + err.Error())
	}
	f.Close()
}

func makeTestBucket(t *testing.T) Bucket {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	b, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Fatalf("Error creating test bucket: %v", err)
	}
	return b
}

func TestBucketRegistry(t *testing.T) {
	_, err := NewBuckets(aNonDirectory,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err == nil {
		t.Fatalf("Expected NewBuckets to fail")
	}
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	bs, err := NewBuckets(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer bs.CloseAll()

	if err != nil {
		t.Fatalf("Expected NewBuckets to succeed: %v", err)
	}
	newb, err := bs.New(DEFAULT_BUCKET_NAME, bs.settings)
	defer bs.Close(DEFAULT_BUCKET_NAME, true)
	if err != nil || newb == nil {
		t.Fatalf("Failed to create default bucket, err: %v", err)
	}
	if !newb.Available() {
		t.Fatalf("New bucket is not available.")
	}

	if b2, err := bs.New(DEFAULT_BUCKET_NAME, bs.settings); err == nil || b2 != nil {
		t.Fatalf("Created default bucket twice?")
	}

	b2 := bs.Get(DEFAULT_BUCKET_NAME)
	if b2 != newb {
		t.Fatalf("Didn't get my default bucket back.")
	}

	namesGet := bs.GetNames()
	if namesGet == nil ||
		len(namesGet) != 1 ||
		namesGet[0] != DEFAULT_BUCKET_NAME {
		t.Fatalf("Expected namesGet to have an entry, got: %#v", namesGet)
	}

	bs.Close(DEFAULT_BUCKET_NAME, true)
	if b2.Available() {
		t.Fatalf("Destroyed bucket is available.")
	}

	if bs.Get(DEFAULT_BUCKET_NAME) != nil {
		t.Fatalf("Got the default bucket after destroying it")
	}

	bs.Close(DEFAULT_BUCKET_NAME, true) // just verify we can do it again

	newb2, err := bs.New(DEFAULT_BUCKET_NAME, bs.settings)
	defer bs.Close(DEFAULT_BUCKET_NAME, true)
	if err != nil || newb2 == nil {
		t.Fatalf("Failed to create default bucket again")
	}
	if newb == newb2 {
		t.Fatalf("Returned the bucket again.")
	}

	namesGet2 := bs.GetNames()
	if namesGet2 == nil ||
		len(namesGet2) != 1 ||
		namesGet2[0] != DEFAULT_BUCKET_NAME {
		t.Fatalf("Expected namesGet to have an entry, got: %#v", namesGet2)
	}
}

// Verify the current and future bucket changes are sent.
func TestBucketNotifications(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	b, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Fatalf("Expected NewBucket() to work")
	}

	b.CreateVBucket(0)
	b.SetVBState(0, VBActive)

	bch := make(chan interface{}, 5)

	b.Subscribe(bch)
	// Short yield to wait for the subscribe to occur so we'll get
	// the messages in the order we expect during the test.  It
	// generally doesn't matter, but I verify an expected sequence
	// occurs here (normally the backfill might come slightly
	// after an immediate change).
	time.Sleep(time.Millisecond * 10)

	b.CreateVBucket(3)
	b.SetVBState(3, VBActive)
	b.DestroyVBucket(3)
	b.Unsubscribe(bch)
	b.DestroyVBucket(0)

	tests := []struct {
		vb uint16
		st VBState
	}{
		{0, VBActive},
		{3, VBActive},
		{3, VBDead},
	}

	for i, x := range tests {
		c := (<-bch).(vbucketChange)
		if c.vbid != x.vb {
			t.Fatalf("Wrong vb at %v: %v, exp %+v", i, c, x)
		}
		if c.newState != x.st {
			t.Fatalf("Wrong st at %v: {%v}, exp %v/%v",
				i, c, x.vb, x.st)
		}
	}

	select {
	case x := <-bch:
		t.Errorf("Expected no more messages, got %v", x)
	default:
	}
}

func TestNewBucket(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	nb, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Fatalf("Expected NewBucket() to work")
	}

	ch := make(chan interface{}, 2)

	nb.(*livebucket).observer.Register(ch)

	nb.CreateVBucket(3)
	nb.SetVBState(3, VBActive)
	nb.DestroyVBucket(3)

	bc := (<-ch).(vbucketChange)
	if bc.vbid != 3 || bc.newState != VBActive {
		t.Fatalf("Expected a 3/active, got %v", bc)
	}

	bc = (<-ch).(vbucketChange)
	if bc.vbid != 3 || bc.newState != VBDead {
		t.Fatalf("Expected a 3/dead, got %v", bc)
	}
}

func TestCreateDestroyVBucket(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	nb, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Fatalf("Expected NewBucket() to work")
	}

	if vb, err := nb.CreateVBucket(300); err != nil || vb == nil {
		t.Fatalf("Expected successful CreateVBucket")
	}
	if vb, err := nb.CreateVBucket(300); err == nil || vb != nil {
		t.Fatalf("Expected failed second CreateVBucket")
	}
	if !nb.DestroyVBucket(300) {
		t.Fatalf("Expected successful DestroyVBucket")
	}
	if nb.DestroyVBucket(300) {
		t.Fatalf("Expected failed second DestroyVBucket")
	}
}

func TestVBString(t *testing.T) {
	tests := map[VBState]string{
		VBState(0):          "", // panics
		VBActive:            "active",
		VBReplica:           "replica",
		VBPending:           "pending",
		VBDead:              "dead",
		VBState(VBDead + 1): "", // panics
	}

	for in, exp := range tests {
		var got string
		var err interface{}
		func() {
			defer func() { err = recover() }()
			got = in.String()
		}()

		if got != exp {
			t.Errorf("Expected %v for %v, got %v",
				exp, int(in), got)
		}

		if exp == "" {
			if err == nil {
				t.Errorf("Expected error on %v, got %v",
					int(in), got)
			}
		}
	}

	testParse := []struct {
		s        string
		expState VBState
	}{
		{"active", VBActive},
		{"replica", VBReplica},
		{"pending", VBPending},
		{"dead", VBDead},
		{"ACTIVE", VBDead},
		{"not a vbstate", VBDead},
		{"", VBDead},
	}
	for testIdx, test := range testParse {
		got := parseVBState(test.s)
		if test.expState != got {
			t.Errorf("%v - Expected %v for parseVBState '%v', got %v",
				testIdx, test.expState, test.s, got)
		}
	}
}

func TestBucketClose(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	nb, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Fatalf("Expected NewBucket() to work")
	}

	if vb, err := nb.CreateVBucket(300); err != nil || vb == nil {
		t.Fatalf("Expected successful CreateVBucket")
	}
	defer nb.DestroyVBucket(300)

	vb, _ := nb.GetVBucket(300)
	if vb == nil {
		t.Fatalf("Expected vb not returned")
	}

	nb.Close()

	vb2, _ := nb.GetVBucket(300)
	if vb2 != nil {
		t.Fatalf("Got a vbucket from a closed bucket: %v", vb2)
	}

	vb3, err := nb.CreateVBucket(200)
	if err == nil || vb3 != nil {
		t.Fatalf("Created a vbucket on a closed bucket: %v", vb3)
	}
}

func TestBucketPath(t *testing.T) {
	d, err := ioutil.TempDir("./tmp", "test")
	if err != nil {
		t.Fatalf("Expected TempDir to work, got: %v", err)
	}
	defer os.RemoveAll(d)
	b, err := NewBuckets(d,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	p, err := b.Path("hello")
	// crc32("hello") == 0x3610a686
	if err != nil {
		t.Errorf("expected b.Path() to work, got: %v", err)
	}
	if p != path.Join(d, "a6", "86", "hello-bucket") {
		t.Errorf("unepxected bucket path: %v", p)
	}
	p, err = b.Path("../bad-bucket-name")
	if err == nil {
		t.Errorf("expected bad b.Path() to error")
	}
}

func TestBucketsLoadNames(t *testing.T) {
	d, err := ioutil.TempDir("./tmp", "test")
	if err != nil {
		t.Fatalf("Expected TempDir to work, got: %v", err)
	}
	defer os.RemoveAll(d)
	b, err := NewBuckets(d,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer b.CloseAll()
	if err != nil {
		t.Fatalf("Expected NewBuckets() to work on temp dir")
	}

	names, err := b.LoadNames()
	if err != nil || len(names) != 0 {
		t.Fatalf("Expected names to be empty")
	}

	if err = os.Mkdir(d+string(os.PathSeparator)+"foo", 0777); err != nil {
		t.Fatalf("Expected mkdir to work, got: %v", err)
	}
	if err = os.Mkdir(d+string(os.PathSeparator)+"foo-bucket-NOT", 0777); err != nil {
		t.Fatalf("Expected mkdir to work, got: %v", err)
	}

	names, err = b.LoadNames()
	if err != nil || len(names) != 0 {
		t.Fatalf("Expected names to be empty")
	}
	if err = b.Load(false); err != nil {
		t.Fatalf("Expected Buckets.Load() on empty directory to work")
	}

	// crc32("foo") == 0x8c736521
	// crc32("bar") == 0x76ff8caa

	os.MkdirAll(path.Join(d, "65", "21", "foo-bucket"), 0777)
	os.MkdirAll(path.Join(d, "8c", "aa", "bar-bucket"), 0777)

	names, err = b.LoadNames()
	if err != nil || len(names) != 2 {
		t.Fatalf("Expected names to be len(2), got: %v", names)
	}

	namesGet := b.GetNames()
	if namesGet == nil || len(namesGet) != 0 {
		t.Fatalf("Expected namesGet to be empty, got: %v", namesGet)
	}
}

func TestEmptyBucketSampleStats(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	bs, _ := NewBuckets(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer bs.CloseAll()

	b, _ := bs.New("mybucket", bs.settings)
	b.(*livebucket).sampleStats(time.Now()) // Should be zeroes.

	s := b.SnapshotStats().(*BucketStatsSnapshot)
	if s.Current == nil {
		t.Errorf("Expected stats.current to be non-nil")
	}
	sInitial := &Stats{}
	if !s.Current.Equal(sInitial) {
		t.Errorf("Expected initial to be zeroed, got: %#v", s)
	}

	bss := s.BucketStore
	if bss == nil {
		t.Errorf("Expected stats.bucketStore to be non-nil")
	}
	bssInitial := &BucketStoreStats{
		Stats:      STORES_PER_BUCKET,
		NodeAllocs: bss.NodeAllocs,
	}
	if !bss.Equal(bssInitial) {
		t.Errorf("Expected GetLastBucketStoreStats() to be %#v, got: %#v",
			bssInitial, bss)
	}

	b.CreateVBucket(0)
	b.CreateVBucket(11)
	b.CreateVBucket(222)

	b.SetVBState(0, VBActive)
	b.SetVBState(11, VBActive)
	b.SetVBState(222, VBActive)

	b.(*livebucket).sampleStats(time.Now()) // Should still be zeroes.

	s = b.SnapshotStats().(*BucketStatsSnapshot)
	if s.Current == nil {
		t.Errorf("Expected current stats to be non-nil")
	}
	if !s.Current.Equal(&Stats{ItemBytes: 240}) {
		t.Errorf("Expected current stats to be zeroed, got: %#v", s.Current)
	}

	bss = s.BucketStore
	if bss == nil {
		t.Errorf("Expected bucket store stats to be non-nil")
	}
	bssNext := &BucketStoreStats{
		Stats:      STORES_PER_BUCKET,
		NodeAllocs: bss.NodeAllocs,
	}
	if !bss.Equal(bssNext) {
		t.Errorf("Expected bucket store stats to be %#v, got: %#v",
			bssNext, bss)
	}

	as := s.Agg
	if as == nil {
		t.Errorf("Expected agg stats to be non-nil")
	}
	for _, level := range as.Levels {
		if level.Stats != nil && !level.Stats.(*Stats).Equal(sInitial) {
			t.Errorf("Expected GetAggStats()[0] to be zeroed, got: %#v",
				level.Stats)
		}
	}

	abss := s.AggBucketStore
	if abss == nil {
		t.Errorf("Expected GetAggBucketStoreStats() to be non-nil")
	}
	for _, level := range abss.Levels {
		if level.Stats != nil && !level.Stats.(*BucketStoreStats).Equal(bssInitial) {
			t.Errorf("Expected GetAggBucketStoreStats()[0] to be zeroed, got: %#v",
				level.Stats)
		}
	}

	b.Close()
	b.StopStats() // Should not hang.
}

func TestMissingBucketsDir(t *testing.T) {
	d, err := ioutil.TempDir("./tmp", "test")
	b, err := NewBuckets(d,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer b.CloseAll()
	names, err := b.LoadNames()
	if err != nil || len(names) != 0 {
		t.Fatalf("Expected names to be empty")
	}
	os.RemoveAll(d)
	names, err = b.LoadNames()
	if err == nil {
		t.Fatalf("Expected names to fail on missing dir")
	}
	err = b.Load(false)
	if err == nil {
		t.Fatalf("Expected load to fail on missing dir")
	}
}

func TestBucketsLoad(t *testing.T) {
	d, err := ioutil.TempDir("./tmp", "test")
	if err != nil {
		t.Fatalf("Expected TempDir to work, got: %v", err)
	}
	defer os.RemoveAll(d)
	b, err := NewBuckets(d,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer b.CloseAll()
	if err != nil {
		t.Fatalf("Expected NewBuckets() to work on temp dir")
	}
	b.New("b1", b.settings)
	b.New("b2", b.settings)
	b.Get("b1").Flush()
	b.Get("b2").Flush()
	err = b.Load(false)
	if err == nil {
		t.Errorf("expected re-Buckets.Load() to fail")
	}

	b2, err := NewBuckets(d,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer b2.CloseAll()
	if err != nil {
		t.Fatalf("Expected NewBuckets() to work on temp dir")
	}
	err = b2.Load(false)
	if err != nil {
		t.Errorf("expected re-Buckets.Load() to fail")
	}
	if b.Get("b0") != nil {
		t.Errorf("expected Buckets.Get(b0) to fail")
	}
	if b.Get("b1") == nil {
		t.Errorf("expected Buckets.Get(b1) to work")
	}
	if b.Get("b2") == nil {
		t.Errorf("expected Buckets.Get(b2) to work")
	}
}

func TestSetVBState(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	b, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	err = b.SetVBState(0, VBActive)
	if err == nil {
		t.Errorf("expected SetVBState to fail on a missing vbucket")
	}
}

func TestShouldContinueDoingStats(t *testing.T) {
	b := makeTestBucket(t)
	defer os.RemoveAll(b.(*livebucket).dir)
	defer b.Close()
	lb := b.(*livebucket)

	// Stat request to make it appear that we're interested in
	// stats.
	b.SnapshotStats()
	if !lb.shouldContinueDoingStats(time.Now()) {
		t.Fatalf("Should keep doing stats, but won't.")
	}

	// No stats request, we can stop now.
	if lb.shouldContinueDoingStats(time.Now()) {
		t.Fatalf("Should not keep doing stats, but would.")
	}
}

func TestBucketSettingsSafeView(t *testing.T) {
	bs := &BucketSettings{
		NumPartitions:    123,
		PasswordHashFunc: "hashfunc",
		PasswordHash:     "hashbash",
		PasswordSalt:     "salty",
		QuotaBytes:       321,
		MemoryOnly:       1,
	}
	sv := bs.SafeView()
	if sv["numPartitions"].(int) != 123 ||
		sv["quotaBytes"].(int64) != 321 ||
		sv["memoryOnly"].(int) != 1 {
		t.Errorf("safe view didn't match expected: %v, got: %v", bs, sv)
	}
	if _, ok := sv["passwordHashFunc"]; ok {
		t.Errorf("safe view should not expose password hashfunc, got: %v", sv)
	}
	if _, ok := sv["passwordHash"]; ok {
		t.Errorf("safe view should not expose password hash, got: %v", sv)
	}
	if _, ok := sv["passwordSalt"]; ok {
		t.Errorf("safe view should not expose password salt, got: %v", sv)
	}
}

func TestMemoryOnlyLevel1Bucket(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
			MemoryOnly:    MemoryOnly_LEVEL_PERSIST_METADATA,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}

	r0 := &reqHandler{currentBucket: b0}
	vb0, _ := b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)

	if 0 != vb0.stats.Items {
		t.Errorf("expected to have 0 items initially")
	}

	testLoadInts(t, r0, 2, 5)
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "initial data load")

	if 5 != vb0.stats.Items {
		t.Errorf("expected to have 5 items")
	}

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	if 5 != vb0.stats.Items {
		t.Errorf("expected to have 5 items still after flushing")
	}

	b0.Close()

	b1, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
			MemoryOnly:    MemoryOnly_LEVEL_PERSIST_METADATA,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b1.Close()

	b1.Load()
	vb1, _ := b1.GetVBucket(2)
	if vb1 == nil {
		t.Errorf("expected vbucket 2 to be there after re-load")
	}
	vbs1 := vb1.GetVBState()
	if vbs1 != VBActive {
		t.Errorf("expected vbucket 2 to be active, got: %v", vbs1)
	}
	if 0 != vb1.stats.Items {
		t.Errorf("expected to have 0 items after loading, got: %v", vb1.stats.Items)
	}
	r1 := &reqHandler{currentBucket: b1}
	testExpectInts(t, r1, 2, []int{}, "data re-load")
}

func TestMemoryOnlyBucketSettingsReload(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	buckets0, err := NewBuckets(testBucketDir,
		&BucketSettings{NumPartitions: MAX_VBUCKETS})
	if err != nil {
		t.Fatalf("Expected NewBuckets to succeed: %v", err)
	}
	defer buckets0.CloseAll()

	b0, err := buckets0.New("foo",
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
			MemoryOnly:    MemoryOnly_LEVEL_PERSIST_METADATA,
		})
	b0.Flush()
	b0.Close()

	buckets1, err := NewBuckets(testBucketDir,
		&BucketSettings{NumPartitions: MAX_VBUCKETS})
	if err != nil {
		t.Fatalf("Expected NewBuckets to succeed: %v", err)
	}
	defer buckets1.CloseAll()

	err = buckets1.Load(false)
	if err != nil {
		t.Errorf("expected buckets reload to work, got: %v", err)
	}

	b1 := buckets1.Get("foo")
	if b1 == nil {
		t.Errorf("expected metadata-persisted bucket to survive restart")
	}
	if b1.GetBucketSettings().MemoryOnly != MemoryOnly_LEVEL_PERSIST_METADATA {
		t.Errorf("expected foo bucket to have same mem-only setting, got: %#v", b1)
	}
	if b1.GetBucketSettings().NumPartitions != MAX_VBUCKETS {
		t.Errorf("expected foo bucket to have same partitions setting, got: %#v", b1)
	}
}

func TestPersistNothing(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	buckets0, err := NewBuckets(testBucketDir,
		&BucketSettings{NumPartitions: MAX_VBUCKETS})
	if err != nil {
		t.Fatalf("Expected NewBuckets to succeed: %v", err)
	}
	defer buckets0.CloseAll()

	b0, err := buckets0.New("foo",
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
			MemoryOnly:    MemoryOnly_LEVEL_PERSIST_NOTHING,
		})
	b0.Flush()
	b0.Close()

	buckets1, err := NewBuckets(testBucketDir,
		&BucketSettings{NumPartitions: MAX_VBUCKETS})
	if err != nil {
		t.Fatalf("Expected NewBuckets to succeed: %v", err)
	}
	defer buckets1.CloseAll()

	err = buckets1.Load(false)
	if err != nil {
		t.Errorf("expected buckets reload to work, got: %v", err)
	}

	b1 := buckets1.Get("foo")
	if b1 != nil {
		t.Errorf("expected persist-nothing bucket disappear at restart")
	}
}

func TestBucketQuotaBytes(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	quota := int64(1000)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
			QuotaBytes:    quota,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b0.Close()

	b0_numBytes0 := b0.GetItemBytes()
	if b0_numBytes0 >= quota {
		t.Errorf("reached quota earlier than expected")
	}

	r0 := &reqHandler{currentBucket: b0}
	vb0, _ := b0.CreateVBucket(2)

	b0_numBytes1 := b0.GetItemBytes()
	if b0_numBytes1 >= quota {
		t.Errorf("reached quota earlier than expected")
	}

	b0.SetVBState(2, VBActive)

	b0_numBytes2 := b0.GetItemBytes()
	if b0_numBytes2 >= quota {
		t.Errorf("reached quota earlier than expected")
	}

	testLoadInts(t, r0, 2, 1)
	testExpectInts(t, r0, 2, []int{0}, "initial data load")

	if 1 != vb0.stats.Items {
		t.Errorf("expected to have 5 items")
	}
	b0_numBytes3 := b0.GetItemBytes()
	if b0_numBytes3 >= quota {
		t.Errorf("reached quota earlier than expected")
	}

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	if 1 != vb0.stats.Items {
		t.Errorf("expected to have 5 items still after flushing")
	}
	b0_numBytes4 := b0.GetItemBytes()
	if b0_numBytes4 >= quota {
		t.Errorf("reached quota earlier than expected")
	}

	res := r0.HandleMessage(ioutil.Discard, nil, &gomemcached.MCRequest{
		Opcode:  gomemcached.SET,
		VBucket: 2,
		Key:     []byte("toobig"),
		Body:    make([]byte, 2000),
	})

	if res.Status != gomemcached.E2BIG {
		t.Errorf("expected to have reached quota, got: %v", res)
	}

	res = r0.HandleMessage(ioutil.Discard, nil, &gomemcached.MCRequest{
		Opcode:  gomemcached.SET,
		VBucket: 2,
		Key:     []byte("shouldfit"),
		Body:    make([]byte, 100),
	})

	if res.Status != gomemcached.SUCCESS {
		t.Errorf("expected to have not reached quota, got: %v", res)
	}
}

func TestReloadOnlyNewDirectory(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	buckets0, err := NewBuckets(testBucketDir,
		&BucketSettings{NumPartitions: MAX_VBUCKETS})
	if err != nil {
		t.Fatalf("Expected NewBuckets to succeed: %v", err)
	}
	defer buckets0.CloseAll()

	b0, err := buckets0.New("foo",
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)
	r0 := &reqHandler{currentBucket: b0}
	testLoadInts(t, r0, 2, 5)
	b0.Flush()
	b0.Close()

	buckets1, err := NewBuckets(testBucketDir,
		&BucketSettings{NumPartitions: MAX_VBUCKETS})
	if err != nil {
		t.Fatalf("Expected NewBuckets to succeed: %v", err)
	}
	defer buckets1.CloseAll()

	err = buckets1.Load(true)
	if err != nil {
		t.Errorf("expected buckets reload to work, got: %v", err)
	}
	b1 := buckets1.Get("foo")
	if b1 == nil {
		t.Errorf("expected bucket foo to be there")
	}

	err = buckets1.Load(true) // Load again.
	if err != nil {
		t.Errorf("expected buckets reload to work, got: %v", err)
	}
	b1x := buckets1.Get("foo")
	if b1x == nil {
		t.Errorf("expected bucket foo to be there")
	}
	if b1x != b1 {
		t.Errorf("expected bucket foo to be bucket foo")
	}

	// Copy all files from foo dir to bar dir.
	fooPath, _ := buckets1.Path("foo")
	barPath, _ := buckets1.Path("bar")
	os.MkdirAll(barPath, 0777)
	filepath.Walk(fooPath, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		sf, err := os.Open(p)
		if err != nil {
			return err
		}
		defer sf.Close()
		df, err := os.OpenFile(path.Join(barPath, path.Base(p)),
			os.O_TRUNC|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		defer df.Close()
		_, err = io.Copy(df, sf)
		return err
	})

	err = buckets1.Load(true) // Load again.
	if err != nil {
		t.Errorf("expected buckets reload to work, got: %v", err)
	}
	bx := buckets1.Get("bar")
	if bx == nil {
		t.Errorf("expected bar dir to be found")
	}
	b1x = buckets1.Get("foo")
	if b1x == nil {
		t.Errorf("expected bucket foo to be there")
	}
	if b1x != b1 {
		t.Errorf("expected bucket foo to be bucket foo")
	}
}

func TestBucketMakeCloser(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	buckets, _ := NewBuckets(testBucketDir,
		&BucketSettings{NumPartitions: MAX_VBUCKETS})

	c := buckets.makeCloser("not-a-bucket")
	if c == nil {
		t.Errorf("expected makeCloser() to work")
	}
	x := c(time.Now())
	if x {
		t.Errorf("expected closer to be false")
	}
}

func TestBucketMaybeClose(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	buckets, _ := NewBuckets(testBucketDir,
		&BucketSettings{NumPartitions: MAX_VBUCKETS})
	b0, _ := buckets.New("foo", &BucketSettings{NumPartitions: MAX_VBUCKETS})

	lb := b0.(*livebucket)
	lb.activity = int64(1234)

	x := buckets.maybeClose("foo")
	if x {
		t.Errorf("expected active foo to not be closed")
	}
	x = buckets.maybeClose("foo")
	if !x {
		t.Errorf("expected foo to be closed")
	}
	x = buckets.maybeClose("foo")
	if !x {
		t.Errorf("expected foo to still be closed")
	}
}
