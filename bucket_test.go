package cbgb

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestBucketRegistry(t *testing.T) {
	_, err := NewBuckets("./this-is-not-a-directory",
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err == nil {
		t.Fatalf("Expected NewBuckets to fail")
	}
	bs, err := NewBuckets("./tmp",
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Fatalf("Expected NewBuckets to succeed: %v", err)
	}
	newb, err := bs.New(DEFAULT_BUCKET_NAME)
	defer bs.Close(DEFAULT_BUCKET_NAME, true)
	if err != nil || newb == nil {
		t.Fatalf("Failed to create default bucket")
	}
	if !newb.Available() {
		t.Fatalf("New bucket is not available.")
	}

	if b2, err := bs.New(DEFAULT_BUCKET_NAME); err == nil || b2 != nil {
		t.Fatalf("Created default bucket twice?")
	}

	b2 := bs.Get(DEFAULT_BUCKET_NAME)
	if b2 != newb {
		t.Fatalf("Didn't get my default bucket back.")
	}

	bs.Close(DEFAULT_BUCKET_NAME, true)
	if b2.Available() {
		t.Fatalf("Destroyed bucket is available.")
	}

	if bs.Get(DEFAULT_BUCKET_NAME) != nil {
		t.Fatalf("Got the default bucket after destroying it")
	}

	bs.Close(DEFAULT_BUCKET_NAME, true) // just verify we can do it again

	newb2, err := bs.New(DEFAULT_BUCKET_NAME)
	defer bs.Close(DEFAULT_BUCKET_NAME, true)
	if err != nil || newb2 == nil {
		t.Fatalf("Failed to create default bucket again")
	}
	if newb == newb2 {
		t.Fatalf("Returned the bucket again.")
	}
}

// Verify the current and future bucket changes are sent.
func TestBucketNotifications(t *testing.T) {
	b, err := NewBucket("tmp",
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
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
	b.destroyVBucket(3)
	b.Unsubscribe(bch)
	b.destroyVBucket(0)

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
	nb, err := NewBucket("tmp",
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Fatalf("Expected NewBucket() to work")
	}

	ch := make(chan interface{}, 2)

	nb.(*livebucket).observer.Register(ch)

	nb.CreateVBucket(3)
	nb.SetVBState(3, VBActive)
	nb.destroyVBucket(3)

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
	nb, err := NewBucket("tmp",
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
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
	if !nb.destroyVBucket(300) {
		t.Fatalf("Expected successful destroyVBucket")
	}
	if nb.destroyVBucket(300) {
		t.Fatalf("Expected failed second destroyVBucket")
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
	nb, err := NewBucket("tmp",
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Fatalf("Expected NewBucket() to work")
	}

	if vb, err := nb.CreateVBucket(300); err != nil || vb == nil {
		t.Fatalf("Expected successful CreateVBucket")
	}
	defer nb.destroyVBucket(300)

	vb := nb.getVBucket(300)
	if vb == nil {
		t.Fatalf("Expected vb not returned")
	}

	nb.Close()

	vb2 := nb.getVBucket(300)
	if vb2 != nil {
		t.Fatalf("Got a vbucket from a closed bucket: %v", vb2)
	}

	vb3, err := nb.CreateVBucket(200)
	if err == nil || vb3 != nil {
		t.Fatalf("Created a vbucket on a closed bucket: %v", vb3)
	}
}

func TestBucketLoadNames(t *testing.T) {
	d, err := ioutil.TempDir(os.TempDir(), "cbgb-TestBucketLoadNames")
	if err != nil {
		t.Fatalf("Expected TempDir to work, got: %v", err)
	}

	t.Logf("tmpdir %v", d)
	b, err := NewBuckets(d,
		&BucketSettings{
			FlushInterval:   time.Second,
			SleepInterval:   time.Second,
			CompactInterval: 10 * time.Second,
		})
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
	if err = b.Load(); err != nil {
		t.Fatalf("Expected Buckets.Load() on empty directory to work")
	}

	os.Mkdir(d+string(os.PathSeparator)+"foo-bucket", 0777)
	os.Mkdir(d+string(os.PathSeparator)+"bar-bucket", 0777)

	names, err = b.LoadNames()
	if err != nil || len(names) != 2 {
		t.Fatalf("Expected names to be len(2), got: %v", names)
	}
}
