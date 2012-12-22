package cbgb

import (
	"io/ioutil"
	"log"
	"testing"
	"time"
)

// Don't do any normal logging while running tests.
func init() {
	log.SetOutput(ioutil.Discard)
}

// Verify the current and future bucket changes are sent.
func TestBucketNotifications(t *testing.T) {
	b := NewBucket()
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
	b.Observer().Unregister(bch)
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
	nb := NewBucket()

	ch := make(chan interface{}, 2)

	nb.observer.Register(ch)

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
	nb := NewBucket()

	if nb.CreateVBucket(300) == nil {
		t.Fatalf("Expected successful CreateVBucket")
	}
	if nb.CreateVBucket(300) != nil {
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
}

func TestVBSuspend(t *testing.T) {
	nb := NewBucket()

	if nb.CreateVBucket(300) == nil {
		t.Fatalf("Expected successful CreateVBucket")
	}
	defer nb.destroyVBucket(300)

	vb := nb.getVBucket(300)
	vb.SetVBState(VBActive, nil)

	// Verify we can get a thing
	x := vb.get([]byte{'x'})
	if x != nil {
		t.Fatalf("Expected nil in x req, got %v", x)
	}

	// Verify the current state
	st := vb.GetVBState()
	if st != VBActive {
		t.Fatalf("Expected state active, got %v", st)
	}

	// Now suspend it.  Only state changes can occur and be reflected.
	vb.Suspend()

	// At this point, we're going to asynchronously try to grab a
	// value It should hang until we resume the vbucket state.
	ch := make(chan *item)
	go func() { ch <- vb.get([]byte{'x'}) }()

	// Verify there is no item ready.
	select {
	default:
	case x = <-ch:
		t.Fatalf("Expected no item ready, got %v", x)
	}

	// However, state changes are fine.
	vb.SetVBState(VBReplica, nil)
	st = vb.GetVBState()
	if st != VBReplica {
		t.Fatalf("Expected state replica, got %v", st)
	}

	// We've seen messages processed, but we're still not getting our value.
	select {
	default:
	case x = <-ch:
		t.Fatalf("Expected no item ready, got %v", x)
	}

	// But once we resume our vbucket, data should be pretty much immediately ready.
	vb.Resume()

	x = <-ch
	if x != nil {
		t.Fatalf("Expected nil in x req, got %v", x)
	}
}
