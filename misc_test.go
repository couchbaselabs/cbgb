package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
)

// Don't do any normal logging while running tests.
func init() {
	log.SetOutput(ioutil.Discard)
}

// Exercise the mutation logger code. Output is not examined.
func TestMutationLogger(t *testing.T) {
	b := newBucket()
	b.createVBucket(0)

	ch := make(chan interface{}, 10)
	ch <- bucketChange{bucket: b, vbid: 0, oldState: vbDead, newState: vbActive}
	ch <- mutation{deleted: false, key: []byte("a"), cas: 0}
	ch <- mutation{deleted: true, key: []byte("a"), cas: 0}
	ch <- mutation{deleted: false, key: []byte("a"), cas: 2}
	ch <- bucketChange{bucket: b, vbid: 0, oldState: vbActive, newState: vbDead}
	close(ch)

	mutationLogger(ch)
}

// Verify the current and future bucket changes are sent.
func TestBucketNotifications(t *testing.T) {
	b := newBucket()
	b.createVBucket(0)
	b.setVBState(0, vbActive)

	bch := make(chan interface{}, 5)

	b.Subscribe(bch)
	// Short yield to wait for the subscribe to occur so we'll get
	// the messages in the order we expect during the test.  It
	// generally doesn't matter, but I verify an expected sequence
	// occurs here (normally the backfill might come slightly
	// after an immediate change).
	time.Sleep(time.Millisecond * 10)

	b.createVBucket(3)
	b.setVBState(3, vbActive)
	b.destroyVBucket(3)
	b.observer.Unregister(bch)
	b.destroyVBucket(0)

	tests := []struct {
		vb uint16
		st vbState
	}{
		{0, vbActive},
		{3, vbActive},
		{3, vbDead},
	}

	for i, x := range tests {
		c := (<-bch).(bucketChange)
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

func TestMutationInvalid(t *testing.T) {
	defer func() {
		if x := recover(); x == nil {
			t.Fatalf("Expected panic, didn't get it")
		} else {
			t.Logf("Got expected panic in invalid mutation: %v", x)
		}
	}()

	ch := make(chan interface{}, 5)
	// Notification of a non-existence bucket is a null lookup.
	ch <- bucketChange{vbid: 0, oldState: vbDead, newState: vbActive}
	// But this is crazy stupid and will crash the logger.
	ch <- 19

	mutationLogger(ch)
}

// Run through the sessionLoop code with a quit command.
//
// This test doesn't do much other than confirm that the session loop
// actually would terminate the real session goroutine on quit (by
// completing).
func TestSessionLoop(t *testing.T) {
	req := &gomemcached.MCRequest{
		Opcode: gomemcached.QUIT,
	}

	rh := &reqHandler{}

	req.Bytes()
	sessionLoop(rwCloser{bytes.NewBuffer(req.Bytes())}, "test", rh)
}

func TestNewBucket(t *testing.T) {
	nb := newBucket()

	ch := make(chan interface{}, 2)

	nb.observer.Register(ch)

	nb.createVBucket(3)
	nb.setVBState(3, vbActive)
	nb.destroyVBucket(3)

	bc := (<-ch).(bucketChange)
	if bc.vbid != 3 || bc.newState != vbActive {
		t.Fatalf("Expected a 3/active, got %v", bc)
	}

	bc = (<-ch).(bucketChange)
	if bc.vbid != 3 || bc.newState != vbDead {
		t.Fatalf("Expected a 3/dead, got %v", bc)
	}
}

func TestListener(t *testing.T) {
	b := newBucket()
	l, err := startMCServer("0.0.0.0:0", b)
	if err != nil {
		t.Fatalf("Error starting listener: %v", err)
	}
	t.Logf("Test server listening to %v", l.Addr())

	// Just to be extra ridiculous, dial it.
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	req := &gomemcached.MCRequest{Opcode: gomemcached.QUIT}
	_, err = c.Write(req.Bytes())
	if err != nil {
		t.Fatalf("Error sending hangup request.")
	}

	l.Close()
}

func TestListenerFail(t *testing.T) {
	b := newBucket()
	l, err := startMCServer("1.1.1.1:22", b)
	if err == nil {
		t.Fatalf("Error failing to listen: %v", l.Addr())
	} else {
		t.Logf("Listen failed expectedly:  %v", err)
	}
}

func TestVBString(t *testing.T) {
	tests := map[vbState]string{
		vbState(0):          "", // panics
		vbActive:            "active",
		vbReplica:           "replica",
		vbPending:           "pending",
		vbDead:              "dead",
		vbState(vbDead + 1): "", // panics
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
