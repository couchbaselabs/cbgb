package cbgb

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
)

func TestTapSetup(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	testBucket.CreateVBucket(0)
	testBucket.SetVBState(0, VBActive)
	rh := reqHandler{currentBucket: testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.TAP_CONNECT,
	}
	res := rh.HandleMessage(nil, &errWriter{io.EOF}, req)
	if res.Status != gomemcached.EINVAL {
		t.Fatalf("expected EINVAL due to bad TAP_CONNECT request, got: %v", res)
	}

	req = &gomemcached.MCRequest{
		Opcode: gomemcached.TAP_CONNECT,
		Extras: make([]byte, 4),
	}

	// Adjust the tick time for the test since this is really only
	// the condition that will transmit for this test.
	origFreq := tapTickFreq
	tapTickFreq = time.Millisecond
	defer func() {
		tapTickFreq = origFreq
	}()

	res = rh.HandleMessage(nil, &errWriter{io.EOF}, req)
	if !res.Fatal {
		t.Fatalf("Expected fatality after error tap bringup, got: %v", res)
	}
}

func TestTapChanges(t *testing.T) {
	// This test has a couple of loose sync points where it sleeps
	// waiting for messages to go through since there's no way to
	// observe the effect of the observation currently.  There
	// aren't many cases where this would occur, but if this test
	// starts spuriously failing, that's why, and we'll make it
	// better.

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}

	chpkt := make(chan transmissible, 128)
	cherr := make(chan error, 1)

	treq := &gomemcached.MCRequest{
		Opcode: gomemcached.TAP_CONNECT,
		Extras: make([]byte, 4),
	}

	go doTap(rh.currentBucket, treq, chpkt, cherr)

	vb0, _ := testBucket.CreateVBucket(0)
	testBucket.SetVBState(0, VBActive)

	testKey := []byte("testKey")

	mustTransmit := func(m string, typ gomemcached.CommandCode) {
		select {
		case m := <-chpkt:
			req := m.(*gomemcached.MCRequest)
			if req.Opcode != typ {
				t.Fatalf("On %v, expected op %v, got %v",
					m, typ, req.Opcode)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("No change received at %v.", m)
		}
	}

	mustNotTransmit := func(m string) {
		select {
		case rv := <-chpkt:
			t.Fatalf("Unexpected change at %v: %v", m, rv)
		case <-time.After(50 * time.Millisecond):
		}
	}

	sendReq := func(req *gomemcached.MCRequest) {
		res := rh.HandleMessage(nil, ioutil.Discard, req)
		if res.Status != gomemcached.SUCCESS {
			t.Fatalf("Error doing set#1: %v", res)
		}
	}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.SET,
		Key:    testKey,
		Body:   []byte("hi"),
	}

	// Let tap settle.
	time.Sleep(100 * time.Millisecond)

	// Verify we get a set on an active vbucket.
	sendReq(req)
	mustTransmit("positive set", gomemcached.TAP_MUTATION)

	// Verify we get a delete.
	req.Opcode = gomemcached.DELETE
	sendReq(req)
	mustTransmit("positive delete", gomemcached.TAP_DELETE)

	// Verify a change without a backing item does *not* transmit.
	vb0.observer.Submit(mutation{key: testKey})
	mustNotTransmit("negative set")

	// Verify we *don't* get a set on a pending vbucket.
	testBucket.SetVBState(0, VBPending)
	time.Sleep(100 * time.Millisecond) // Let the state change settle
	req.Opcode = gomemcached.SET
	sendReq(req)
	mustNotTransmit("negative set")

	// Verify a change without a valid vbucket at all doesn't transmit

	// This test is weird because it's a weird thing.  I need to
	// have an active subscription, and then I forge a message
	// across it from another vbucket we should ignore.
	testBucket.SetVBState(0, VBActive)
	time.Sleep(100 * time.Millisecond) // settle
	vb0.observer.Submit(mutation{vb: 1, key: testKey})
	mustNotTransmit("no vbucket")
}
