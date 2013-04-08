package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/dustin/gomemcached"
)

// Exercise the mutation logger code. Output is not examined.
func TestMutationLogger(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	b, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	b.CreateVBucket(0)

	ch := make(chan interface{}, 10)
	ch <- vbucketChange{bucket: b, vbid: 0, oldState: VBDead, newState: VBActive}
	ch <- mutation{deleted: false, key: []byte("a"), cas: 0}
	ch <- mutation{deleted: true, key: []byte("a"), cas: 0}
	ch <- mutation{deleted: false, key: []byte("a"), cas: 2}
	ch <- vbucketChange{oldState: VBDead, newState: VBActive} // invalid bucket
	ch <- vbucketChange{bucket: b, vbid: 0, oldState: VBActive, newState: VBDead}
	close(ch)

	MutationLogger(ch)

	// Should've eaten all the things
	if len(ch) != 0 {
		t.Fatalf("Failed to consume all the messages")
	}
}

func TestMutationInvalid(t *testing.T) {
	defer func() {
		if x := recover(); x == nil {
			t.Fatalf("Expected panic, didn't get it")
		}
	}()

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	b, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	b.CreateVBucket(0)

	ch := make(chan interface{}, 5)
	// Notification of a non-existence bucket is a null lookup.
	ch <- vbucketChange{bucket: b, vbid: 0, oldState: VBDead, newState: VBActive}
	// But this is crazy stupid and will crash the logger.
	ch <- 19

	MutationLogger(ch)

	// Should've eaten all the things
	if len(ch) != 0 {
		t.Fatalf("Failed to consume all the messages")
	}
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

	sessionLoop(rwCloser{bytes.NewBuffer(req.Bytes())}, "test", rh)
}

func TestListener(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	b, err := NewBuckets(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer b.CloseAll()
	if err != nil {
		t.Fatalf("Error with NewBuckets: %v", err)
	}
	l, err := StartServer("0.0.0.0:0", b, DEFAULT_BUCKET_NAME)
	if err != nil {
		t.Fatalf("Error starting listener: %v", err)
	}

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
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	b, err := NewBuckets(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer b.CloseAll()
	if err != nil {
		t.Fatalf("Error with NewBuckets: %v", err)
	}
	l, err := StartServer("1.1.1.1:22", b, DEFAULT_BUCKET_NAME)
	if err == nil {
		t.Fatalf("Error failing to listen: %v", l.Addr())
	}
}

func TestBytesEncoder(t *testing.T) {
	tests := map[string]string{
		"simple": `"simple"`,
		"O'Hair": `"O%27Hair"`,
	}

	for in, out := range tests {
		b := Bytes(in)
		got, err := json.Marshal(&b)
		if err != nil {
			t.Errorf("Error marshaling %v", in)
		}
		if string(got) != out {
			t.Errorf("Expected %s, got %s", out, got)
		}
	}
}

func TestBytesDecoder(t *testing.T) {
	pos := map[string]string{
		`"simple"`:   "simple",
		`"O%27Hair"`: "O'Hair",
	}

	for in, out := range pos {
		b := Bytes{}
		err := json.Unmarshal([]byte(in), &b)
		if err != nil {
			t.Errorf("Error unmarshaling %v", in)
		}
		if out != b.String() {
			t.Errorf("Expected %v for %v, got %v", out, in, b)
		}
	}

	neg := []string{"xxx no quotes", `"invalid esc %2x"`}

	for _, in := range neg {
		b := Bytes{}
		err := json.Unmarshal([]byte(in), &b)
		if err == nil {
			t.Errorf("Expected error unmarshaling %v, got %v", in, b)
		}
	}

	// This is odd looking, but I use the internal decoder
	// directly since the interior error is just about impossible
	// to encounter otherwise.
	for _, in := range neg {
		b := Bytes{}
		err := b.UnmarshalJSON([]byte(in))
		if err == nil {
			t.Errorf("Expected error unmarshaling %v, got %v", in, b)
		}
	}
}

func TestDirIsDir(t *testing.T) {
	if !isDir(".") {
		t.Errorf("expected . to be a dir")
	}
}
