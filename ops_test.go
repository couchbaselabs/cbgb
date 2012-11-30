package main

import (
	"io"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/dustin/gomemcached"
)

func bodyEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestBasicOps(t *testing.T) {
	empty := []byte{}
	active := uint16(3)
	ignored := gomemcached.Status(32768)

	tests := []struct {
		op  gomemcached.CommandCode
		vb  uint16
		key string
		val string

		expStatus gomemcached.Status
		expValue  []byte
	}{
		{gomemcached.SET, active, "a", "aye",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("aye")},
		{gomemcached.GETK, active, "a", "", // TODO: Assert the key?
			gomemcached.SUCCESS, []byte("aye")},
		{gomemcached.GET, 2, "a", "",
			gomemcached.NOT_MY_VBUCKET, empty},
		{gomemcached.GET, active, "b", "",
			gomemcached.KEY_ENOENT, empty},
		{gomemcached.DELETE, active, "a", "",
			gomemcached.SUCCESS, empty},
		{gomemcached.DELETE, active, "a", "",
			gomemcached.KEY_ENOENT, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.KEY_ENOENT, empty},

		// quiet
		{gomemcached.GETQ, active, "a", "aye",
			ignored, empty},
		{gomemcached.DELETEQ, active, "a", "",
			ignored, empty},
		{gomemcached.SET, active, "a", "aye",
			gomemcached.SUCCESS, empty},
		{gomemcached.GETQ, active, "a", "",
			gomemcached.SUCCESS, []byte("aye")},
	}

	testBucket := &bucket{}
	rh := reqHandler{testBucket}
	vb := testBucket.createVBucket(3)
	defer vb.Close()

	for _, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: x.vb,
			Key:     []byte(x.key),
			Body:    []byte(x.val),
		}

		res := rh.HandleMessage(nil, req)

		if res == nil && x.expStatus == ignored {
			// this was a "normal" quiet command
			continue
		}

		if res.Status != x.expStatus {
			t.Errorf("Expected %v for %v:%v/%v, got %v",
				x.expStatus, x.op, x.vb, x.key, res.Status)
		}

		if !bodyEqual(x.expValue, res.Body) {
			t.Errorf("Expected body of %v:%v/%v to be\n%#v\ngot\n%#v",
				x.op, x.vb, x.key, x.expValue, res.Body)
		}
	}
}

func testGet(rh *reqHandler, vbid uint16, key string) *gomemcached.MCResponse {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.GET,
		VBucket: vbid,
		Key:     []byte(key),
	}
	return rh.HandleMessage(nil, req)
}

func TestCASDelete(t *testing.T) {
	testBucket := &bucket{}
	rh := reqHandler{testBucket}
	vb := testBucket.createVBucket(3)
	defer vb.Close()

	testKey := "x"

	setreq := &gomemcached.MCRequest{
		Opcode:  gomemcached.SET,
		VBucket: 3,
		Key:     []byte(testKey),
	}

	setres := rh.HandleMessage(nil, setreq)
	if setres.Status != gomemcached.SUCCESS {
		t.Fatalf("Error setting initial value: %v", setres)
	}

	delreq := &gomemcached.MCRequest{
		Opcode:  gomemcached.DELETE,
		VBucket: 3,
		Key:     []byte(testKey),
		Cas:     82859249,
	}

	res := rh.HandleMessage(nil, delreq)
	if res.Status != gomemcached.EINVAL {
		t.Fatalf("Expected einval, got %v", res)
	}

	delreq.Cas = setres.Cas

	res = rh.HandleMessage(nil, delreq)
	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Failed to delete with cas: %v", res)
	}

}

func TestCASSet(t *testing.T) {
	testBucket := &bucket{}
	rh := reqHandler{testBucket}
	vb := testBucket.createVBucket(3)
	defer vb.Close()

	testKey := "x"

	setreq := &gomemcached.MCRequest{
		Opcode:  gomemcached.SET,
		VBucket: 3,
		Key:     []byte(testKey),
		Cas:     883494,
	}
	res := rh.HandleMessage(nil, setreq)
	if res.Status != gomemcached.EINVAL {
		t.Fatalf("Expected einval, got %v", res)
	}

	objcas := res.Cas

	setreq.Cas = 0

	res = rh.HandleMessage(nil, setreq)
	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Error setting initial value: %v", res)
	}

	setreq.Cas = 1 + objcas
	res = rh.HandleMessage(nil, setreq)
	if res.Status != gomemcached.EINVAL {
		t.Fatalf("Expected einval, got %v", res)
	}

	setreq.Cas = objcas
	res = rh.HandleMessage(nil, setreq)
	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Error setting updated value: %v", res)
	}
}

func TestVersionCommand(t *testing.T) {
	testBucket := &bucket{}
	rh := reqHandler{testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.VERSION,
	}

	res := rh.HandleMessage(nil, req)

	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Expected SUCCESS getting version, got %v", res)
	}

	if string(res.Body) != VERSION {
		t.Fatalf("Expected version %s, got %s", VERSION, res.Body)
	}
}

func TestVersionNOOP(t *testing.T) {
	testBucket := &bucket{}
	rh := reqHandler{testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.NOOP,
	}

	res := rh.HandleMessage(nil, req)

	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Expected SUCCESS on NOOP, got %v", res)
	}
}

func TestQuit(t *testing.T) {
	testBucket := &bucket{}
	rh := reqHandler{testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.QUIT,
	}

	res := rh.HandleMessage(nil, req)

	if !res.Fatal {
		t.Fatalf("Expected quit to hangup, got %v", res)
	}
}

func TestStats(t *testing.T) {
	testBucket := &bucket{}
	rh := reqHandler{testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.STAT,
	}

	// TODO:  maybe grab the results and look at them.
	res := rh.HandleMessage(ioutil.Discard, req)

	if res != nil {
		t.Fatalf("Expected nil from stats, got %v", res)
	}

	res = rh.HandleMessage(&errWriter{io.EOF}, req)
	if !res.Fatal {
		t.Fatalf("Expected Fatal from a bad stats, got %v", res)
	}
}

func TestInvalidCommand(t *testing.T) {
	testBucket := &bucket{}
	vb := testBucket.createVBucket(0)
	defer vb.Close()
	rh := reqHandler{testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.CommandCode(255),
	}

	res := rh.HandleMessage(nil, req)

	if res.Status != gomemcached.UNKNOWN_COMMAND {
		t.Fatalf("Expected unknown command, got %v", res)
	}
}

func BenchmarkInvalidCommand(b *testing.B) {
	testBucket := &bucket{}
	vb := testBucket.createVBucket(0)
	defer vb.Close()
	rh := reqHandler{testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.CommandCode(255),
	}

	for i := 0; i < b.N; i++ {
		rh.HandleMessage(nil, req)
	}
}

// This test doesn't assert much, but relies on the race detector to
// determine whether anything bad is happening.
func TestParallelMutations(t *testing.T) {
	testBucket := &bucket{}
	vb := testBucket.createVBucket(3)
	defer vb.Close()

	keys := []string{"a", "b", "c"}

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			rh := reqHandler{testBucket}

			seq := []gomemcached.CommandCode{
				gomemcached.SET,
				gomemcached.SET,
				gomemcached.SET,
				gomemcached.DELETE,
			}

			for _, op := range seq {
				req := &gomemcached.MCRequest{
					Opcode:  op,
					VBucket: 3,
					Key:     []byte(key),
				}

				rh.HandleMessage(nil, req)
			}
		}(keys[i%len(keys)])
	}
	wg.Wait()
}

// Parallel dispatcher invocation timing.
func BenchmarkParallelGet(b *testing.B) {
	testBucket := &bucket{}
	vb := testBucket.createVBucket(3)
	defer vb.Close()

	rh := reqHandler{testBucket}

	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.GET,
		Key:     []byte("k"),
		VBucket: 3,
	}

	wg := sync.WaitGroup{}
	var parallel = 32
	wg.Add(parallel)

	// Ignore time from above.
	b.ResetTimer()

	for worker := 0; worker < parallel; worker++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N/parallel; i++ {
				rh.HandleMessage(nil, req)
			}
		}()
	}
	wg.Wait()
}

// Best case dispatcher timing.
func BenchmarkDispatch(b *testing.B) {
	testBucket := &bucket{}
	vb := testBucket.createVBucket(3)
	defer vb.Close()

	rh := reqHandler{testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.GET,
		Key:    []byte("k"),
	}

	// Ignore time from above.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rh.HandleMessage(nil, req)
	}
}
