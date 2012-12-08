package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
)

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
		{gomemcached.SETQ, active, "a", "aye",
			ignored, empty},
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

		if !bytes.Equal(x.expValue, res.Body) {
			t.Errorf("Expected body of %v:%v/%v to be\n%#v\ngot\n%#v",
				x.op, x.vb, x.key, x.expValue, res.Body)
		}
	}
}

func TestMutationBroadcast(t *testing.T) {
	testBucket := &bucket{}
	rh := reqHandler{testBucket}
	vb := testBucket.createVBucket(3)
	defer vb.Close()

	ch := make(chan interface{}, 16)

	vb.observer.Register(ch)

	key := "testkey"

	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.DELETE,
		VBucket: 3,
		Key:     []byte(key),
	}
	res := &gomemcached.MCResponse{}

	tests := []struct {
		name   string
		prep   func()
		exp    gomemcached.Status
		hasMsg bool
	}{
		{"missing delete",
			func() {},
			gomemcached.KEY_ENOENT, false},
		{"missing CAS",
			func() {
				req.Opcode = gomemcached.SET
				req.Cas = 85824
			},
			gomemcached.EINVAL, false},
		{"good set",
			func() { req.Cas = 0 },
			gomemcached.SUCCESS, true},
		{"good CAS",
			func() { req.Cas = res.Cas },
			gomemcached.SUCCESS, true},
		{"good delete",
			func() {
				req.Opcode = gomemcached.DELETE
				req.Cas = 0
			},
			gomemcached.SUCCESS, true},
	}

	for _, x := range tests {
		x.prep()
		res = rh.HandleMessage(nil, req)
		if res.Status != x.exp {
			t.Errorf("%v: expected %v, got %v", x.name, x.exp, res)
		}

		// Verify delete did *not* send a notification
		var msg interface{}
		select {
		case msg = <-ch:
		case <-time.After(time.Millisecond * 10):
			// I'd normally use default here, but the
			// sending branches out asynchronously, so
			// this is expected to cover a typical case.
		}

		switch {
		case x.hasMsg && msg == nil:
			t.Errorf("Expected broadcast for %v, got none",
				x.name)
		case (!x.hasMsg) && msg != nil:
			t.Errorf("Expected no broadcast for %v, got %v",
				x.name, msg)
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

func TestChangesSince(t *testing.T) {
	for _, numItems := range []int{10, 9, 6, 5, 4, 1, 0} {
		for _, changesSince := range []int{0, 5, 10, 11} {
			testChangesSince(t, uint64(changesSince), numItems)
		}
	}
}

func testChangesSince(t *testing.T, changesSinceCAS uint64, numItems int) {
	testBucket := &bucket{}
	rh := reqHandler{testBucket}
	vb := testBucket.createVBucket(0)
	defer vb.Close()

	for i := 0; i < numItems; i++ {
		req := &gomemcached.MCRequest{
			Opcode: gomemcached.SET,
			Key:    []byte(strconv.Itoa(i)),
			Body:   []byte(strconv.Itoa(i)),
		}

		rh.HandleMessage(nil, req)
	}

	for i := 0; i < numItems; i++ {
		req := &gomemcached.MCRequest{
			Opcode: gomemcached.GET,
			Key:    []byte(strconv.Itoa(i)),
		}

		res := rh.HandleMessage(nil, req)
		if res.Status != gomemcached.SUCCESS {
			t.Errorf("Expected GET success, got %v",
				res.Status)
		}

		if !bytes.Equal([]byte(strconv.Itoa(i)), res.Body) {
			t.Errorf("Expected body of %#v\ngot\n%#v",
				[]byte(strconv.Itoa(i)), res.Body)
		}
	}

	req := &gomemcached.MCRequest{
		Opcode: CHANGES_SINCE,
		Cas:    changesSinceCAS,
	}

	w := &bytes.Buffer{}

	res := rh.HandleMessage(w, req)
	if res.Opcode != CHANGES_SINCE {
		t.Errorf("Expected last changes opcode %v, got %v",
			CHANGES_SINCE, res.Opcode)
	}
	if res.Key != nil {
		t.Errorf("Expected last changes key %v, got %v",
			nil, res.Key)
	}
	if res.Cas != changesSinceCAS {
		t.Errorf("Expected last changes cas %v, got %v",
			0, res.Cas)
	}

	changes := decodeResponses(t, w.Bytes())
	changesExpected := numItems - 1 - int(changesSinceCAS)
	if changesExpected < 0 {
		changesExpected = 0
	}
	if len(changes) != changesExpected {
		t.Errorf("Expected to see %v changes, got %v",
			changesExpected, len(changes))
	}

	for i := range changes {
		res := changes[i]
		if res.Opcode != CHANGES_SINCE {
			t.Errorf("Expected opcode %v, got %v",
				CHANGES_SINCE, res.Opcode)
		}
		if res.Cas < changesSinceCAS {
			t.Errorf("Expected changes cas > %v, got %v",
				changesSinceCAS, res.Cas)
		}
		if res.Cas != uint64(i+1+int(changesSinceCAS)) {
			t.Errorf("Expected changes cas %v, got %v",
				i+1+int(changesSinceCAS), res.Cas)
		}
		if !bytes.Equal(res.Key, []byte(strconv.Itoa(i+1+int(changesSinceCAS)))) {
			t.Errorf("Expected changes key %v, got %v",
				[]byte(strconv.Itoa(i+1+int(changesSinceCAS))), res.Key)
		}
	}
}

func TestChangesSinceTransmitError(t *testing.T) {
	w := errWriter{io.EOF}
	v := newVbucket(0)
	for _, k := range []string{"a", "b"} {
		vbSet(v, nil, &gomemcached.MCRequest{
			Opcode: gomemcached.SET,
			Key:    []byte(k),
			Body:   []byte(k),
		})
	}
	res, _ := vbChangesSince(v, w, &gomemcached.MCRequest{
		Opcode: CHANGES_SINCE,
		Cas:    0,
	})
	if !res.Fatal {
		t.Errorf("Expected fatal response due to transmit error, %v", res)
	}
}

func TestVBucketConfig(t *testing.T) {
	empty := []byte{}

	tests := []struct {
		op  gomemcached.CommandCode
		val string

		expStatus   gomemcached.Status
		expValue    []byte
		expVBConfig *VBConfig
	}{
		{GET_VBUCKET_CONFIG, "",
			gomemcached.SUCCESS, []byte("{}"),
			nil},
		{SET_VBUCKET_CONFIG, "",
			gomemcached.EINVAL, empty,
			nil},
		{GET_VBUCKET_CONFIG, "",
			gomemcached.SUCCESS, []byte("{}"),
			nil},
		{SET_VBUCKET_CONFIG, "not-json",
			gomemcached.EINVAL, empty,
			nil},
		{GET_VBUCKET_CONFIG, "",
			gomemcached.SUCCESS, []byte("{}"),
			nil},
		{SET_VBUCKET_CONFIG, "{}",
			gomemcached.SUCCESS, empty,
			&VBConfig{}},
		{GET_VBUCKET_CONFIG, "",
			gomemcached.SUCCESS,
			[]byte(`{"minKeyInclusive":"","maxKeyExclusive":""}`),
			&VBConfig{}},
		{SET_VBUCKET_CONFIG, `{"hi":"world"}`,
			gomemcached.SUCCESS, empty,
			&VBConfig{}},
		{GET_VBUCKET_CONFIG, "",
			gomemcached.SUCCESS,
			[]byte(`{"minKeyInclusive":"","maxKeyExclusive":""}`),
			&VBConfig{}},
		{SET_VBUCKET_CONFIG, `{"minKeyInclusive":""}`,
			gomemcached.SUCCESS, empty,
			&VBConfig{MinKeyInclusive: []byte("")}},
		{GET_VBUCKET_CONFIG, "",
			gomemcached.SUCCESS,
			[]byte(`{"minKeyInclusive":"","maxKeyExclusive":""}`),
			&VBConfig{MinKeyInclusive: []byte("")}},
		{SET_VBUCKET_CONFIG, `{"minKeyInclusive":"aaa"}`,
			gomemcached.SUCCESS, empty,
			&VBConfig{MinKeyInclusive: []byte("aaa")}},
		{GET_VBUCKET_CONFIG, "",
			gomemcached.SUCCESS,
			[]byte(`{"minKeyInclusive":"aaa","maxKeyExclusive":""}`),
			&VBConfig{MinKeyInclusive: []byte("aaa")}},
		{SET_VBUCKET_CONFIG,
			`{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb","x":"X"}`,
			gomemcached.SUCCESS, empty,
			&VBConfig{
				MinKeyInclusive: []byte("aaa"),
				MaxKeyExclusive: []byte("bbb"),
			},
		},
		{GET_VBUCKET_CONFIG, "",
			gomemcached.SUCCESS,
			[]byte(`{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb"}`),
			&VBConfig{
				MinKeyInclusive: []byte("aaa"),
				MaxKeyExclusive: []byte("bbb"),
			},
		},
	}

	testBucket := &bucket{}
	rh := reqHandler{testBucket}
	vb := testBucket.createVBucket(3)
	defer vb.Close()

	for i, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: 3,
			Body:    []byte(x.val),
		}

		res := rh.HandleMessage(nil, req)

		if res.Status != x.expStatus {
			t.Errorf("Test %v, expected status %v for %v, got %v",
				i, x.expStatus, x.op, res.Status)
		}

		if !bytes.Equal(x.expValue, res.Body) {
			t.Errorf("Test %v, expected body for %v to be\n%v\ngot\n%v",
				i, x.op, string(x.expValue), string(res.Body))
		}

		if x.expVBConfig != nil {
			if !x.expVBConfig.Equal(vb.config) {
				t.Errorf("Test %v, expected vbconfig for %v to be\n%#v\ngot\n%#v",
					i, x.op, x.expVBConfig, vb.config)
			}
		} else {
			if vb.config != nil {
				t.Errorf("Test %v, expected vbconfig for %v to be nil\ngot\n%#v",
					i, x.op, vb.config)
			}
		}
	}
}

func TestMinMaxRange(t *testing.T) {
	empty := []byte{}

	testBucket := &bucket{}
	rh := reqHandler{testBucket}
	vb := testBucket.createVBucket(3)
	defer vb.Close()

	tests := []struct {
		op  gomemcached.CommandCode
		key string
		val string

		expStatus gomemcached.Status
		expValue  []byte
	}{
		{gomemcached.SET, "ccc", "CCC",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, "ccc", "",
			gomemcached.SUCCESS, []byte("CCC")},
		{SET_VBUCKET_CONFIG, "", `{"minKeyInclusive":"b"}`,
			gomemcached.SUCCESS, empty},
		{gomemcached.SET, "aaa", "AAA",
			NOT_MY_RANGE, empty},
		{gomemcached.GET, "aaa", "",
			NOT_MY_RANGE, empty},
		{gomemcached.SET, "bbb", "BBB",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, "bbb", "",
			gomemcached.SUCCESS, []byte("BBB")},
		{gomemcached.GET, "ccc", "",
			gomemcached.SUCCESS, []byte("CCC")},
		{SET_VBUCKET_CONFIG, "", `{"minKeyInclusive":"b","maxKeyExclusive":"c"}`,
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, "aaa", "",
			NOT_MY_RANGE, empty},
		{gomemcached.SET, "bbb", "BBB",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, "ccc", "",
			NOT_MY_RANGE, empty},
		{gomemcached.DELETE, "aaa", "",
			NOT_MY_RANGE, empty},
		{gomemcached.DELETE, "bbb", "",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, "bbb", "",
			gomemcached.KEY_ENOENT, empty},
		{gomemcached.DELETE, "ccc", "",
			NOT_MY_RANGE, empty},
	}

	for _, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: 3,
			Key:     []byte(x.key),
			Body:    []byte(x.val),
		}

		res := rh.HandleMessage(nil, req)

		if res.Status != x.expStatus {
			t.Errorf("Expected %v for %v:%v, got %v",
				x.expStatus, x.op, x.key, res.Status)
		}

		if !bytes.Equal(x.expValue, res.Body) {
			t.Errorf("Expected body of %v:%v to be\n%#v\ngot\n%#v",
				x.op, x.key, x.expValue, res.Body)
		}
	}
}

func TestRGet(t *testing.T) {
	for _, numItems := range []int{8, 7, 6, 5, 4, 1, 0} {
		for _, startKey := range []int{0, 5, 8, 9} {
			testRGet(t, startKey, numItems)
		}
	}
}

func testRGet(t *testing.T, startKey int, numItems int) {
	testBucket := &bucket{}
	rh := reqHandler{testBucket}
	vb := testBucket.createVBucket(0)
	defer vb.Close()

	for i := 0; i < numItems; i++ {
		req := &gomemcached.MCRequest{
			Opcode: gomemcached.SET,
			Key:    []byte(strconv.Itoa(i)),
			Body:   []byte(strconv.Itoa(i)),
		}

		rh.HandleMessage(nil, req)
	}

	startKeyBytes := []byte(strconv.Itoa(startKey))

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.RGET,
		Key:    startKeyBytes,
	}

	w := &bytes.Buffer{}

	res := rh.HandleMessage(w, req)
	if res.Opcode != gomemcached.RGET {
		t.Errorf("Expected last rget response opcode %v, got %v",
			gomemcached.RGET, res.Opcode)
	}
	if res.Key != nil {
		t.Errorf("Expected last rget response key %v, got %v",
			nil, res.Key)
	}
	if res.Body != nil {
		t.Errorf("Expected last rget response data %v, got %v",
			nil, res.Body)
	}

	results := decodeResponses(t, w.Bytes())

	resultsExpected := numItems - startKey
	if resultsExpected < 0 {
		resultsExpected = 0
	}
	if len(results) != resultsExpected {
		t.Errorf("Expected to see %v results, got %v, numItems %v, startKey %v",
			resultsExpected, len(results), numItems, startKey)
	}

	for i := range results {
		res := results[i]
		if res.Opcode != gomemcached.RGET {
			t.Errorf("Expected opcode %v, got %v",
				gomemcached.RGET, res.Opcode)
		}
		if bytes.Compare(res.Key, startKeyBytes) < 0 {
			t.Errorf("Expected results key >= %v, got %v",
				startKeyBytes, res.Key)
		}
	}
}

