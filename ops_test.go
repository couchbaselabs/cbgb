package cbgb

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
)

// Don't do any normal logging while running tests.
func init() {
	log.SetOutput(ioutil.Discard)
}

type slowWriter struct {
	numWritten  int
	slowWriteAt int // The nth write will be slow.
}

func (s *slowWriter) Write([]byte) (int, error) {
	if s.numWritten == s.slowWriteAt {
		time.Sleep(50 * time.Millisecond)
	}
	s.numWritten++
	return 0, nil
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
		{255, active, "", "",
			gomemcached.UNKNOWN_COMMAND, nil},
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

	expStats := Stats{
		Items:              1,
		Ops:                int64(len(tests)) - 1, // Don't count the NOT_MY_VBUCKET.
		Gets:               6,
		GetMisses:          3,
		Mutations:          2,
		Sets:               2,
		Deletes:            3,
		Creates:            2,
		Unknowns:           1,
		IncomingValueBytes: 6,
		OutgoingValueBytes: 9,
		ItemBytes:          160,
	}

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	vb, _ := testBucket.CreateVBucket(3)
	testBucket.SetVBState(3, VBActive)

	for _, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: x.vb,
			Key:     []byte(x.key),
			Body:    []byte(x.val),
		}

		res := rh.HandleMessage(ioutil.Discard, nil, req)

		if res == nil && x.expStatus == ignored {
			// this was a "normal" quiet command
			continue
		}

		if res.Status != x.expStatus {
			t.Errorf("Expected %v for %v:%v/%v, got %v",
				x.expStatus, x.op, x.vb, x.key, res.Status)
		}

		if x.expValue != nil && !bytes.Equal(x.expValue, res.Body) {
			t.Errorf("Expected body of %v:%v/%v to be\n%#v\ngot\n%#v",
				x.op, x.vb, x.key, x.expValue, res.Body)
		}
	}

	time.Sleep(10 * time.Millisecond) // Let async stats catch up.

	if !reflect.DeepEqual(&expStats, &vb.stats) {
		t.Errorf("Expected stats of %#v, got %#v", expStats, vb.stats)
	}

	expStatItems := make(chan statItem)
	actStatItems := make(chan statItem)

	go func() {
		expStats.Send(expStatItems)
		close(expStatItems)
	}()
	go func() {
		AggregateStats(testBucket, "").Send(actStatItems)
		close(actStatItems)
	}()

	for expitem := range expStatItems {
		actitem := <-actStatItems
		if expitem.key != actitem.key {
			t.Errorf("agg stats expected key %v, got %v",
				expitem.key, actitem.key)
		}
		if expitem.val != actitem.val {
			t.Errorf("agg stats expected val %v, got %v for key %v",
				expitem.val, actitem.val, expitem.key)
		}
	}
}

func TestMutationBroadcast(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	vb, _ := testBucket.CreateVBucket(3)

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

	for idx, x := range tests {
		x.prep()
		res = rh.HandleMessage(nil, nil, req)
		if res.Status != x.exp {
			t.Errorf("%v - %v: expected %v, got %v", idx, x.name, x.exp, res)
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
	return rh.HandleMessage(nil, nil, req)
}

func TestCASDelete(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	testBucket.CreateVBucket(3)

	testKey := "x"

	setreq := &gomemcached.MCRequest{
		Opcode:  gomemcached.SET,
		VBucket: 3,
		Key:     []byte(testKey),
	}

	setres := rh.HandleMessage(nil, nil, setreq)
	if setres.Status != gomemcached.SUCCESS {
		t.Fatalf("Error setting initial value: %v", setres)
	}

	delreq := &gomemcached.MCRequest{
		Opcode:  gomemcached.DELETE,
		VBucket: 3,
		Key:     []byte(testKey),
		Cas:     82859249,
	}

	res := rh.HandleMessage(nil, nil, delreq)
	if res.Status != gomemcached.EINVAL {
		t.Fatalf("Expected einval, got %v", res)
	}

	delreq.Cas = setres.Cas

	res = rh.HandleMessage(nil, nil, delreq)
	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Failed to delete with cas: %v", res)
	}
}

func TestCASSet(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	testBucket.CreateVBucket(3)
	testBucket.SetVBState(3, VBActive)

	testKey := "x"

	setreq := &gomemcached.MCRequest{
		Opcode:  gomemcached.SET,
		VBucket: 3,
		Key:     []byte(testKey),
		Cas:     883494,
	}
	res := rh.HandleMessage(nil, nil, setreq)
	if res.Status != gomemcached.EINVAL {
		t.Fatalf("Expected einval, got %v", res)
	}

	setreq.Cas = 0
	res = rh.HandleMessage(nil, nil, setreq)
	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Error setting initial value: %v", res)
	}

	objcas := res.Cas

	setreq.Cas = 1 + objcas
	res = rh.HandleMessage(nil, nil, setreq)
	if res.Status != gomemcached.EINVAL {
		t.Fatalf("Expected einval, got %v when objcas %v != setcas %v",
			res, objcas, setreq.Cas)
	}

	setreq.Cas = objcas
	res = rh.HandleMessage(nil, nil, setreq)
	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Error setting updated value: %v", res)
	}
}

func TestVersionCommand(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.VERSION,
	}

	res := rh.HandleMessage(nil, nil, req)

	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Expected SUCCESS getting version, got %v", res)
	}

	if string(res.Body) != VERSION {
		t.Fatalf("Expected version %s, got %s", VERSION, res.Body)
	}
}

func TestVersionNOOP(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.NOOP,
	}

	res := rh.HandleMessage(nil, nil, req)

	if res.Status != gomemcached.SUCCESS {
		t.Fatalf("Expected SUCCESS on NOOP, got %v", res)
	}
}

func TestQuit(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.QUIT,
	}

	res := rh.HandleMessage(nil, nil, req)

	if !res.Fatal {
		t.Fatalf("Expected quit to hangup, got %v", res)
	}
}

func TestStats(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.STAT,
	}

	// TODO:  maybe grab the results and look at them.
	res := rh.HandleMessage(ioutil.Discard, nil, req)

	if res != nil {
		t.Fatalf("Expected nil from stats, got %v", res)
	}

	res = rh.HandleMessage(&errWriter{io.EOF}, nil, req)
	if !res.Fatal {
		t.Fatalf("Expected Fatal from a bad stats, got %v", res)
	}
}

func TestInvalidCommand(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	testBucket.CreateVBucket(0)
	rh := reqHandler{currentBucket: testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.CommandCode(255),
	}

	res := rh.HandleMessage(nil, nil, req)

	if res.Status != gomemcached.UNKNOWN_COMMAND {
		t.Fatalf("Expected unknown command, got %v", res)
	}
}

func BenchmarkInvalidCommand(b *testing.B) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	testBucket.CreateVBucket(0)
	rh := reqHandler{currentBucket: testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.CommandCode(255),
	}

	// Ignore time from above.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rh.HandleMessage(nil, nil, req)
	}
}

// This test doesn't assert much, but relies on the race detector to
// determine whether anything bad is happening.
func TestParallelMutations(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	testBucket.CreateVBucket(3)

	keys := []string{"a", "b", "c"}

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			rh := reqHandler{currentBucket: testBucket}

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
				rh.HandleMessage(nil, nil, req)
			}
		}(i, keys[i%len(keys)])
	}
	wg.Wait()
}

// Parallel dispatcher invocation timing.
func BenchmarkParallelGet(b *testing.B) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.GET,
		Key:     []byte("k"),
		VBucket: 3,
	}
	benchmarkParallelCmd(b, req)
}

func BenchmarkParallelSet(b *testing.B) {
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.SET,
		Key:     []byte("k"),
		VBucket: 3,
		Body:    []byte("hello"),
	}
	benchmarkParallelCmd(b, req)
}

func benchmarkParallelCmd(b *testing.B, req *gomemcached.MCRequest) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	testBucket.CreateVBucket(3)

	rh := reqHandler{currentBucket: testBucket}

	wg := sync.WaitGroup{}
	var parallel = 32
	wg.Add(parallel)

	// Ignore time from above.
	b.ResetTimer()
	for worker := 0; worker < parallel; worker++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N/parallel; i++ {
				rh.HandleMessage(nil, nil, req)
			}
		}()
	}
	wg.Wait()
}

// Best case dispatcher timing.
func BenchmarkDispatch(b *testing.B) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	testBucket.CreateVBucket(3)
	rh := reqHandler{currentBucket: testBucket}

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.GET,
		Key:    []byte("k"),
	}

	// Ignore time from above.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rh.HandleMessage(nil, nil, req)
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
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	testBucket.CreateVBucket(0)

	for i := 0; i < numItems; i++ {
		req := &gomemcached.MCRequest{
			Opcode: gomemcached.SET,
			Key:    []byte(strconv.Itoa(i)),
			Body:   []byte(strconv.Itoa(i)),
		}

		res := rh.HandleMessage(nil, nil, req)
		if res.Cas != uint64(i+1) {
			t.Errorf("Expected SET cas of %v, got %v",
				i+1, res.Cas)
		}
	}

	for i := 0; i < numItems; i++ {
		req := &gomemcached.MCRequest{
			Opcode: gomemcached.GET,
			Key:    []byte(strconv.Itoa(i)),
		}

		res := rh.HandleMessage(nil, nil, req)
		if res.Status != gomemcached.SUCCESS {
			t.Errorf("Expected GET success, got %v",
				res.Status)
		}
		if res.Cas != uint64(i+1) {
			t.Errorf("Expected GET cas of %v, got %v",
				i+1, res.Cas)
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

	res := rh.HandleMessage(w, nil, req)
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
	changesExpected := numItems - int(changesSinceCAS)
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
		if !bytes.Equal(res.Key, []byte(strconv.Itoa(i+int(changesSinceCAS)))) {
			t.Errorf("Expected changes key %v, got %v",
				[]byte(strconv.Itoa(i+int(changesSinceCAS))), res.Key)
		}
	}
}

func TestChangesSinceTransmitError(t *testing.T) {
	w := errWriter{io.EOF}
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	testBucket.CreateVBucket(0)
	for _, k := range []string{"a", "b", "c"} {
		rh.HandleMessage(nil, nil, &gomemcached.MCRequest{
			Opcode: gomemcached.SET,
			Key:    []byte(k),
			Body:   []byte(k),
		})

	}
	res := rh.HandleMessage(w, nil, &gomemcached.MCRequest{
		Opcode: CHANGES_SINCE,
		Cas:    0,
	})

	if !res.Fatal {
		t.Errorf("Expected fatal response due to transmit error, %v", res)
	}
}

func TestVBMeta(t *testing.T) {
	allow_vbSetVBMeta = true

	empty := []byte{}

	tests := []struct {
		op  gomemcached.CommandCode
		val string

		expStatus gomemcached.Status
		expValue  []byte
		expVBMeta *VBMeta
	}{
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":0,"metaCas":0,"state":"dead","keyRange":null}`),
			nil},
		{SET_VBMETA, "",
			gomemcached.EINVAL, empty,
			nil},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":0,"metaCas":0,"state":"dead","keyRange":null}`),
			nil},
		{SET_VBMETA, "not-json",
			gomemcached.EINVAL, empty,
			nil},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":0,"metaCas":0,"state":"dead","keyRange":null}`),
			nil},
		{SET_VBMETA, "{}",
			gomemcached.SUCCESS, empty,
			&VBMeta{Id: 3, LastCas: 1, MetaCas: 1, State: "dead"}},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":1,"metaCas":1,"state":"dead","keyRange":null}`),
			&VBMeta{Id: 3, LastCas: 1, MetaCas: 1, State: "dead"}},
		{SET_VBMETA, `{"hi":"world"}`,
			gomemcached.SUCCESS, empty,
			&VBMeta{Id: 3, LastCas: 2, MetaCas: 2, State: "dead"}},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":2,"metaCas":2,"state":"dead","keyRange":null}`),
			&VBMeta{Id: 3, LastCas: 2, MetaCas: 2, State: "dead"}},
		{SET_VBMETA, `{"keyRange":{"minKeyInclusive":""}}`,
			gomemcached.SUCCESS, empty,
			&VBMeta{Id: 3,
				LastCas:  3,
				MetaCas:  3,
				State:    "dead",
				KeyRange: &VBKeyRange{MinKeyInclusive: []byte{}}}},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":3,"metaCas":3,"state":"dead","keyRange":{"minKeyInclusive":"","maxKeyExclusive":""}}`),
			&VBMeta{Id: 3,
				LastCas:  3,
				MetaCas:  3,
				State:    "dead",
				KeyRange: &VBKeyRange{MinKeyInclusive: []byte{}}}},
		{SET_VBMETA, `{"keyRange":{"minKeyInclusive":"aaa"}}`,
			gomemcached.SUCCESS, empty,
			&VBMeta{Id: 3,
				LastCas:  4,
				MetaCas:  4,
				State:    "dead",
				KeyRange: &VBKeyRange{MinKeyInclusive: []byte("aaa")}}},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":4,"metaCas":4,"state":"dead","keyRange":{"minKeyInclusive":"aaa","maxKeyExclusive":""}}`),
			&VBMeta{Id: 3,
				LastCas:  4,
				MetaCas:  4,
				State:    "dead",
				KeyRange: &VBKeyRange{MinKeyInclusive: []byte("aaa")}}},
		{SET_VBMETA,
			`{"keyRange":{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb","x":"X"}}`,
			gomemcached.SUCCESS, empty,
			&VBMeta{Id: 3,
				LastCas: 5,
				MetaCas: 5,
				State:   "dead",
				KeyRange: &VBKeyRange{
					MinKeyInclusive: []byte("aaa"),
					MaxKeyExclusive: []byte("bbb"),
				}}},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":5,"metaCas":5,"state":"dead","keyRange":{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb"}}`),
			&VBMeta{Id: 3,
				LastCas: 5,
				MetaCas: 5,
				State:   "dead",
				KeyRange: &VBKeyRange{
					MinKeyInclusive: []byte("aaa"),
					MaxKeyExclusive: []byte("bbb"),
				}}},
		{SET_VBMETA,
			`{"lastCAS":100,"keyRange":{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb","x":"X"}}`,
			gomemcached.SUCCESS, empty,
			&VBMeta{Id: 3,
				LastCas: 100,
				MetaCas: 6,
				State:   "dead",
				KeyRange: &VBKeyRange{
					MinKeyInclusive: []byte("aaa"),
					MaxKeyExclusive: []byte("bbb"),
				}}},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":100,"metaCas":6,"state":"dead","keyRange":{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb"}}`),
			&VBMeta{Id: 3,
				LastCas: 100,
				MetaCas: 6,
				State:   "dead",
				KeyRange: &VBKeyRange{
					MinKeyInclusive: []byte("aaa"),
					MaxKeyExclusive: []byte("bbb"),
				}}},
		{SET_VBMETA,
			`{"lastCAS":99,"keyRange":{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb","x":"X"}}`,
			gomemcached.SUCCESS, empty,
			&VBMeta{Id: 3,
				LastCas: 101,
				MetaCas: 101,
				State:   "dead",
				KeyRange: &VBKeyRange{
					MinKeyInclusive: []byte("aaa"),
					MaxKeyExclusive: []byte("bbb"),
				}}},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":101,"metaCas":101,"state":"dead","keyRange":{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb"}}`),
			&VBMeta{Id: 3,
				State:   "dead",
				LastCas: 101,
				MetaCas: 101,
				KeyRange: &VBKeyRange{
					MinKeyInclusive: []byte("aaa"),
					MaxKeyExclusive: []byte("bbb"),
				}}},
		{SET_VBMETA,
			`{"lastCAS":99,"metaCAS":42,"keyRange":{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb","x":"X"}}`,
			gomemcached.SUCCESS, empty,
			&VBMeta{Id: 3,
				LastCas: 102,
				MetaCas: 102,
				State:   "dead",
				KeyRange: &VBKeyRange{
					MinKeyInclusive: []byte("aaa"),
					MaxKeyExclusive: []byte("bbb"),
				}}},
		{GET_VBMETA, "",
			gomemcached.SUCCESS,
			[]byte(`{"id":3,"lastCas":102,"metaCas":102,"state":"dead","keyRange":{"minKeyInclusive":"aaa","maxKeyExclusive":"bbb"}}`),
			&VBMeta{Id: 3,
				State:   "dead",
				LastCas: 102,
				MetaCas: 102,
				KeyRange: &VBKeyRange{
					MinKeyInclusive: []byte("aaa"),
					MaxKeyExclusive: []byte("bbb"),
				}}},
	}

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	vb, _ := testBucket.CreateVBucket(3)

	for i, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: 3,
			Body:    []byte(x.val),
		}

		res := rh.HandleMessage(nil, nil, req)

		if res.Status != x.expStatus {
			t.Errorf("Test %v, expected status %v for %v, got %v",
				i, x.expStatus, x.op, res.Status)
		}

		if !bytes.Equal(x.expValue, res.Body) {
			t.Errorf("Test %v, expected body for %v to be\n%v\ngot\n%v",
				i, x.op, string(x.expValue), string(res.Body))
		}

		if x.expVBMeta != nil {
			if !x.expVBMeta.Equal(vb.Meta()) {
				t.Errorf("Test %v, expected vbmeta for %v to be\n%#v\ngot\n%#v",
					i, x.op, x.expVBMeta, vb.Meta())
			}
		}
	}
}

func TestMinMaxRange(t *testing.T) {
	empty := []byte{}

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	vb, _ := testBucket.CreateVBucket(3)

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
		{SET_VBMETA, "", `{"keyRange":{"minKeyInclusive":"b"}}`,
			gomemcached.SUCCESS, empty},
		{gomemcached.SET, "aaa", "AAA",
			NOT_MY_RANGE, empty},
		{gomemcached.DELETE, "aaa", "AAA",
			NOT_MY_RANGE, empty},
		{gomemcached.GET, "aaa", "",
			NOT_MY_RANGE, empty},
		{gomemcached.SET, "bbb", "BBB",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, "bbb", "",
			gomemcached.SUCCESS, []byte("BBB")},
		{gomemcached.GET, "ccc", "",
			gomemcached.SUCCESS, []byte("CCC")},
		{SET_VBMETA, "", `{"keyRange":{"minKeyInclusive":"b","maxKeyExclusive":"c"}}`,
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

		res := rh.HandleMessage(nil, nil, req)

		if res.Status != x.expStatus {
			t.Errorf("Expected %v for %v:%v, got %v",
				x.expStatus, x.op, x.key, res.Status)
		}

		if !bytes.Equal(x.expValue, res.Body) {
			t.Errorf("Expected body of %v:%v to be\n%#v\ngot\n%#v",
				x.op, x.key, x.expValue, res.Body)
		}
	}

	if vb.stats.NotMyRangeErrors != 7 {
		t.Errorf("Expected stats NotMyRangeErrors %v, got %v",
			uint64(7), vb.stats.NotMyRangeErrors)
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
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	vb, _ := testBucket.CreateVBucket(0)

	for i := 0; i < numItems; i++ {
		req := &gomemcached.MCRequest{
			Opcode: gomemcached.SET,
			Key:    []byte(strconv.Itoa(i)),
			Body:   []byte(strconv.Itoa(i)),
		}

		rh.HandleMessage(nil, nil, req)
	}

	startKeyBytes := []byte(strconv.Itoa(startKey))

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.RGET,
		Key:    startKeyBytes,
	}

	w := &bytes.Buffer{}

	res := rh.HandleMessage(w, nil, req)
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

	if vb.stats.RGets != 1 {
		t.Errorf("Expected stats RGets %v, got %v",
			1, vb.stats.RGets)
	}

	if vb.stats.RGetResults != int64(len(results)) {
		t.Errorf("Expected stats results %v, got %v",
			len(results), vb.stats.RGetResults)
	}

	if vb.stats.IncomingValueBytes != int64(numItems) {
		t.Errorf("Expected stats results incoming bytes %v, got %v",
			uint64(numItems), vb.stats.IncomingValueBytes)
	}

	if vb.stats.OutgoingValueBytes != int64(len(results)) {
		t.Errorf("Expected stats results outgoing bytes %v, got %v",
			len(results), vb.stats.OutgoingValueBytes)
	}
}

func TestSlowClient(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	vb, _ := testBucket.CreateVBucket(0)
	testBucket.SetVBState(0, VBActive)

	req := &gomemcached.MCRequest{
		Opcode: gomemcached.SET,
		Key:    []byte("hi"),
		Body:   []byte("world"),
	}
	res := rh.HandleMessage(nil, nil, req)

	sw := &slowWriter{slowWriteAt: 0}
	done := make(chan string)

	go func() { // A concurrent slow client.
		req := &gomemcached.MCRequest{
			Opcode: gomemcached.RGET,
			Key:    []byte("a"),
		}
		res := rh.HandleMessage(sw, nil, req)
		if res.Status != gomemcached.SUCCESS {
			t.Errorf("Expected success but was unsuccessful")
		}
		done <- "slow-client"
	}()

	go func() { // A concurrent fast client.
		req = &gomemcached.MCRequest{
			Opcode: gomemcached.GET,
			Key:    []byte("hi"),
		}
		res = rh.HandleMessage(nil, nil, req)
		if res.Status != gomemcached.SUCCESS {
			t.Errorf("Expected success but was unsuccessful")
		}
		done <- "fast-client"
	}()

	var s string
	s = <-done
	if s != "fast-client" {
		t.Errorf("Expected fast-client to be faster than slow-client")
	}
	s = <-done
	if s != "slow-client" {
		t.Errorf("Expected slow-client to be slower than fast-client")
	}
	if sw.numWritten != 1 {
		t.Errorf("Expected slow-client to actually have written one thing")
	}
	if vb.stats.RGetResults != 1 {
		t.Errorf("Expected 1 RGetResults, got: %v",
			vb.stats.RGetResults)
	}
}

func TestStoreFrontBack(t *testing.T) {
	empty := []byte{}
	active := uint16(3)

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
		{gomemcached.SET, active, "b", "bye",
			gomemcached.SUCCESS, empty},
		{gomemcached.SET, active, "c", "cye",
			gomemcached.SUCCESS, empty},
		{gomemcached.DELETE, active, "c", "",
			gomemcached.SUCCESS, empty},
		{gomemcached.SET, active, "a", "aaa",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("aaa")},
		{gomemcached.GET, active, "b", "",
			gomemcached.SUCCESS, []byte("bye")},
		{gomemcached.GET, active, "c", "",
			gomemcached.KEY_ENOENT, empty},
	}

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	testBucket.CreateVBucket(3)
	testBucket.SetVBState(3, VBActive)

	evictMem := func() {
		// TODO: Clear item.data so we have to fetch from disk.
	}

	runTestsFrom := func(start int) {
		for i := start; i < len(tests); i++ {
			x := tests[i]
			req := &gomemcached.MCRequest{
				Opcode:  x.op,
				VBucket: x.vb,
				Key:     []byte(x.key),
				Body:    []byte(x.val),
			}
			res := rh.HandleMessage(ioutil.Discard, nil, req)
			if res.Status != x.expStatus {
				t.Errorf("Expected %v for %v:%v/%v, got %v",
					x.expStatus, x.op, x.vb, x.key, res.Status)
			}
			if x.expValue != nil && !bytes.Equal(x.expValue, res.Body) {
				t.Errorf("Expected body of %v:%v/%v to be\n%#v\ngot\n%#v",
					x.op, x.vb, x.key, x.expValue, res.Body)
			}
		}
	}

	runTestsFrom(0)
	evictMem()
	runTestsFrom(5)

	// Test RGET's background fetching.
	runTestsFrom(0)
	evictMem()
	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.RGET,
		Key:     []byte("a"),
		VBucket: active,
	}
	w := &bytes.Buffer{}
	res := rh.HandleMessage(w, nil, req)
	if res.Status != gomemcached.SUCCESS {
		t.Errorf("Expected RGET success, got: %v", res)
	}
	results := decodeResponses(t, w.Bytes())
	if len(results) != 2 {
		t.Errorf("Expected to see %v results, got: %v",
			2, len(results))
	}
	rgetExpecteds := []struct {
		key string
		val string
	}{
		{"a", "aaa"},
		{"b", "bye"},
	}
	for i, rgetExpected := range rgetExpecteds {
		if !bytes.Equal(results[i].Key, []byte(rgetExpected.key)) {
			t.Errorf("Expected rget result key: %v, got: %v",
				rgetExpected.key, string(results[i].Key))
		}
		if !bytes.Equal(results[i].Body, []byte(rgetExpected.val)) {
			t.Errorf("Expected rget result val: %v, got: %v",
				rgetExpected.val, string(results[i].Body))
		}
	}
}

func TestMutationOps(t *testing.T) {
	empty := []byte{}
	notempty := []byte("sentinel-for-not-empty")
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
		{gomemcached.REPLACE, active, "a", "irreplacable",
			gomemcached.KEY_ENOENT, notempty},
		{gomemcached.GET, active, "a", "",
			gomemcached.KEY_ENOENT, empty},
		{gomemcached.ADD, active, "a", "should-be-added",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("should-be-added")},
		{gomemcached.APPEND, active, "a", "_suffix",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("should-be-added_suffix")},
		{gomemcached.PREPEND, active, "a", "prefix_",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("prefix_should-be-added_suffix")},
		{gomemcached.REPLACE, active, "a", "replacement",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("replacement")},
		{gomemcached.ADD, active, "a", "not-added",
			gomemcached.KEY_EEXISTS, notempty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("replacement")},

		// quiet
		{gomemcached.ADDQ, active, "a", "not-added",
			gomemcached.KEY_EEXISTS, notempty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("replacement")},
		{gomemcached.REPLACEQ, active, "a", "replacement2",
			ignored, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("replacement2")},
		{gomemcached.APPENDQ, active, "a", "_suffix2",
			ignored, empty},
		{gomemcached.PREPENDQ, active, "a", "prefix2_",
			ignored, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("prefix2_replacement2_suffix2")},
		{gomemcached.DELETE, active, "a", "",
			gomemcached.SUCCESS, empty},
		{gomemcached.REPLACEQ, active, "a", "replacement2",
			gomemcached.KEY_ENOENT, notempty},
	}

	expStats := Stats{
		Items:              0,
		Ops:                int64(len(tests)),
		Gets:               9,
		GetMisses:          1,
		Mutations:          11,
		Sets:               0,
		Adds:               3,
		Replaces:           4,
		Appends:            2,
		Prepends:           2,
		Deletes:            1,
		Creates:            1,
		Updates:            6,
		Unknowns:           0,
		IncomingValueBytes: 68,
		OutgoingValueBytes: 139,
		ItemBytes:          126,
	}

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	vb, _ := testBucket.CreateVBucket(3)
	testBucket.SetVBState(3, VBActive)

	for _, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: x.vb,
			Key:     []byte(x.key),
			Body:    []byte(x.val),
		}

		res := rh.HandleMessage(ioutil.Discard, nil, req)

		if res == nil && x.expStatus == ignored {
			// this was a "normal" quiet command
			continue
		}

		if res.Status != x.expStatus {
			t.Errorf("Expected %v for %v:%v/%v, got %v",
				x.expStatus, x.op, x.vb, x.key, res.Status)
		}

		if x.expValue != nil {
			if bytes.Equal(x.expValue, notempty) {
				if len(res.Body) <= 0 {
					t.Errorf("Expected non-empty body of %v:%v/%v, got: %#v",
						x.op, x.vb, x.key, res)
				}
			} else if !bytes.Equal(x.expValue, res.Body) {
				t.Errorf("Expected body of %v:%v/%v to be\n%#v\ngot\n%#v",
					x.op, x.vb, x.key, x.expValue, res.Body)
			}
		}
	}

	time.Sleep(10 * time.Millisecond) // Let async stats catch up.

	if !reflect.DeepEqual(&expStats, &vb.stats) {
		t.Errorf("Expected stats of %#v, got %#v", expStats, vb.stats)
	}

	expStatItems := make(chan statItem)
	actStatItems := make(chan statItem)

	go func() {
		expStats.Send(expStatItems)
		close(expStatItems)
	}()
	go func() {
		AggregateStats(testBucket, "").Send(actStatItems)
		close(actStatItems)
	}()

	for expitem := range expStatItems {
		actitem := <-actStatItems
		if expitem.key != actitem.key {
			t.Errorf("agg stats expected key %v, got %v",
				expitem.key, actitem.key)
		}
		if expitem.val != actitem.val {
			t.Errorf("agg stats expected val %v, got %v for key %v",
				expitem.val, actitem.val, expitem.key)
		}
	}
}

func TestArithOps(t *testing.T) {
	empty := []byte{}
	notempty := []byte("sentinel-for-not-empty")
	active := uint16(3)
	ignored := gomemcached.Status(32768)

	tests := []struct {
		op  gomemcached.CommandCode
		vb  uint16
		key string
		amt uint64
		def uint64

		expStatus gomemcached.Status
		expValue  []byte
	}{
		{gomemcached.INCREMENT, active, "a", 111, 222,
			gomemcached.SUCCESS, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 222}},
		{gomemcached.GET, active, "a", 0, 0,
			gomemcached.SUCCESS, []byte("222")},
		{gomemcached.INCREMENT, active, "a", 444, 333,
			gomemcached.SUCCESS, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x9a}},
		{gomemcached.GET, active, "a", 0, 0,
			gomemcached.SUCCESS, []byte("666")},
		{gomemcached.DECREMENT, active, "a", 555, 777,
			gomemcached.SUCCESS, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 111}},
		{gomemcached.GET, active, "a", 0, 0,
			gomemcached.SUCCESS, []byte("111")},

		// quiet
		{gomemcached.INCREMENTQ, active, "a", 10, 888,
			ignored, empty},
		{gomemcached.INCREMENTQ, active, "a", 3, 999,
			ignored, empty},
		{gomemcached.DECREMENTQ, active, "a", 1, 222,
			ignored, empty},
		{gomemcached.GET, active, "a", 0, 0,
			gomemcached.SUCCESS, []byte("123")},
	}

	expStats := Stats{
		Items:              1,
		Ops:                int64(len(tests)),
		Gets:               4,
		GetMisses:          0,
		Mutations:          6,
		Sets:               0,
		Adds:               0,
		Replaces:           0,
		Appends:            0,
		Prepends:           0,
		Incrs:              4,
		Decrs:              2,
		Deletes:            0,
		Creates:            1,
		Updates:            5,
		Unknowns:           0,
		IncomingValueBytes: 0,
		OutgoingValueBytes: 12,
		ItemBytes:          129,
	}

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	vb, _ := testBucket.CreateVBucket(3)
	testBucket.SetVBState(3, VBActive)

	for idx, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: x.vb,
			Key:     []byte(x.key),
			Body:    []byte{},
		}
		if x.op != gomemcached.GET {
			req.Extras = make([]byte, 8+8+4)
			binary.BigEndian.PutUint64(req.Extras[:8], x.amt)
			binary.BigEndian.PutUint64(req.Extras[8:16], x.def)
			binary.BigEndian.PutUint32(req.Extras[16:20], uint32(0))
		}

		res := rh.HandleMessage(ioutil.Discard, nil, req)

		if res == nil && x.expStatus == ignored {
			// this was a "normal" quiet command
			continue
		}

		if res.Status != x.expStatus {
			t.Errorf("Expected %v for %v -  %v:%v/%v, got %v",
				x.expStatus, idx, x.op, x.vb, x.key, res.Status)
		}

		if x.expValue != nil {
			if bytes.Equal(x.expValue, notempty) {
				if len(res.Body) <= 0 {
					t.Errorf("Expected non-empty body of %v - %v:%v/%v, got: %#v",
						idx, x.op, x.vb, x.key, res)
				}
			} else if !bytes.Equal(x.expValue, res.Body) {
				t.Errorf("Expected body of %v - %v:%v/%v to be\n%#v\ngot\n%#v",
					idx, x.op, x.vb, x.key, x.expValue, res.Body)
			}
		}
	}

	time.Sleep(10 * time.Millisecond) // Let async stats catch up.

	if !reflect.DeepEqual(&expStats, &vb.stats) {
		t.Errorf("Expected stats of %#v, got %#v", expStats, vb.stats)
	}

	expStatItems := make(chan statItem)
	actStatItems := make(chan statItem)

	go func() {
		expStats.Send(expStatItems)
		close(expStatItems)
	}()
	go func() {
		AggregateStats(testBucket, "").Send(actStatItems)
		close(actStatItems)
	}()

	for expitem := range expStatItems {
		actitem := <-actStatItems
		if expitem.key != actitem.key {
			t.Errorf("agg stats expected key %v, got %v",
				expitem.key, actitem.key)
		}
		if expitem.val != actitem.val {
			t.Errorf("agg stats expected val %v, got %v for key %v",
				expitem.val, actitem.val, expitem.key)
		}
	}
}
