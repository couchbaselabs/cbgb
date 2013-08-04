package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/dustin/gomemcached"
)

func TestViewRows(t *testing.T) {
	var v ViewRows
	v = make([]*ViewRow, 3)
	v[0] = &ViewRow{Key: "2"}
	v[1] = &ViewRow{Key: "1"}
	v[2] = &ViewRow{Key: "3"}
	sort.Sort(v)
	if v[0].Key.(string) != "1" {
		t.Errorf("sort off")
	}
	if v[1].Key.(string) != "2" {
		t.Errorf("sort off")
	}
	if v[2].Key.(string) != "3" {
		t.Errorf("sort off")
	}
}

func viewParamsEqual(a, b *ViewParams) bool {
	aj, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bj, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return bytes.Equal(aj, bj)
}

type testform struct {
	m map[string]string
}

func (f *testform) FormValue(k string) string {
	return f.m[k]
}

func TestParseViewParams(t *testing.T) {
	p, err := ParseViewParams(nil)
	if err != nil {
		t.Errorf("expected nil parse to work")
	}
	if !viewParamsEqual(p, NewViewParams()) {
		t.Errorf("expected nil parse to give a blank viewparams")
	}

	f := &testform{
		m: map[string]string{
			"key":            `"aaa"`,
			"keys":           "1,2,3",
			"startkey":       `"AA"`,
			"startkey_docid": "AADD",
			"end_key":        `"ZZ"`,
			"endkey_docid":   "ZZDD",
			"stale":          "ok",
			"descending":     "true",
			"group":          "true",
			"group_level":    "321",
			"include_docs":   "true",
			"inclusive_end":  "false",
			"limit":          "100",
			"reduce":         "false",
			"skip":           "456",
			"update_seq":     "true",
		},
	}
	p, err = ParseViewParams(f)
	if err != nil {
		t.Errorf("unexpected error parsing view params: %v", err)
	}
	exp := &ViewParams{
		Key:           "aaa",
		Keys:          "1,2,3",
		StartKey:      "AA",
		StartKeyDocId: "AADD",
		EndKey:        "ZZ",
		EndKeyDocId:   "ZZDD",
		Stale:         "ok",
		Descending:    true,
		Group:         true,
		GroupLevel:    321,
		IncludeDocs:   true,
		InclusiveEnd:  false,
		Limit:         100,
		Reduce:        false,
		Skip:          456,
		UpdateSeq:     true,
	}
	if !viewParamsEqual(exp, p) {
		t.Errorf("expected %#v, got %#v", exp, p)
	}
}

func TestMergeViewRows(t *testing.T) {
	expectViewRows := func(msg string, c chan *ViewRow, exp []string) {
		for i, s := range exp {
			v, ok := <-c
			if !ok {
				t.Errorf("expectViewRows expected %v for %v, index %v,"+
					" but chan was closed", s, msg, i)
			}
			if v.Key != s {
				t.Errorf("expectViewRows %v to equal %v, index %v, for %v",
					v, s, i, msg)
			}
		}
		v, ok := <-c
		if ok {
			t.Errorf("expectViewRows expected chan closed for %v, but got %v",
				msg, v)
		}
	}

	feed := func(c chan *ViewRow, arr []string) {
		for _, s := range arr {
			c <- &ViewRow{Key: s}
		}
		close(c)
	}

	out := make(chan *ViewRow, 1)
	go MergeViewRows(nil, out)
	expectViewRows("empty merge", out, []string{})

	out = make(chan *ViewRow, 1)
	in0 := make(chan *ViewRow, 1)
	in := []chan *ViewRow{in0}
	close(in[0])
	go MergeViewRows(in, out)
	expectViewRows("merge closed chan", out, []string{})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in1 := make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0, in1}
	close(in[0])
	close(in[1])
	go MergeViewRows(in, out)
	expectViewRows("merge 2 closed chans", out, []string{})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0}
	go feed(in[0], []string{})
	go MergeViewRows(in, out)
	expectViewRows("empty feed channel", out, []string{})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in1 = make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0, in1}
	go feed(in[0], []string{})
	go feed(in[1], []string{})
	go MergeViewRows(in, out)
	expectViewRows("empty feed channels", out, []string{})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0}
	go feed(in[0], []string{"a", "b", "c"})
	go MergeViewRows(in, out)
	expectViewRows("1 feed channel", out, []string{"a", "b", "c"})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in1 = make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0, in1}
	go feed(in[0], []string{"a", "c", "e"})
	go feed(in[1], []string{"b", "d", "f"})
	go MergeViewRows(in, out)
	expectViewRows("2 feed channel", out,
		[]string{"a", "b", "c", "d", "e", "f"})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in1 = make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0, in1}
	go feed(in[0], []string{"a", "b", "c"})
	go feed(in[1], []string{})
	go MergeViewRows(in, out)
	expectViewRows("1 empty feed channel", out,
		[]string{"a", "b", "c"})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in1 = make(chan *ViewRow, 1)
	in2 := make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0, in1, in2}
	go feed(in[0], []string{"a", "b", "c"})
	go feed(in[1], []string{})
	go feed(in[2], []string{})
	go MergeViewRows(in, out)
	expectViewRows("2 empty feed channel", out,
		[]string{"a", "b", "c"})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in1 = make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0, in1}
	go feed(in[0], []string{})
	go feed(in[1], []string{"a", "b", "c"})
	go MergeViewRows(in, out)
	expectViewRows("other empty feed channel", out,
		[]string{"a", "b", "c"})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in1 = make(chan *ViewRow, 1)
	in2 = make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0, in1, in2}
	go feed(in[0], []string{})
	go feed(in[1], []string{"a", "b", "c"})
	go feed(in[2], []string{})
	go MergeViewRows(in, out)
	expectViewRows("more empty feed channel", out,
		[]string{"a", "b", "c"})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in1 = make(chan *ViewRow, 1)
	in2 = make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0, in1, in2}
	go feed(in[0], []string{"c"})
	go feed(in[1], []string{"b"})
	go feed(in[2], []string{"a"})
	go MergeViewRows(in, out)
	expectViewRows("reversed feed channels", out,
		[]string{"a", "b", "c"})

	out = make(chan *ViewRow, 1)
	in0 = make(chan *ViewRow, 1)
	in1 = make(chan *ViewRow, 1)
	in2 = make(chan *ViewRow, 1)
	in = []chan *ViewRow{in0, in1, in2}
	go feed(in[0], []string{"b", "c"})
	go feed(in[1], []string{"b", "c"})
	go feed(in[2], []string{"a", "a"})
	go MergeViewRows(in, out)
	expectViewRows("repeated feed channel", out,
		[]string{"a", "a", "b", "b", "c", "c"})
}

func TestStaleness(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b0.Close()

	r0 := &reqHandler{currentBucket: b0}
	v0, _ := b0.CreateVBucket(2)
	if v0.staleness != 0 {
		t.Errorf("expected freshness with a new vbucket, got: %v", v0.staleness)
	}

	b0.SetVBState(2, VBActive)
	if v0.staleness != 0 {
		t.Errorf("expected freshness with an active vbucket, got: %v", v0.staleness)
	}

	testLoadInts(t, r0, 2, 5)
	if v0.staleness != 5 {
		t.Errorf("expected 5 staleness after data loading, got: %v", v0.staleness)
	}
}

func TestMkViewsRefreshFun(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b0.Close()

	v0, _ := b0.CreateVBucket(2)

	f := v0.mkViewsRefreshFun()
	x := f(time.Now())
	if x {
		t.Errorf("expected freshness after a refresh")
	}
}

func TestGetViewsStore(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b0.Close()

	v0, _ := b0.CreateVBucket(2)

	vs, err := v0.getViewsStore()
	if err != nil || vs == nil {
		t.Errorf("expected views store but got err/nil: %v, %v", err, vs)
	}
}

func TestNoDDocViewsRefresh(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b0.Close()

	r0 := &reqHandler{currentBucket: b0}

	v0, _ := b0.CreateVBucket(2)
	n, err := v0.viewsRefresh()
	if err != nil || n != 0 {
		t.Errorf("expected ok viewsRefresh when no ddocs, got err/nil: %v, %v", err, n)
	}

	b0.SetVBState(2, VBActive)
	n, err = v0.viewsRefresh()
	if err != nil || n != 0 {
		t.Errorf("expected ok viewsRefresh when no ddocs, got err/nil: %v, %v", err, n)
	}

	testLoadInts(t, r0, 2, 5)
	n, err = v0.viewsRefresh()
	if err != nil || n != 0 {
		t.Errorf("expected ok viewsRefresh when no ddocs, got err/nil: %v, %v", err, n)
	}
}

func TestVIndexKeyParse(t *testing.T) {
	tests := []struct {
		docId   string
		emitKey string
		expErr  bool
	}{
		{"a", "b", false},
		{"a", "", false},
		{"", "", false},
		{"", "b", false},
		{"a/b/c", "d", false},
		{"a", "b/c/d", false},
	}
	for i, test := range tests {
		k, err := vindexKey([]byte(test.docId), test.emitKey)
		if err != nil {
			t.Errorf("expected vindexKey to work: %v, %v, %v, err: %v",
				i, test.docId, test.emitKey, err)
		}
		docId, emitKey, err := vindexKeyParse(k)
		if test.expErr {
			if err == nil {
				t.Errorf("expected vindexKeyParse error: %v, %v, %v",
					i, test.docId, test.emitKey)
			}
		} else {
			if err != nil {
				t.Errorf("expected vindexKeyParse to work: %v, %v, %v, err: %v, %v, %v",
					i, test.docId, test.emitKey, err, docId, emitKey)
			}
			if string(docId) != test.docId || emitKey.(string) != string(test.emitKey) {
				t.Errorf("expected vindexKeyParse to match: %v, %v, %v, err: %v, %v, %v",
					i, test.docId, test.emitKey, err, docId, emitKey)
			}
		}
	}
}

func TestVIndexKeyParseBad(t *testing.T) {
	b := make([]byte, 5)
	_, _, err := vindexKeyParse(b)
	if err == nil {
		t.Errorf("expected err on too many NUL's to vindexKeyParse()")
	}

	corrupt, _ := vindexKey([]byte("hello"), "emit")
	corrupt[0] = 1
	_, _, err = vindexKeyParse(corrupt)
	if err == nil {
		t.Errorf("expected err corrupt input to vindexKeyParse()")
	}
}

func TestVIndexKeyBad(t *testing.T) {
	_, err := vindexKey(nil, vindexKey)
	if err == nil {
		t.Errorf("expected err on non-json'able input")
	}
}

func TestVIndexKeyCompare(t *testing.T) {
	tests := []struct {
		docIdA   string
		emitKeyA interface{}
		docIdB   string
		emitKeyB interface{}
		exp      int
	}{
		{"a", 0, "a", 0, 0},
		{"a", 0, "a", 1, -1},
		{"a", 1, "a", 0, 1},
		{"a", 9, "a", 0, 1},
		{"a", 9, "a", 10, -1},
		{"a", []int{0, 1}, "a", []int{0, 1}, 0},
		{"b", []int{0, 1}, "a", []int{0, 1}, 1},
		{"b", []int{0, 1}, "a", []int{0, 10}, -1},
		{"b", []int{0, 9}, "a", []int{0, 10}, -1},
		{"b", []interface{}{0, "x"}, "a", []interface{}{0, "b"}, 1},
	}
	for i, test := range tests {
		ka, err := vindexKey([]byte(test.docIdA), test.emitKeyA)
		if err != nil {
			t.Errorf("expected no err, got: %v", err)
		}
		kb, err := vindexKey([]byte(test.docIdB), test.emitKeyB)
		if err != nil {
			t.Errorf("expected no err, got: %v", err)
		}
		c := vindexKeyCompare(ka, kb)
		if c != test.exp {
			t.Errorf("expected %v, got %v for vindexKeyCompare(%#v, %#v), i: %v",
				test.exp, c, string(ka), string(kb), i)
		}
	}
}

func TestVIndexKeyNil(t *testing.T) {
	b, err := vindexKey(nil, 123)
	if err != nil {
		t.Errorf("expected no failures, err: %v", err)
	}
	if !bytes.Equal(b, []byte("123\x00")) {
		t.Errorf("didn't get expected bytes, got: %#v", string(b))
	}
	b, err = vindexKey(nil, nil)
	if err != nil {
		t.Errorf("expected no failures, err: %v", err)
	}
	if !bytes.Equal(b, []byte("null\x00")) {
		t.Errorf("didn't get expected bytes, got: %#v", string(b))
	}
	b, err = vindexKey(nil, []int{})
	if err != nil {
		t.Errorf("expected no failures, err: %v", err)
	}
	if !bytes.Equal(b, []byte("[]\x00")) {
		t.Errorf("didn't get expected bytes, got: %#v", string(b))
	}
	b, err = vindexKey(nil, []interface{}(nil))
	if err != nil {
		t.Errorf("expected no failures, err: %v", err)
	}
	if !bytes.Equal(b, []byte("null\x00")) {
		t.Errorf("didn't get expected bytes, got: %#v", string(b))
	}
}

func TestMemoryOnlyViewsStore(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
			MemoryOnly:    MemoryOnly_LEVEL_PERSIST_METADATA,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	vb0, _ := b0.CreateVBucket(2)
	vs, err := vb0.getViewsStore()
	if err != nil {
		t.Errorf("expected getViewsStoreToWork")
	}
	if vs.bsfMemoryOnly == nil {
		t.Errorf("expected memory-only viewsStore to have a memory-only BSF")
	}
	pvr := vb0.periodicViewsRefresh(time.Now())
	if pvr {
		t.Errorf("expected periodicViewsRefresh() on a clean db return false")
	}
}

func TestViewKeyCompareForCollection(t *testing.T) {
	c := viewKeyCompareForCollection("a" + VINDEX_COLL_SUFFIX + "b")
	if c([]byte("10\x00DocId"), []byte("9\x00DocId")) != -1 {
		t.Errorf("expected a bytes.Compare comparison")
	}
	c = viewKeyCompareForCollection("a" + VINDEX_COLL_SUFFIX)
	if c([]byte("10\x00DocId"), []byte("9\x00DocId")) != 1 {
		t.Errorf("expected a JSON comparison")
	}
}

func TestCouchViewWithMutations(t *testing.T) {
	d, _, bucket := testSetupDefaultBucket(t, 1, uint16(0))
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	testSetupDDoc(t, bucket, `{
		"_id":"_design/d0",
		"language": "javascript",
		"views": {
			"v0": {
				"map": "function(doc) { emit(doc.amount, null) }"
			}
		}
    }`, nil)

	testExpectations := func() {
		rr := httptest.NewRecorder()
		r, _ := http.NewRequest("GET",
			"http://127.0.0.1/default/_design/d0/_view/v0?stale=false", nil)
		mr.ServeHTTP(rr, r)
		if rr.Code != 200 {
			t.Errorf("expected req to 200, got: %#v, %v",
				rr, rr.Body.String())
		}
		dd := &ViewResult{}
		err := jsonUnmarshal(rr.Body.Bytes(), dd)
		if err != nil {
			t.Errorf("expected good view result, err: %v", err)
		}
		k := []string{"a", "d", "b", "c"}
		a := []int{1, 2, 3, 4}
		if dd.TotalRows != len(k) {
			t.Errorf("expected %v rows, got: %v, %v, %v",
				len(k), dd.TotalRows, dd, rr.Body.String())
		}
		for i, row := range dd.Rows {
			if k[i] != row.Id {
				t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
			}
			if a[i] != asInt(row.Key) {
				t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
			}
			if row.Doc != nil {
				t.Errorf("expected no doc since it's not include_docs, got: %#v", row)
			}
		}
	}

	testExpectations()

	// Modify some items.
	docFmt := func(i int) string {
		return fmt.Sprintf(`{"amount":%d}`, i)
	}

	var res *gomemcached.MCResponse

	res = SetItem(bucket, []byte("a"), []byte(docFmt(1)),
		VBActive)
	if res == nil || res.Status != gomemcached.SUCCESS {
		t.Errorf("expected SetItem to work, got: %v", res)
	}
	res = SetItem(bucket, []byte("b"), []byte(docFmt(3)),
		VBActive)
	if res == nil || res.Status != gomemcached.SUCCESS {
		t.Errorf("expected SetItem to work, got: %v", res)
	}

	testExpectations() // Re-test the expectations.
}
