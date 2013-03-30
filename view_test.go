package cbgb

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"testing"
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

	b0, err := NewBucket(testBucketDir,
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

func TestGetViewsStore(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
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
