package cbgb

import (
	"bytes"
	"encoding/json"
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
			"endkey":         `"ZZ"`,
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
