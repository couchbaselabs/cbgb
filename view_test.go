package cbgb

import (
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
