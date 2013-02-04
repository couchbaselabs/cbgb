package cbgb

import (
	"bytes"
	"testing"
)

func TestItemCloneEqual(t *testing.T) {
	i := &item{
		key:  []byte("a"),
		exp:  123,
		flag: 321,
		cas:  111,
		data: []byte("b"),
	}
	j := i.clone()
	if !i.Equal(j) {
		t.Errorf("expected item clone/equal to work")
	}
	if !j.Equal(i) {
		t.Errorf("expected item clone/equal to work")
	}
	k := &item{}
	if k.Equal(i) {
		t.Errorf("expected item equal to detect non-equal")
	}
	if i.Equal(k) {
		t.Errorf("expected item equal to detect non-equal")
	}
}

func TestItemDeletionSentinel(t *testing.T) {
	i := &item{}
	if i.isDeletion() {
		t.Errorf("expected not-a-deletion sentinel")
	}
	i.markAsDeletion()
	if !i.isDeletion() {
		t.Errorf("expected deletion sentinel")
	}
}

func TestItemSerialization(t *testing.T) {
	i := &item{
		key:  []byte("a"),
		exp:  123,
		flag: 321,
		cas:  111,
		data: []byte("b"),
	}
	ib := i.toValueBytes()
	if ib == nil {
		t.Errorf("expected item.toValueBytes() to work")
	}
	j := &item{}
	err := j.fromValueBytes(ib)
	if err != nil {
		t.Errorf("expected item.fromValueBytes() to work")
	}
	if !i.Equal(j) {
		t.Errorf("expected serialize/deserialize to equal")
	}
	jb := j.toValueBytes()
	if jb == nil {
		t.Errorf("expected item.toValueBytes() to work")
	}
	if !bytes.Equal(ib, jb) {
		t.Errorf("expected item.toValueBytes() to be the same")
	}
}
