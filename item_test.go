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
		exp:  0x81234321,
		flag: 0xffffffff,
		cas:  0xfedcba9876432100,
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

	kb := []byte{}
	k := &item{}
	err = k.fromValueBytes(kb)
	if err == nil {
		t.Errorf("expected toValueBytes error on empty buf")
	}

	kb = ib[1:]
	err = k.fromValueBytes(kb)
	if err == nil {
		t.Errorf("expected toValueBytes error on empty buf")
	}

	kb = ib[4:]
	err = k.fromValueBytes(kb)
	if err == nil {
		t.Errorf("expected toValueBytes error on empty buf")
	}
}

func TestItemNilSerialization(t *testing.T) {
	i := &item{
		key:  nil,
		data: nil,
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
	if j.key == nil {
		t.Errorf("expected item.key to be non-nil")
	}
	if j.data == nil {
		t.Errorf("expected item.data to be non-nil")
	}
	if len(j.key) != 0 || len(j.data) != 0 {
		t.Errorf("expected item.key/data to be 0 len")
	}
}

func TestCASSerialization(t *testing.T) {
	cas0 := uint64(0xfedcba9876432100)
	b0 := casBytes(cas0)
	if b0 == nil {
		t.Errorf("expected casBytes() to be non-nil")
	}
	cas1, err := casBytesParse(b0)
	if err != nil {
		t.Errorf("expected casBytesParse() to work")
	}
	if cas1 != cas0 {
		t.Errorf("expected cas to equal")
	}

	_, err = casBytesParse([]byte{})
	if err == nil {
		t.Errorf("expected casBytesParse() to error on short bytes")
	}

	_, err = casBytesParse(b0[1:])
	if err == nil {
		t.Errorf("expected casBytesParse() to error on short bytes")
	}
}

func TestKeyLess(t *testing.T) {
	if KeyLess(&item{key: []byte("a")}, &item{key: []byte("b")}) >= 0 {
		t.Errorf("expected KeyLess to work")
	}
	if KeyLess(&item{key: []byte("a")}, &item{key: []byte("aa")}) >= 0 {
		t.Errorf("expected KeyLess to work")
	}
	if KeyLess(&item{key: []byte("a")}, &item{key: []byte("a")}) != 0 {
		t.Errorf("expected KeyLess to work")
	}
	if KeyLess(&item{key: []byte{}}, &item{key: []byte{}}) != 0 {
		t.Errorf("expected KeyLess to work for zero-length keys")
	}
	if KeyLess(&item{}, &item{}) != 0 {
		t.Errorf("expected KeyLess to work for nil keys")
	}
}

func TestCASLess(t *testing.T) {
	if CASLess(&item{cas: 1}, &item{cas: 0}) < 0 {
		t.Errorf("expected CASLess to work")
	}
	if CASLess(&item{cas: 0}, &item{cas: 1}) >= 0 {
		t.Errorf("expected CASLess to work")
	}
	if CASLess(&item{cas: 1}, &item{cas: 1}) != 0 {
		t.Errorf("expected CASLess to work")
	}
}
