package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/steveyen/gkvlite"
)

const DELETION_EXP  = 0x80000000 // Deletion sentinel exp.
const DELETION_FLAG = 0xffffffff // Deletion sentinel flag.

type item struct {
	key       []byte
	exp, flag uint32
	cas       uint64
	data      []byte
}

func (i item) String() string {
	return fmt.Sprintf("{item key=%s}", i.key)
}

func (i *item) clone() *item {
	return &item{
		key:  i.key,
		exp:  i.exp,
		flag: i.flag,
		cas:  i.cas,
		data: i.data,
	}
}

func (i *item) markAsDeletion() *item {
	i.exp = DELETION_EXP
	i.flag = DELETION_FLAG
	i.data = nil
	return i
}

func (i *item) isDeletion() bool {
	return i.exp == DELETION_EXP && i.flag == DELETION_FLAG &&
		(i.data == nil || len(i.data) == 0)
}

func (i *item) Equal(j *item) bool {
	return bytes.Equal(i.key, j.key) &&
		i.exp == j.exp &&
		i.flag == j.flag &&
		i.cas == j.cas &&
		bytes.Equal(i.data, j.data)
}

func (i item) isExpired(t time.Time) bool {
	if i.exp == 0 {
		return false
	}
	return !time.Unix(int64(i.exp), 0).After(t)
}

const itemHdrLen = 4 + 4 + 8 + 2 + 4

func (i *item) toValueBytes() []byte {
	if len(i.key) > MAX_ITEM_KEY_LENGTH {
		return nil
	}
	if len(i.data) > MAX_ITEM_DATA_LENGTH {
		return nil
	}

	rv := make([]byte, itemHdrLen+len(i.key)+len(i.data))
	off := 0
	binary.BigEndian.PutUint32(rv[off:], i.exp)
	off += 4
	binary.BigEndian.PutUint32(rv[off:], i.flag)
	off += 4
	binary.BigEndian.PutUint64(rv[off:], i.cas)
	off += 8
	binary.BigEndian.PutUint16(rv[off:], uint16(len(i.key)))
	off += 2
	binary.BigEndian.PutUint32(rv[off:], uint32(len(i.data)))
	off += 4
	n := copy(rv[off:], i.key)
	off += n
	copy(rv[off:], i.data)
	return rv
}

func (i *item) fromValueBytes(b []byte) (err error) {
	if itemHdrLen > len(b) {
		return fmt.Errorf("item.fromValueBytes(): arr too short: %v, minimum: %v",
			len(b), itemHdrLen)
	}
	buf := bytes.NewBuffer(b)
	must(binary.Read(buf, binary.BigEndian, &i.exp))
	must(binary.Read(buf, binary.BigEndian, &i.flag))
	must(binary.Read(buf, binary.BigEndian, &i.cas))
	var keylen uint16
	must(binary.Read(buf, binary.BigEndian, &keylen))
	var datalen uint32
	must(binary.Read(buf, binary.BigEndian, &datalen))
	if len(b) < itemHdrLen+int(keylen)+int(datalen) {
		return fmt.Errorf("item.fromValueBytes(): arr too short: %v, wanted: %v",
			len(b), itemHdrLen+int(keylen)+int(datalen))
	}
	if keylen > 0 {
		i.key = b[itemHdrLen : itemHdrLen+int(keylen)]
	} else {
		i.key = []byte{}
	}
	if datalen > 0 {
		i.data = b[itemHdrLen+int(keylen) : itemHdrLen+int(keylen)+int(datalen)]
	} else {
		i.data = []byte{}
	}
	return nil
}

// Returns the number of bytes needed to persist the item into
// the changes collection (not counting any gkvlite tree nodes).
func (i *item) NumBytes() int64 {
	// 8 == sizeof CAS, which is the key used in the changes collection.
	return int64(len(i.key)+len(i.data)) + itemHdrLen + 8
}

func itemValLength(coll *gkvlite.Collection, i *gkvlite.Item) int {
	if !strings.HasSuffix(coll.Name(), COLL_SUFFIX_CHANGES) {
		return len(i.Val)
	}
	if i.Val != nil {
		return len(i.Val)
	}
	if i.Transient == unsafe.Pointer(nil) {
		// TODO: The item might have nil Val when gkvlite is
		// traversing with a withValue of false; so we haven't
		// read/unmarshal'ed the Val/Transient yet.  Impact: the byte
		// aggregate math might wrong, and if we try to write with
		// this 0 length, it'll be wrong; but, assuming here that the
		// item.Loc() is non-empty so we'll never try to write this
		// nil Val/Transient.
		// panic(fmt.Sprintf("itemValLength saw nil Transient, i: %#v, coll.name: %v",
		// 	i, coll.Name()))
		return 0
	}
	ti := (interface{})(i.Transient)
	item, ok := ti.(*item)
	if !ok {
		panic(fmt.Sprintf("itemValLength invoked on non-item, i: %#v", i))
	}
	if item == nil {
		panic(fmt.Sprintf("itemValLength invoked on nil item, i: %#v", i))
	}
	return itemHdrLen + len(item.key) + len(item.data)
}

func itemValWrite(coll *gkvlite.Collection, i *gkvlite.Item,
	w io.WriterAt, offset int64) error {
	if !strings.HasSuffix(coll.Name(), COLL_SUFFIX_CHANGES) {
		_, err := w.WriteAt(i.Val, offset)
		return err
	}
	if i.Val != nil {
		_, err := w.WriteAt(i.Val, offset)
		return err
	}
	if i.Transient == unsafe.Pointer(nil) {
		panic(fmt.Sprintf("itemValWrite saw nil Transient, i: %#v", i))
	}
	ti := (interface{})(i.Transient)
	item, ok := ti.(*item)
	if !ok {
		panic(fmt.Sprintf("itemValWrite invoked on non-item, i: %#v", i))
	}
	if item == nil {
		panic(fmt.Sprintf("itemValWrite invoked on nil item, i: %#v", i))
	}
	vBytes := item.toValueBytes()
	_, err := w.WriteAt(vBytes, offset)
	return err
}

func itemValRead(coll *gkvlite.Collection, i *gkvlite.Item,
	r io.ReaderAt, offset int64, valLength uint32) error {
	if i.Val != nil {
		panic(fmt.Sprintf("itemValRead saw non-nil Val, i: %#v", i))
	}
	i.Val = make([]byte, valLength)
	_, err := r.ReadAt(i.Val, offset)
	if err != nil {
		return err
	}
	if !strings.HasSuffix(coll.Name(), COLL_SUFFIX_CHANGES) {
		return nil
	}
	x := &item{}
	if err = x.fromValueBytes(i.Val); err != nil {
		return err
	}
	atomic.StorePointer(&i.Transient, unsafe.Pointer(x))
	return nil
}

func KeyLess(p, q interface{}) int {
	return bytes.Compare(p.(*item).key, q.(*item).key)
}

func CASLess(p, q interface{}) int {
	if p.(*item).cas < q.(*item).cas {
		return -1
	}
	if p.(*item).cas == q.(*item).cas {
		return 0
	}
	return 1
}

func casBytes(cas uint64) []byte {
	return casBytesFill(cas, make([]byte, 8))
}

func casBytesFill(cas uint64, buf []byte) []byte {
	binary.BigEndian.PutUint64(buf, cas)
	return buf
}

func casBytesParse(b []byte) (cas uint64, err error) {
	if len(b) < 8 {
		return 0, fmt.Errorf("item.casBytesParse() arr len: %v", len(b))
	}
	return binary.BigEndian.Uint64(b), nil
}
