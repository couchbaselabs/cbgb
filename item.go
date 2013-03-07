package cbgb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

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

// Serialize everything but the key.
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
	if err = binary.Read(buf, binary.BigEndian, &i.exp); err != nil {
		return err
	}
	if err = binary.Read(buf, binary.BigEndian, &i.flag); err != nil {
		return err
	}
	if err = binary.Read(buf, binary.BigEndian, &i.cas); err != nil {
		return err
	}
	var keylen uint16
	if err = binary.Read(buf, binary.BigEndian, &keylen); err != nil {
		return err
	}
	var datalen uint32
	if err = binary.Read(buf, binary.BigEndian, &datalen); err != nil {
		return err
	}
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

func (i *item) KeyDataNumBytes() int64 {
	return int64(len(i.key) + len(i.data))
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
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, cas)
	return buf
}

func casBytesParse(b []byte) (cas uint64, err error) {
	if len(b) < 8 {
		return 0, fmt.Errorf("item.casBytesParse() arr len: %v", len(b))
	}
	return binary.BigEndian.Uint64(b), nil
}
