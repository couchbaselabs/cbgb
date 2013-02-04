package cbgb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type item struct {
	key       []byte
	exp, flag uint32
	cas       uint64
	data      []byte
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

// Serialize everything but the key.
func (i *item) toValueBytes() []byte {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, i.exp); err != nil {
		return nil
	}
	if err := binary.Write(buf, binary.BigEndian, i.flag); err != nil {
		return nil
	}
	if err := binary.Write(buf, binary.BigEndian, i.cas); err != nil {
		return nil
	}
	// TODO: Handle if len(key) > uint16.
	if err := binary.Write(buf, binary.BigEndian, uint16(len(i.key))); err != nil {
		return nil
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(i.data))); err != nil {
		return nil
	}
	if _, err := buf.Write(i.key); err != nil {
		return nil
	}
	if _, err := buf.Write(i.data); err != nil {
		return nil
	}
	return buf.Bytes()
}

func (i *item) fromValueBytes(b []byte) (err error) {
	hdrlen := 4 + 4 + 8 + 2 + 4
	if hdrlen > len(b) {
		return fmt.Errorf("item.fromValueBytes(): arr too short: %v, minimum: %v",
			len(b), hdrlen)
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
	if len(b) < hdrlen+int(keylen)+int(datalen) {
		return fmt.Errorf("item.fromValueBytes(): arr too short: %v, wanted: %v",
			len(b), hdrlen+int(keylen)+int(datalen))
	}
	if keylen > 0 {
		i.key = b[hdrlen : hdrlen+int(keylen)]
	} else {
		i.key = []byte("")
	}
	if datalen > 0 {
		i.data = b[hdrlen+int(keylen) : hdrlen+int(keylen)+int(datalen)]
	} else {
		i.data = []byte("")
	}
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
	buf := bytes.NewBuffer(make([]byte, 8)[:0])
	binary.Write(buf, binary.BigEndian, cas)
	return buf.Bytes()
}

func casBytesParse(b []byte) (cas uint64, err error) {
	if len(b) < 8 {
		return 0, fmt.Errorf("item.casBytesParse() arr len: %v", len(b))
	}
	buf := bytes.NewBuffer(b)
	if err = binary.Read(buf, binary.BigEndian, &cas); err != nil {
		return 0, err
	}
	return cas, nil
}
