package cbgb

import (
	"bytes"
	"encoding/binary"
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
	if err := binary.Write(buf, binary.BigEndian, uint32(len(i.data))); err != nil {
		return nil
	}
	if _, err := buf.Write(i.data); err != nil {
		return nil
	}
	return buf.Bytes()
}

func (i *item) fromValueBytes(b []byte) (err error) {
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
	var length uint32
	if err = binary.Read(buf, binary.BigEndian, &length); err != nil {
		return err
	}
	start := 4 + 4 + 8 + 4
	i.data = b[start : start+int(length)]
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
