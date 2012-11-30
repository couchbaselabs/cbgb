package main

import (
	"testing"

	"github.com/dustin/gomemcached"
)

func bodyEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestBasicOps(t *testing.T) {
	empty := []byte{}
	active := uint16(3)
	ignored := gomemcached.Status(32768)

	tests := []struct {
		op  gomemcached.CommandCode
		vb  uint16
		key string
		val string

		expStatus gomemcached.Status
		expValue  []byte
	}{
		{gomemcached.SET, active, "a", "aye",
			gomemcached.SUCCESS, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.SUCCESS, []byte("aye")},
		{gomemcached.GET, 2, "a", "",
			gomemcached.NOT_MY_VBUCKET, empty},
		{gomemcached.GET, active, "b", "",
			gomemcached.KEY_ENOENT, empty},
		{gomemcached.DELETE, active, "a", "",
			gomemcached.SUCCESS, empty},
		{gomemcached.DELETE, active, "a", "",
			gomemcached.KEY_ENOENT, empty},
		{gomemcached.GET, active, "a", "",
			gomemcached.KEY_ENOENT, empty},

		// quiet
		{gomemcached.GETQ, active, "a", "aye",
			ignored, empty},
		{gomemcached.SET, active, "a", "aye",
			gomemcached.SUCCESS, empty},
		{gomemcached.GETQ, active, "a", "",
			gomemcached.SUCCESS, []byte("aye")},
	}

	testBucket := &bucket{}
	rh := reqHandler{testBucket}
	testBucket.createVBucket(3)

	for _, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: x.vb,
			Key:     []byte(x.key),
			Body:    []byte(x.val),
		}

		res := rh.HandleMessage(nil, req)

		if res == nil && x.expStatus == ignored {
			// this was a "normal" quiet command
			continue
		}

		if res.Status != x.expStatus {
			t.Errorf("Expected %v for %v:%v/%v, got %v",
				x.expStatus, x.op, x.vb, x.key, res.Status)
		}

		if !bodyEqual(x.expValue, res.Body) {
			t.Errorf("Expected body of %v:%v/%v to be\n%#v\ngot\n%#v",
				x.op, x.vb, x.key, x.expValue, res.Body)
		}
	}
}
