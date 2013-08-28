// Copyright (c) 2013 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License. You
// may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"encoding/binary"
	"errors"
)

const minObsReq = 4

const (
	obsNotPersisted = 0
	obsPersisted    = 1
	obsNotFound     = 0x80
	obsDeleted      = 0x81
)

type obsKey struct {
	vbid uint16
	key  []byte
}

type obsStatus struct {
	obsKey
	state byte
	cas   uint64
}

var obsError = errors.New("observation request parse error")

func parseObserveKeys(from []byte) ([]obsKey, error) {
	rv := []obsKey{}

	for len(from) > 0 {
		if len(from) < minObsReq {
			return nil, obsError
		}
		ob := obsKey{vbid: binary.BigEndian.Uint16(from)}
		klen := int(binary.BigEndian.Uint16(from[2:]))
		from = from[4:]
		if len(from) < klen {
			return nil, obsError
		}
		ob.key = from[:klen]
		from = from[klen:]

		rv = append(rv, ob)
	}

	return rv, nil
}

func encodeObserveBody(r []obsStatus) []byte {
	rv := []byte{}

	for _, v := range r {
		// vb, klen, key, status, CAS
		stuff := make([]byte, 2+2+len(v.key)+1+8)
		p := stuff
		binary.BigEndian.PutUint16(p, v.vbid)
		p = p[2:]
		binary.BigEndian.PutUint16(p, uint16(len(v.key)))
		p = p[2:]
		copy(stuff[4:], v.key)
		p = p[len(v.key):]
		p[0] = v.state
		p = p[1:]
		binary.BigEndian.PutUint64(p, v.cas)

		rv = append(rv, stuff...)
	}

	return rv
}
