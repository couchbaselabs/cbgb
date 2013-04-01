package main

import (
	"bytes"
	"sync/atomic"
)

type VBState uint8

const (
	_ = VBState(iota)
	VBActive
	VBReplica
	VBPending
	VBDead
)

var vbStateNames = []string{
	VBActive:  "active",
	VBReplica: "replica",
	VBPending: "pending",
	VBDead:    "dead",
}

func (v VBState) String() string {
	if v < VBActive || v > VBDead {
		panic("Invalid vb state")
	}
	return vbStateNames[v]
}

func parseVBState(s string) VBState {
	for i, v := range vbStateNames {
		if v != "" && v == s {
			return VBState(uint8(i))
		}
	}
	return VBDead
}

type VBKeyRange struct {
	MinKeyInclusive Bytes `json:"minKeyInclusive"`
	MaxKeyExclusive Bytes `json:"maxKeyExclusive"`
}

func (t *VBKeyRange) Equal(u *VBKeyRange) bool {
	if t == nil {
		return u == nil
	}
	if u == nil {
		return false
	}
	return bytes.Equal(t.MinKeyInclusive, u.MinKeyInclusive) &&
		bytes.Equal(t.MaxKeyExclusive, u.MaxKeyExclusive)
}

type VBMeta struct {
	Id       uint16      `json:"id"`
	LastCas  uint64      `json:"lastCas"`
	MetaCas  uint64      `json:"metaCas"`
	State    string      `json:"state"`
	KeyRange *VBKeyRange `json:"keyRange"`
}

func (t *VBMeta) Equal(u *VBMeta) bool {
	return t.Id == u.Id &&
		t.LastCas == u.LastCas &&
		t.MetaCas == u.MetaCas &&
		t.State == u.State &&
		t.KeyRange.Equal(u.KeyRange)
}

func (t *VBMeta) Copy() *VBMeta {
	return (&VBMeta{Id: t.Id}).update(t)
}

func (t *VBMeta) update(from *VBMeta) *VBMeta {
	t.State = parseVBState(from.State).String()
	fromCas := atomic.LoadUint64(&from.LastCas)
	if atomic.LoadUint64(&t.LastCas) < fromCas {
		atomic.StoreUint64(&t.LastCas, fromCas)
	}
	metaCas := atomic.LoadUint64(&from.MetaCas)
	if atomic.LoadUint64(&t.MetaCas) < metaCas {
		atomic.StoreUint64(&t.MetaCas, metaCas)
	}
	t.KeyRange = nil
	if from.KeyRange != nil {
		t.KeyRange = &VBKeyRange{
			MinKeyInclusive: from.KeyRange.MinKeyInclusive,
			MaxKeyExclusive: from.KeyRange.MaxKeyExclusive,
		}
	}
	return t
}
