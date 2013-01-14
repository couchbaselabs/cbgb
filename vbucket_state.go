package cbgb

import (
	"bytes"
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
	State    string      `json:"state"`
	KeyRange *VBKeyRange `json:"keyRange"`
}

func (t *VBMeta) Equal(u *VBMeta) bool {
	return t.Id == u.Id &&
		t.LastCas == u.LastCas &&
		t.State == u.State &&
		t.KeyRange.Equal(u.KeyRange)
}

func (t *VBMeta) update(from *VBMeta) {
	t.State = parseVBState(from.State).String()
	if t.LastCas < from.LastCas {
		t.LastCas = from.LastCas
	}
	t.KeyRange = from.KeyRange
}
