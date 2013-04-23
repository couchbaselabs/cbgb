package main

import (
	"fmt"
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

type VBMeta struct {
	LastCas uint64 `json:"lastCas"`
	MetaCas uint64 `json:"metaCas"`
	State   string `json:"state"`
	Id      uint16 `json:"id"`
}

func (t *VBMeta) Equal(u *VBMeta) bool {
	return t.Id == u.Id &&
		t.LastCas == u.LastCas &&
		t.MetaCas == u.MetaCas &&
		t.State == u.State
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
	return t
}

type vbucketChange struct {
	bucket             Bucket
	vbid               uint16
	oldState, newState VBState
}

func (c vbucketChange) getVBucket() *VBucket {
	if c.bucket == nil {
		return nil
	}
	vb, _ := c.bucket.GetVBucket(c.vbid)
	return vb
}

func (c vbucketChange) String() string {
	return fmt.Sprintf("bucket: %s, vbucket: %v, state: %v -> %v",
		c.bucket.Name(), c.vbid, c.oldState, c.newState)
}
