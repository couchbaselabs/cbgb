package main

import (
	"sync/atomic"
	"unsafe"
)

type bucket struct {
	vbuckets [MAX_VBUCKET]unsafe.Pointer
}

func (b *bucket) getVBucket(vbid uint16) *vbucket {
	vbp := atomic.LoadPointer(&b.vbuckets[vbid])
	return (*vbucket)(vbp)
}

func (b *bucket) setVBucket(vbid uint16, vb *vbucket) {
	atomic.StorePointer(&b.vbuckets[vbid], unsafe.Pointer(vb))
}

func (b *bucket) createVBucket(vbid uint16) *vbucket {
	vb := newVbucket(vbid)
	b.setVBucket(vbid, vb)
	return vb
}
