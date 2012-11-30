package main

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

type bucketChange struct {
	vbid    uint16
	deleted bool
}

func (c bucketChange) String() string {
	t := "created"
	if c.deleted {
		t = "deleted"
	}
	return fmt.Sprintf("vbucket %v %v", c.vbid, t)
}

type bucket struct {
	vbuckets [MAX_VBUCKET]unsafe.Pointer
	observer *broadcaster
}

func newBucket() *bucket {
	return &bucket{
		observer: newBroadcaster(10),
	}
}

func (b *bucket) getVBucket(vbid uint16) *vbucket {
	vbp := atomic.LoadPointer(&b.vbuckets[vbid])
	return (*vbucket)(vbp)
}

func (b *bucket) setVBucket(vbid uint16, vb *vbucket) {
	atomic.StorePointer(&b.vbuckets[vbid], unsafe.Pointer(vb))
	b.observer.Submit(bucketChange{vbid, vb == nil})
}

func (b *bucket) createVBucket(vbid uint16) *vbucket {
	vb := newVbucket(vbid)
	b.setVBucket(vbid, vb)
	return vb
}
