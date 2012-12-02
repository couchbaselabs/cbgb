package main

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

type bucketChange struct {
	bucket             *bucket
	vbid               uint16
	oldState, newState vbState
}

func (c bucketChange) getVBucket() *vbucket {
	return c.bucket.getVBucket(c.vbid)
}

func (c bucketChange) String() string {
	return fmt.Sprintf("vbucket %v %v -> %v",
		c.vbid, c.oldState, c.newState)
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
	if b == nil {
		return nil
	}
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

func (b *bucket) destroyVBucket(vbid uint16) {
	b.setVBState(vbid, vbDead)
	b.setVBucket(vbid, nil)
}

func (b *bucket) setVBState(vbid uint16, to vbState) {
	vb := b.getVBucket(vbid)
	oldState := vbDead
	if vb != nil {
		oldState = vb.state
		vb.state = to
	}
	b.observer.Submit(bucketChange{b, vbid, oldState, to})
}
