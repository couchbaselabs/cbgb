package cbgb

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

const (
	MAX_VBUCKET = 1024
)

type vbucketChange struct {
	bucket             *bucket
	vbid               uint16
	oldState, newState VBState
}

func (c vbucketChange) getVBucket() *vbucket {
	return c.bucket.getVBucket(c.vbid)
}

func (c vbucketChange) String() string {
	return fmt.Sprintf("vbucket %v %v -> %v",
		c.vbid, c.oldState, c.newState)
}

type bucket struct {
	vbuckets [MAX_VBUCKET]unsafe.Pointer
	observer *broadcaster
}

func NewBucket() *bucket {
	return &bucket{
		observer: newBroadcaster(0),
	}
}

func (b *bucket) Observer() *broadcaster {
	return b.observer
}

// Subscribe to bucket events.
//
// Note that this is retroactive -- it will send existing states.
func (b *bucket) Subscribe(ch chan<- interface{}) {
	b.observer.Register(ch)
	go func() {
		for i := uint16(0); i < MAX_VBUCKET; i++ {
			c := vbucketChange{bucket: b,
				vbid:     i,
				oldState: VBDead,
				newState: VBDead}
			vb := c.getVBucket()
			if vb != nil {
				s := vb.GetState()
				if s != VBDead {
					c.newState = s
					ch <- c
				}
			}
		}
	}()
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

func (b *bucket) CreateVBucket(vbid uint16) *vbucket {
	vb := newVbucket(vbid)
	b.setVBucket(vbid, vb)
	return vb
}

func (b *bucket) destroyVBucket(vbid uint16) {
	b.SetVBState(vbid, VBDead)
	b.setVBucket(vbid, nil)
}

func (b *bucket) SetVBState(vbid uint16, to VBState) {
	vb := b.getVBucket(vbid)
	oldState := VBDead
	if vb != nil {
		oldState = vb.SetState(to)
	}
	bc := vbucketChange{b, vbid, oldState, to}
	b.observer.Submit(bc)
}
