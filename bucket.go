package cbgb

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	MAX_VBUCKET        = 1024
	DEFAULT_BUCKET_KEY = "default"
)

// That in which everything is bucketized.
type bucket interface {
	getVBucket(vbid uint16) *vbucket
	Close() error
	Observer() *broadcaster
	Subscribe(ch chan<- interface{})
	Unsubscribe(ch chan<- interface{})
	CreateVBucket(vbid uint16) *vbucket
	SetVBState(vbid uint16, newState VBState) *vbucket
	destroyVBucket(vbid uint16) (destroyed bool)
	Available() bool
}

type vbucketChange struct {
	bucket             bucket
	vbid               uint16
	oldState, newState VBState
}

func (c vbucketChange) getVBucket() *vbucket {
	if c.bucket == nil {
		return nil
	}
	return c.bucket.getVBucket(c.vbid)
}

func (c vbucketChange) String() string {
	return fmt.Sprintf("vbucket %v %v -> %v",
		c.vbid, c.oldState, c.newState)
}

type livebucket struct {
	vbuckets    [MAX_VBUCKET]unsafe.Pointer
	availablech chan bool
	observer    *broadcaster
	dir         string
}

func NewBucket(dirForBucket string) bucket {
	return &livebucket{
		dir:         dirForBucket,
		observer:    newBroadcaster(0),
		availablech: make(chan bool),
	}
}

// Holder of buckets
type Buckets struct {
	buckets map[string]bucket
	dir     string // Directory where all buckets are stored.
	lock    sync.Mutex
}

// Build a new holder of buckets.
func NewBuckets(dirForBuckets string) *Buckets {
	// TODO: Need to load existing buckets from the dir.
	return &Buckets{buckets: map[string]bucket{}, dir: dirForBuckets}
}

// Create a new named bucket.
// Return the new bucket, or nil if the bucket already exists.
func (b *Buckets) New(name string) bucket {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.buckets[name] != nil {
		return nil
	}

	// The suffix allows non-buckets to be ignored in that directory.
	bdir := b.dir + string(os.PathSeparator) + name + "-bucket"
	os.Mkdir(bdir, 0777)

	f, err := os.Open(bdir)
	if err != nil {
		return nil
	}
	defer f.Close()
	if finfo, err := f.Stat(); err != nil || !finfo.IsDir() {
		return nil
	}

	rv := NewBucket(bdir)
	b.buckets[name] = rv
	return rv
}

// Get the named bucket (or nil if it doesn't exist).
func (b *Buckets) Get(name string) bucket {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.buckets[name]
}

// Destroy the named bucket.
func (b *Buckets) Destroy(name string) {
	b.lock.Lock()
	defer b.lock.Unlock()

	bucket := b.buckets[name]
	if bucket != nil {
		bucket.Close()
		delete(b.buckets, name)
	}
}

func (b *livebucket) Observer() *broadcaster {
	return b.observer
}

// Subscribe to bucket events.
//
// Note that this is retroactive -- it will send existing states.
func (b *livebucket) Subscribe(ch chan<- interface{}) {
	b.observer.Register(ch)
	go func() {
		for i := uint16(0); i < MAX_VBUCKET; i++ {
			c := vbucketChange{bucket: b,
				vbid:     i,
				oldState: VBDead,
				newState: VBDead}
			vb := c.getVBucket()
			if vb != nil {
				s := vb.GetVBState()
				if s != VBDead {
					c.newState = s
					ch <- c
				}
			}
		}
	}()
}

func (b *livebucket) Unsubscribe(ch chan<- interface{}) {
	b.observer.Unregister(ch)
}

func (b *livebucket) Close() error {
	close(b.availablech)
	return nil
}

func (b *livebucket) Available() bool {
	select {
	default:
	case <-b.availablech:
		return false
	}
	return true
}

func (b *livebucket) getVBucket(vbid uint16) *vbucket {
	if b == nil || !b.Available() {
		return nil
	}
	vbp := atomic.LoadPointer(&b.vbuckets[vbid])
	return (*vbucket)(vbp)
}

func (b *livebucket) casVBucket(vbid uint16, vb *vbucket, vbPrev *vbucket) bool {
	return atomic.CompareAndSwapPointer(&b.vbuckets[vbid],
		unsafe.Pointer(vbPrev), unsafe.Pointer(vb))
}

func (b *livebucket) CreateVBucket(vbid uint16) *vbucket {
	if b == nil || !b.Available() {
		return nil
	}
	vb, err := newVBucket(b, vbid, b.dir)
	if err != nil {
		return nil // TODO: Error propagation / logging.
	}
	if b.casVBucket(vbid, vb, nil) {
		return vb
	}
	return nil
}

func (b *livebucket) destroyVBucket(vbid uint16) (destroyed bool) {
	destroyed = false
	vb := b.getVBucket(vbid)
	if vb != nil {
		vb.SetVBState(VBDead, func(oldState VBState) {
			if b.casVBucket(vbid, nil, vb) {
				b.observer.Submit(vbucketChange{b, vbid, oldState, VBDead})
				destroyed = true
			}
		})
	}
	return
}

func (b *livebucket) SetVBState(vbid uint16, newState VBState) *vbucket {
	vb := b.getVBucket(vbid)
	if vb != nil {
		vb.SetVBState(newState, func(oldState VBState) {
			if b.getVBucket(vbid) == vb {
				b.observer.Submit(vbucketChange{b, vbid, oldState, newState})
			} else {
				vb = nil
			}
		})
	}
	return vb
}
