package cbgb

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	MAX_VBUCKET        = 1024
	DEFAULT_BUCKET_KEY = "default"
	BUCKET_DIR_SUFFIX  = "-bucket" // Suffix allows non-buckets to be ignored.
)

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

// Holder of buckets.
type Buckets struct {
	buckets map[string]bucket
	dir     string // Directory where all buckets are stored.
	lock    sync.Mutex
}

// Build a new holder of buckets.
func NewBuckets(dirForBuckets string) (*Buckets, error) {
	if !isDir(dirForBuckets) {
		return nil, errors.New(fmt.Sprintf("not a directory: %v", dirForBuckets))
	}
	return &Buckets{buckets: map[string]bucket{}, dir: dirForBuckets}, nil
}

// Create a new named bucket.
// Return the new bucket, or nil if the bucket already exists.
func (b *Buckets) New(name string) bucket {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.buckets[name] != nil {
		return nil
	}

	// TODO: Need name checking & encoding for safety/security.
	bdir := b.dir + string(os.PathSeparator) + name + BUCKET_DIR_SUFFIX
	os.Mkdir(bdir, 0777)
	if !isDir(bdir) {
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

// Reads the buckets directory and returns list of bucket names.
func (b *Buckets) LoadNames() ([]string, error) {
	list, err := ioutil.ReadDir(b.dir)
	if err == nil {
		res := make([]string, 0, len(list))
		for _, entry := range list {
			if entry.IsDir() &&
				strings.HasSuffix(entry.Name(), BUCKET_DIR_SUFFIX) {
				res = append(res,
					entry.Name()[0:len(entry.Name())-len(BUCKET_DIR_SUFFIX)])
			}
		}
		return res, nil
	}
	return nil, err
}

// Loads all buckets from the buckets directory.
func (b *Buckets) Load() error {
	bucketNames, err := b.LoadNames()
	if err != nil {
		return err
	}
	for _, bucketName := range bucketNames {
		b := b.New(bucketName)
		if b == nil {
			return errors.New(fmt.Sprintf("loading bucket %v, but it exists already",
				bucketName))
		}
		// if err = b.Load(); err != nil {
		// 	return err
		// }
	}
	return nil
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
