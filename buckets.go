package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

var quiescePeriodic *periodically

var noBucket = errors.New("No bucket")

// Holder of buckets.
type Buckets struct {
	buckets  map[string]Bucket
	dir      string // Directory where all buckets are stored.
	lock     sync.Mutex
	settings *BucketSettings
}

// Build a new holder of buckets.
func NewBuckets(bdir string, settings *BucketSettings) (*Buckets, error) {
	if err := os.MkdirAll(bdir, 0777); err != nil && !isDir(bdir) {
		return nil, fmt.Errorf("could not create/access bucket dir: %v", bdir)
	}
	buckets := &Buckets{
		buckets:  map[string]Bucket{},
		dir:      bdir,
		settings: settings.Copy(),
	}
	return buckets, nil
}

// Allocates and registers a new, named bucket, or error if it exists.
func (b *Buckets) New(name string,
	defaultSettings *BucketSettings) (Bucket, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.new_unlocked(name, defaultSettings)
}

func (b *Buckets) new_unlocked(name string,
	defaultSettings *BucketSettings) (rv Bucket, err error) {
	if b.buckets[name] != nil {
		return nil, fmt.Errorf("bucket already exists: %v", name)
	}
	rv, err = b.alloc_unlocked(name, true, defaultSettings)
	if err != nil {
		return nil, err
	}
	b.register_unlocked(name, rv)
	return rv, nil
}

func (b *Buckets) alloc_unlocked(name string, create bool,
	defaultSettings *BucketSettings) (rv Bucket, err error) {
	settings := &BucketSettings{}
	if defaultSettings != nil {
		settings = defaultSettings.Copy()
	}
	settings.UUID = CreateNewUUID()

	bdir, err := b.Path(name)
	if err != nil {
		return nil, err
	}
	exists, err := settings.load(bdir)
	if err != nil {
		return nil, err
	}

	if !(exists || create) {
		return nil, noBucket
	}

	if settings.MemoryOnly < MemoryOnly_LEVEL_PERSIST_NOTHING {
		// If an accessible bdir directory exists already, it's ok.
		if err = os.MkdirAll(bdir, 0777); err != nil && !isDir(bdir) {
			return nil, fmt.Errorf("could not access bucket dir: %v", bdir)
		}
	}

	return NewBucket(name, bdir, settings)
}

func (b *Buckets) register_unlocked(name string, bucket Bucket) {
	var ch chan bool
	if lb, ok := bucket.(*livebucket); ok {
		ch = lb.availablech
	}
	quiescePeriodic.Register(ch, b.makeQuiescer(name))
	b.buckets[name] = bucket
}

func (b *Buckets) GetNames() []string {
	b.lock.Lock()
	defer b.lock.Unlock()

	res := make([]string, 0, len(b.buckets))
	for name := range b.buckets {
		res = append(res, name)
	}
	return res
}

// Get the named bucket (or nil if it doesn't exist).
func (b *Buckets) Get(name string) Bucket {
	b.lock.Lock()
	defer b.lock.Unlock()

	rv := b.buckets[name]
	if rv != nil {
		return rv
	}

	// The entry is nil (previously quiesced), so try to re-load it.
	rv, err := b.loadBucket_unlocked(name, false)
	if err != nil {
		return nil
	}
	return rv
}

// Close the named bucket, optionally purging all its files.
func (b *Buckets) Close(name string, purgeFiles bool) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	bucket, ok := b.buckets[name]
	if !ok {
		return fmt.Errorf("not a bucket: %v", name)
	}
	if bucket != nil {
		bucket.Close()
	}
	delete(b.buckets, name)
	if purgeFiles {
		// Permanent destroy.
		bp, err := b.Path(name)
		if err == nil {
			os.RemoveAll(bp)
		}
	}
	return nil
}

func (b *Buckets) CloseAll() {
	if b == nil {
		return
	}
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, bucket := range b.buckets {
		bucket.Close()
	}
}

func (b *Buckets) Path(name string) (string, error) {
	bp, err := BucketPath(name)
	if err != nil {
		return "", err
	}
	return filepath.Join(b.dir, bp), nil
}

func BucketPath(bucketName string) (string, error) {
	match, err := regexp.MatchString("^[A-Za-z0-9\\-_]+$", bucketName)
	if err != nil {
		return "", err
	}
	if !match {
		return "", fmt.Errorf("bad bucket name: %v", bucketName)
	}
	c := uint16(crc32.ChecksumIEEE([]byte(bucketName)))
	lo := fmt.Sprintf("%02x", c&0xff)
	hi := fmt.Sprintf("%02x", c>>8)
	// Example result for "default" bucket: "$BUCKETS_DIR/00/df/default-bucket".
	return filepath.Join(hi, lo, bucketName+BUCKET_DIR_SUFFIX), nil
}

func (b *Buckets) loadBucket_unlocked(name string, create bool) (Bucket, error) {
	log.Printf("loading bucket: %v", name)
	if b.buckets[name] != nil {
		return nil, fmt.Errorf("bucket already registered: %v", name)
	}
	bucket, err := b.alloc_unlocked(name, create, b.settings)
	if err != nil {
		log.Printf("Alloc error on %v: %v", name, err)
		return nil, err
	}
	err = bucket.Load()
	if err != nil {
		return nil, err
	}
	b.register_unlocked(name, bucket)
	return bucket, nil
}

func (b *Buckets) makeQuiescer(name string) func(time.Time) bool {
	return func(t time.Time) bool {
		nrv := b.maybeQuiesce(name)
		return !nrv
	}
}

// Returns true if the bucket is closed.
func (b *Buckets) maybeQuiesce(name string) bool {
	bucket := b.Get(name)
	if bucket == nil {
		return true
	}

	lb, ok := bucket.(*livebucket)
	if !ok {
		b.Close(name, false)
		return true
	}

	val := atomic.LoadInt64(&lb.activity)
	if val > 0 {
		atomic.AddInt64(&lb.activity, -val)
		return false
	}

	log.Printf("quiescing bucket: %v", name)
	b.Close(name, false)
	return true
}
