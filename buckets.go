package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"time"
)

var quiescePeriodic *periodically

var noBucket = errors.New("No bucket")

type bres struct {
	b   Bucket
	err error
}

type getBReq struct {
	name string
	res  chan Bucket
}

type newBReq struct {
	name     string
	settings *BucketSettings
	res      chan bres
}

type closeBReq struct {
	name  string
	purge bool
	res   chan error
}

type asyncOpenComplete struct {
	name   string
	bucket Bucket
}

// Holder of buckets.
type Buckets struct {
	buckets  map[string]Bucket
	dir      string // Directory where all buckets are stored.
	settings *BucketSettings
	// When a bucket is currently opening, it's listed here
	opening map[string][]chan<- Bucket

	gch    chan getBReq
	agch   chan asyncOpenComplete
	sch    chan newBReq
	cch    chan closeBReq
	listch chan chan []string
}

// Build a new holder of buckets.
func NewBuckets(bdir string, settings *BucketSettings) (*Buckets, error) {
	if err := os.MkdirAll(bdir, 0777); err != nil && !isDir(bdir) {
		return nil, fmt.Errorf("could not create/access bucket dir: %v", bdir)
	}
	buckets := &Buckets{
		buckets:  map[string]Bucket{},
		opening:  map[string][]chan<- Bucket{},
		dir:      bdir,
		settings: settings.Copy(),
		gch:      make(chan getBReq),
		agch:     make(chan asyncOpenComplete),
		sch:      make(chan newBReq),
		cch:      make(chan closeBReq),
		listch:   make(chan chan []string),
	}
	go buckets.service()
	return buckets, nil
}

// Allocates and registers a new, named bucket, or error if it exists.
func (b *Buckets) New(name string,
	defaultSettings *BucketSettings) (Bucket, error) {
	r := newBReq{name, defaultSettings, make(chan bres)}

	b.sch <- r
	rv := <-r.res

	return rv.b, rv.err
}

func (b *Buckets) create(name string,
	defaultSettings *BucketSettings) (rv Bucket, err error) {
	if b.buckets[name] != nil {
		return nil, fmt.Errorf("bucket already exists: %v", name)
	}
	rv, err = b.alloc(name, true, defaultSettings)
	if err != nil {
		return nil, err
	}
	b.register(name, rv)
	return rv, nil
}

func (b *Buckets) alloc(name string, create bool,
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

func (b *Buckets) register(name string, bucket Bucket) {
	var ch chan bool
	if lb, ok := bucket.(*livebucket); ok {
		ch = lb.availablech
	}
	quiescePeriodic.Register(ch, b.makeQuiescer(name))
	b.buckets[name] = bucket
}

func (b *Buckets) GetNames() []string {
	ch := make(chan []string)
	b.listch <- ch
	return <-ch
}

func (b *Buckets) listInternal() []string {
	res := make([]string, 0, len(b.buckets))
	for name := range b.buckets {
		res = append(res, name)
	}
	return res
}

func (b *Buckets) service() {
	for {
		select {
		case req, ok := <-b.gch:
			if !ok {
				return
			}
			// request to retrieve a bucket
			b.getInternal(req.name, req.res)
		case ao := <-b.agch:
			for _, ch := range b.opening[ao.name] {
				ch <- ao.bucket
			}
			delete(b.opening, ao.name)
			if ao.bucket != nil {
				b.buckets[ao.name] = ao.bucket
			}
		case req := <-b.sch:
			// request to create a bucket
			rv := bres{}
			rv.b, rv.err = b.create(req.name, req.settings)
			req.res <- rv
		case req := <-b.cch:
			// request to close a bucket
			req.res <- b.closeInternal(req.name, req.purge)
		case req := <-b.listch:
			// list buckets
			req <- b.listInternal()
		}
	}
}

// Get the named bucket (or nil if it doesn't exist).
func (b *Buckets) Get(name string) Bucket {
	r := getBReq{name, make(chan Bucket)}

	b.gch <- r

	return <-r.res
}

func (b *Buckets) asyncOpen(name string) {
	rv, err := b.loadBucket(name, false)
	if err != nil {
		log.Printf("Problem opening bucket %v: %v", name, err)
		b.agch <- asyncOpenComplete{name: name}
		return
	}
	b.agch <- asyncOpenComplete{name: name, bucket: rv}
}

func (b *Buckets) getInternal(name string, ch chan<- Bucket) {
	// If the bucket is already open, we're done here.
	rv := b.buckets[name]
	if rv != nil {
		ch <- rv
		return
	}

	// We only actually start opening a bucket if it's not already open
	if b.opening[name] == nil {
		go b.asyncOpen(name)
	}
	// Subscribe to notification of this bucket opening.
	b.opening[name] = append(b.opening[name], ch)
}

// Close the named bucket, optionally purging all its files.
func (b *Buckets) Close(name string, purgeFiles bool) error {
	r := closeBReq{name, purgeFiles, make(chan error)}
	b.cch <- r
	return <-r.res
}

func (b *Buckets) closeInternal(name string, purgeFiles bool) error {
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

	for _, bname := range b.GetNames() {
		b.Close(bname, false)
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

func (b *Buckets) loadBucket(name string, create bool) (Bucket, error) {
	log.Printf("loading bucket: %v", name)
	if b.buckets[name] != nil {
		return nil, fmt.Errorf("bucket already registered: %v", name)
	}
	bucket, err := b.alloc(name, create, b.settings)
	if err != nil {
		log.Printf("Alloc error on %v: %v", name, err)
		return nil, err
	}
	err = bucket.Load()
	if err != nil {
		return nil, err
	}
	b.register(name, bucket)
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
