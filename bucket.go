package cbgb

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	MAX_VBUCKETS        = 1024
	BUCKET_DIR_SUFFIX   = "-bucket" // Suffix allows non-buckets to be ignored.
	DEFAULT_BUCKET_NAME = "default"
	STORES_PER_BUCKET   = 4 // The # of *.store files per bucket (ignoring compaction).
)

type Bucket interface {
	Available() bool
	Compact() error
	Close() error
	Flush() error
	Load() error

	Subscribe(ch chan<- interface{})
	Unsubscribe(ch chan<- interface{})

	CreateVBucket(vbid uint16) (*vbucket, error)
	DestroyVBucket(vbid uint16) (destroyed bool)
	GetVBucket(vbid uint16) *vbucket
	SetVBState(vbid uint16, newState VBState) error

	GetBucketStore(int) *bucketstore

	SampleStats()
	GetLastStats() *Stats
	GetLastBucketStoreStats() *BucketStoreStats
	GetAggStats() *AggStats
	GetAggBucketStoreStats() *AggStats
}

// Holder of buckets.
type Buckets struct {
	buckets  map[string]Bucket
	dir      string // Directory where all buckets are stored.
	lock     sync.Mutex
	settings *BucketSettings
	statsch  chan *funreq
}

type BucketSettings struct {
	FlushInterval   time.Duration `json:"flushInterval"`
	SleepInterval   time.Duration `json:"sleepInterval"`
	CompactInterval time.Duration `json:"compactInterval"`
	PurgeTimeout    time.Duration `json:"purgeTimeout"`
}

func (bs *BucketSettings) Copy() *BucketSettings {
	return &BucketSettings{
		FlushInterval:   bs.FlushInterval,
		SleepInterval:   bs.SleepInterval,
		CompactInterval: bs.CompactInterval,
		PurgeTimeout:    bs.PurgeTimeout,
	}
}

// Build a new holder of buckets.
func NewBuckets(dirForBuckets string, settings *BucketSettings) (*Buckets, error) {
	if !isDir(dirForBuckets) {
		return nil, errors.New(fmt.Sprintf("not a directory: %v", dirForBuckets))
	}
	buckets := &Buckets{
		buckets:  map[string]Bucket{},
		dir:      dirForBuckets,
		settings: settings.Copy(),
		statsch:  make(chan *funreq),
	}
	go buckets.serviceStats()
	return buckets, nil
}

func (b *Buckets) serviceStats() {
	tickerS := time.NewTicker(time.Second)
	defer tickerS.Stop()

	for {
		select {
		case r, ok := <-b.statsch:
			if !ok {
				return
			}
			r.fun()
			close(r.res)
		case <-tickerS.C:
			b.sampleStats()
		}
	}
}

func (b *Buckets) sampleStats() {
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, bucket := range b.buckets {
		bucket.SampleStats()
	}
}

func (b *Buckets) StatsApply(fun func()) {
	req := &funreq{fun: fun, res: make(chan bool)}
	b.statsch <- req
	<-req.res
}

// Create a new named bucket.
// Return the new bucket, or nil if the bucket already exists.
//
// TODO: Need clearer names around New vs Create vs Open vs Destroy,
// especially now that there's persistence.
func (b *Buckets) New(name string) (rv Bucket, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.buckets[name] != nil {
		return nil, errors.New(fmt.Sprintf("bucket already exists: %v", name))
	}

	// TODO: Need name checking & encoding for safety/security.
	bdir := b.Path(name) // If an accessible bdir directory exists already, it's ok.
	if err = os.Mkdir(bdir, 0777); err != nil && !isDir(bdir) {
		return nil, errors.New(fmt.Sprintf("could not access bucket dir: %v", bdir))
	}

	if rv, err = NewBucket(bdir, b.settings); err != nil {
		return nil, err
	}

	b.buckets[name] = rv
	return rv, nil
}

func (b *Buckets) GetNames() []string {
	b.lock.Lock()
	defer b.lock.Unlock()

	res := make([]string, 0, len(b.buckets))
	for name, _ := range b.buckets {
		res = append(res, name)
	}
	return res
}

// Get the named bucket (or nil if it doesn't exist).
func (b *Buckets) Get(name string) Bucket {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.buckets[name]
}

// Close the named bucket, optionally purging all its files.
func (b *Buckets) Close(name string, purgeFiles bool) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if bucket := b.buckets[name]; bucket != nil {
		bucket.Close()
		delete(b.buckets, name)
	}

	if purgeFiles {
		os.RemoveAll(b.Path(name))
	}
}

func (b *Buckets) Path(name string) string {
	return path.Join(b.dir, name+BUCKET_DIR_SUFFIX)
}

// Reads the buckets directory and returns list of bucket names.
func (b *Buckets) LoadNames() ([]string, error) {
	list, err := ioutil.ReadDir(b.dir)
	if err != nil {
		return nil, err
	}
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

// Loads all buckets from the buckets directory.
func (b *Buckets) Load() error {
	bucketNames, err := b.LoadNames()
	if err != nil {
		return err
	}
	for _, bucketName := range bucketNames {
		b, err := b.New(bucketName)
		if err != nil {
			return err
		}
		if b == nil {
			return errors.New(fmt.Sprintf("loading bucket %v, but it exists already",
				bucketName))
		}
		if err = b.Load(); err != nil {
			return err
		}
	}
	return nil
}

type livebucket struct {
	availablech  chan bool
	dir          string
	vbuckets     [MAX_VBUCKETS]unsafe.Pointer
	bucketstores map[int]*bucketstore
	observer     *broadcaster

	lastStats            *Stats
	lastBucketStoreStats *BucketStoreStats

	aggStats            *AggStats
	aggBucketStoreStats *AggStats
}

func NewBucket(dirForBucket string, settings *BucketSettings) (Bucket, error) {
	fileNames, err := latestStoreFileNames(dirForBucket, STORES_PER_BUCKET)
	if err != nil {
		return nil, err
	}

	aggStats := NewAggStats(func() Aggregatable {
		return &Stats{}
	})
	aggBucketStoreStats := NewAggStats(func() Aggregatable {
		return &BucketStoreStats{}
	})

	res := &livebucket{
		availablech:          make(chan bool),
		dir:                  dirForBucket,
		bucketstores:         make(map[int]*bucketstore),
		observer:             newBroadcaster(0),
		lastStats:            &Stats{},
		lastBucketStoreStats: &BucketStoreStats{},
		aggStats:             aggStats,
		aggBucketStoreStats:  aggBucketStoreStats,
	}

	for i, fileName := range fileNames {
		p := path.Join(dirForBucket, fileName)
		bs, err := newBucketStore(p,
			settings.FlushInterval,
			settings.SleepInterval,
			settings.CompactInterval,
			settings.PurgeTimeout)
		if err != nil {
			res.Close()
			return nil, err
		}
		res.bucketstores[i] = bs
	}

	return res, nil
}

// Subscribe to bucket events.
//
// Note that this is retroactive -- it will send existing states.
func (b *livebucket) Subscribe(ch chan<- interface{}) {
	b.observer.Register(ch)
	go func() {
		for i := uint16(0); i < MAX_VBUCKETS; i++ {
			c := vbucketChange{bucket: b,
				vbid:     i,
				oldState: VBDead,
				newState: VBDead}
			if vb := c.getVBucket(); vb != nil {
				if s := vb.GetVBState(); s != VBDead {
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

func (b *livebucket) Available() bool {
	select {
	default:
	case <-b.availablech:
		return false
	}
	return true
}

func (b *livebucket) Close() error {
	close(b.availablech)
	for vbid, _ := range b.vbuckets {
		if vbp := atomic.LoadPointer(&b.vbuckets[vbid]); vbp != nil {
			vb := (*vbucket)(vbp)
			vb.Close()
		}
	}
	for _, bs := range b.bucketstores {
		bs.Close()
	}
	return nil
}

func (b *livebucket) GetBucketStore(idx int) *bucketstore {
	return b.bucketstores[idx]
}

func (b *livebucket) Flush() error {
	for _, bs := range b.bucketstores {
		err := bs.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *livebucket) Compact() error {
	for _, bs := range b.bucketstores {
		err := bs.Compact()
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO: Need to track some bucket "uuid", so that a recreated bucket X'
// is distinct from a previously deleted bucket X?
func (b *livebucket) Load() (err error) {
	for _, bs := range b.bucketstores {
		for _, collName := range bs.collNames() {
			if !strings.HasSuffix(collName, COLL_SUFFIX_CHANGES) {
				continue
			}
			vbidStr := collName[0 : len(collName)-len(COLL_SUFFIX_CHANGES)]
			if !bs.collExists(vbidStr+COLL_SUFFIX_CHANGES) ||
				!bs.collExists(vbidStr+COLL_SUFFIX_KEYS) {
				continue
			}
			vbid, err := strconv.Atoi(vbidStr)
			if err != nil {
				return err
			}
			vb, err := newVBucket(b, uint16(vbid), bs)
			if err != nil {
				return err
			}
			if err = vb.load(); err != nil {
				return err
			}
			if !b.casVBucket(uint16(vbid), vb, nil) {
				return errors.New(fmt.Sprintf("loading vbucket: %v,"+
					" but it already exists", vbid))
			}
			// TODO: Need to poke observers with changed vbstate?
		}
	}
	return nil
}

func (b *livebucket) GetVBucket(vbid uint16) *vbucket {
	// TODO: Revisit the available approach, as it feels racy.
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

func (b *livebucket) CreateVBucket(vbid uint16) (*vbucket, error) {
	if b == nil || !b.Available() {
		return nil, errors.New("cannot create vbucket as bucket is unavailable")
	}
	bs := b.bucketstores[int(vbid)%STORES_PER_BUCKET]
	if bs == nil {
		return nil, errors.New("cannot create vbucket as bucketstore missing")
	}
	vb, err := newVBucket(b, vbid, bs)
	if err != nil {
		return nil, err
	}
	if b.casVBucket(vbid, vb, nil) {
		return vb, nil
	}
	return nil, errors.New("vbucket already exists")
}

func (b *livebucket) DestroyVBucket(vbid uint16) (destroyed bool) {
	destroyed = false
	if vb := b.GetVBucket(vbid); vb != nil {
		vb.SetVBState(VBDead, func(oldState VBState) {
			if b.casVBucket(vbid, nil, vb) {
				b.observer.Submit(vbucketChange{b, vbid, oldState, VBDead})
				destroyed = true
			}
		})
	}
	return
}

func (b *livebucket) SetVBState(vbid uint16, newState VBState) error {
	vb := b.GetVBucket(vbid)
	if vb != nil {
		_, err := vb.SetVBState(newState, func(oldState VBState) {
			if b.GetVBucket(vbid) == vb {
				b.observer.Submit(vbucketChange{b, vbid, oldState, newState})
			}
		})
		return err
	}
	return errors.New("no vbucket during SetVBState()")
}

// Call only during StatsApply/serviceStats().
func (b *livebucket) GetAggStats() *AggStats {
	return b.aggStats
}

// Call only during StatsApply/serviceStats().
func (b *livebucket) GetAggBucketStoreStats() *AggStats {
	return b.aggBucketStoreStats
}

// Call only during StatsApply/serviceStats().
func (b *livebucket) GetLastStats() *Stats {
	return b.lastStats
}

// Call only during StatsApply/serviceStats().
func (b *livebucket) GetLastBucketStoreStats() *BucketStoreStats {
	return b.lastBucketStoreStats
}

// Call only during StatsApply/serviceStats().
func (b *livebucket) SampleStats() {
	currStats := AggregateStats(b, "")
	diffStats := &Stats{}
	diffStats.Add(currStats)
	diffStats.Sub(b.lastStats)
	b.aggStats.addSample(diffStats)
	b.lastStats = currStats

	currBucketStoreStats := AggregateBucketStoreStats(b, "")
	diffBucketStoreStats := &BucketStoreStats{}
	diffBucketStoreStats.Add(currBucketStoreStats)
	diffBucketStoreStats.Sub(b.lastBucketStoreStats)
	b.aggBucketStoreStats.addSample(diffBucketStoreStats)
	b.lastBucketStoreStats = currBucketStoreStats
}

type vbucketChange struct {
	bucket             Bucket
	vbid               uint16
	oldState, newState VBState
}

func (c vbucketChange) getVBucket() *vbucket {
	if c.bucket == nil {
		return nil
	}
	return c.bucket.GetVBucket(c.vbid)
}

func (c vbucketChange) String() string {
	return fmt.Sprintf("vbucket %v %v -> %v", c.vbid, c.oldState, c.newState)
}
