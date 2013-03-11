package cbgb

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dustin/go-broadcast"
	"github.com/steveyen/gkvlite"
)

const (
	MAX_VBUCKETS        = 1024
	BUCKET_DIR_SUFFIX   = "-bucket" // Suffix allows non-buckets to be ignored.
	DEFAULT_BUCKET_NAME = "default"
	STORES_PER_BUCKET   = 1 // The # of *.store files per bucket (ignoring compaction).
	VBID_DDOC           = uint16(0xffff)
)

var broadcastMux = broadcast.NewMuxObserver(0, 0)

var statAggPeriodic = newPeriodically(time.Second, 10)
var statAggPassivator = newPeriodically(time.Minute*5, 10)

type Bucket interface {
	Available() bool
	Compact() error
	Close() error
	Flush() error
	Load() error

	Subscribe(ch chan<- interface{})
	Unsubscribe(ch chan<- interface{})

	GetBucketSettings() *BucketSettings

	CreateVBucket(vbid uint16) (*vbucket, error)
	DestroyVBucket(vbid uint16) (destroyed bool)
	GetVBucket(vbid uint16) *vbucket
	SetVBState(vbid uint16, newState VBState) error

	GetBucketStore(int) *bucketstore

	Auth([]byte) bool

	Statish

	GetDDocVBucket() *vbucket
	GetDDoc(ddocId string) ([]byte, error)
	SetDDoc(ddocId string, body []byte) error

	GetItemBytes() int64
}

// Holder of buckets.
type Buckets struct {
	buckets  map[string]Bucket
	dir      string // Directory where all buckets are stored.
	lock     sync.Mutex
	settings *BucketSettings
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
	}
	return buckets, nil
}

// Create a new named bucket.
// Return the new bucket, or nil if the bucket already exists.
//
// TODO: Need clearer names around New vs Create vs Open vs Destroy,
// especially now that there's persistence.
func (b *Buckets) New(name string,
	defaultSettings *BucketSettings) (rv Bucket, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.buckets[name] != nil {
		return nil, errors.New(fmt.Sprintf("bucket already exists: %v", name))
	}

	settings := &BucketSettings{}
	if defaultSettings != nil {
		settings = defaultSettings.Copy()
	}

	settings.UUID = createNewUUID()

	// TODO: Need name checking & encoding for safety/security.
	bdir := b.Path(name) // If an accessible bdir directory exists already, it's ok.

	if settings.MemoryOnly < MemoryOnly_LEVEL_PERSIST_NOTHING {
		if err = os.MkdirAll(bdir, 0777); err != nil && !isDir(bdir) {
			return nil, errors.New(fmt.Sprintf("could not access bucket dir: %v", bdir))
		}
	}

	_, err = settings.load(bdir)
	if err != nil {
		return nil, err
	}

	if rv, err = NewBucket(bdir, settings); err != nil {
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

func (b *Buckets) Path(name string) string {
	return BucketPath(b.dir, name)
}

func BucketPath(bucketsDir string, bucketName string) string {
	c := uint16(crc32.ChecksumIEEE([]byte(bucketName)))
	lo := fmt.Sprintf("%02x", c&0xff)
	hi := fmt.Sprintf("%02x", c>>8)
	// Example result for "default" bucket: "$BUCKETS_DIR/00/df/default-bucket".
	return path.Join(bucketsDir, hi, lo, bucketName+BUCKET_DIR_SUFFIX)
}

// Reads the buckets directory and returns list of bucket names.
func (b *Buckets) LoadNames() ([]string, error) {
	res := []string{}
	listHi, err := ioutil.ReadDir(b.dir)
	if err != nil {
		return nil, err
	}
	for _, entryHi := range listHi {
		if !entryHi.IsDir() {
			continue
		}
		pathHi := path.Join(b.dir, entryHi.Name())
		listLo, err := ioutil.ReadDir(pathHi)
		if err != nil {
			return nil, err
		}
		for _, entryLo := range listLo {
			if !entryLo.IsDir() {
				continue
			}
			pathLo := path.Join(pathHi, entryLo.Name())
			list, err := ioutil.ReadDir(pathLo)
			if err != nil {
				return nil, err
			}
			for _, entry := range list {
				if !entry.IsDir() ||
					!strings.HasSuffix(entry.Name(), BUCKET_DIR_SUFFIX) {
					continue
				}
				res = append(res,
					entry.Name()[0:len(entry.Name())-len(BUCKET_DIR_SUFFIX)])
			}
		}
	}
	return res, nil
}

// Loads all buckets from the buckets directory tree.  If
// errorIfBucketAlreadyExists is false any existing (already loaded)
// buckets are left unchanged.
func (b *Buckets) Load(ignoreIfBucketAlreadyExists bool) error {
	bucketNames, err := b.LoadNames()
	if err != nil {
		return err
	}
	for _, bucketName := range bucketNames {
		log.Printf("loading bucket: %v", bucketName)
		if b.Get(bucketName) != nil {
			if !ignoreIfBucketAlreadyExists {
				return errors.New(fmt.Sprintf("loading bucket %v, but it exists already",
					bucketName))
			}
			log.Printf("loading bucket: %v, already loaded", bucketName)
			continue
		}
		b, err := b.New(bucketName, b.settings)
		if err != nil {
			return err
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
	settings     *BucketSettings
	vbuckets     [MAX_VBUCKETS]unsafe.Pointer // *vbucket
	vbucketDDoc  *vbucket
	bucketstores map[int]*bucketstore
	observer     broadcast.Broadcaster

	bucketItemBytes int64

	stats    BucketStatsSnapshot
	statLock sync.Mutex
}

func NewBucket(dirForBucket string, settings *BucketSettings) (b Bucket, err error) {
	var fileNames []string

	if settings.MemoryOnly < MemoryOnly_LEVEL_PERSIST_NOTHING {
		fileNames, err = latestStoreFileNames(dirForBucket, STORES_PER_BUCKET)
		if err != nil {
			return nil, err
		}
	} else {
		fileNames = make([]string, STORES_PER_BUCKET)
		for i := 0; i < STORES_PER_BUCKET; i++ {
			fileNames[i] = makeStoreFileName(i, 0)
		}
	}

	if settings.MemoryOnly < MemoryOnly_LEVEL_PERSIST_NOTHING {
		if err = settings.save(dirForBucket); err != nil {
			return nil, err
		}
	}

	aggStats := NewAggStats(func() Aggregatable {
		return &Stats{Time: int64(time.Now().Unix())}
	})
	aggBucketStoreStats := NewAggStats(func() Aggregatable {
		return &BucketStoreStats{Time: int64(time.Now().Unix())}
	})

	res := &livebucket{
		availablech:  make(chan bool),
		dir:          dirForBucket,
		settings:     settings,
		bucketstores: make(map[int]*bucketstore),
		observer:     broadcastMux.Sub(),
		stats: BucketStatsSnapshot{
			Current:        &Stats{},
			BucketStore:    &BucketStoreStats{},
			Agg:            aggStats,
			AggBucketStore: aggBucketStoreStats,
		},
	}

	for i, fileName := range fileNames {
		p := path.Join(dirForBucket, fileName)
		bs, err := newBucketStore(p, *settings)
		if err != nil {
			res.Close()
			return nil, err
		}
		res.bucketstores[i] = bs
	}

	vbucketDDoc, err := newVBucket(res, VBID_DDOC, res.bucketstores[0],
		&res.bucketItemBytes)
	if err != nil {
		res.Close()
		return nil, err
	}
	_, err = vbucketDDoc.SetVBState(VBActive, nil)
	if err != nil {
		res.Close()
		return nil, err
	}
	res.vbucketDDoc = vbucketDDoc

	return res, nil
}

func (b *livebucket) GetBucketSettings() *BucketSettings {
	return b.settings
}

// Subscribe to bucket events.
//
// Note that this is retroactive -- it will send existing states.
func (b *livebucket) Subscribe(ch chan<- interface{}) {
	b.observer.Register(ch)
	go func() {
		for i := uint16(0); i < uint16(b.settings.NumPartitions); i++ {
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
	if !b.Available() {
		return nil
	}
	close(b.availablech)
	for vbid, _ := range b.vbuckets {
		if vbp := atomic.LoadPointer(&b.vbuckets[vbid]); vbp != nil {
			vb := (*vbucket)(vbp)
			vb.Close()
		}
	}
	b.vbucketDDoc.Close()
	for _, bs := range b.bucketstores {
		bs.Close()
	}
	b.observer.Close()
	return nil
}

func (b *livebucket) GetBucketStore(idx int) *bucketstore {
	return b.bucketstores[idx]
}

func (b *livebucket) Flush() error {
	for _, bs := range b.bucketstores {
		_, err := bs.Flush()
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

func (b *livebucket) Load() (err error) {
	b.bucketItemBytes = 0
	for _, bs := range b.bucketstores {
		// TODO: Need to poke observers with changed vbstate?
		var errVisit error
		err = bs.collMeta(COLL_VBMETA).VisitItemsAscend(nil, true,
			func(i *gkvlite.Item) bool {
				vbidStr := i.Key
				vbid, errVisit := strconv.Atoi(string(vbidStr))
				if errVisit != nil {
					return false
				}
				if vbid > 0x0000ffff {
					errVisit = fmt.Errorf("load failed with vbid too big: %v", vbid)
					return false
				}
				vb, errVisit := newVBucket(b, uint16(vbid), bs,
					&b.bucketItemBytes)
				if errVisit != nil {
					return false
				}
				if errVisit = vb.load(); errVisit != nil {
					return false
				}
				if vbid < b.settings.NumPartitions {
					if !b.casVBucket(uint16(vbid), vb, nil) {
						errVisit = fmt.Errorf("loading vbucket: %v, but it already exists",
							vbid)
						return false
					}
				} else if uint16(vbid) == VBID_DDOC {
					if b.vbucketDDoc != nil {
						b.vbucketDDoc.Close()
					}
					b.vbucketDDoc = vb
				} else {
					errVisit = fmt.Errorf("vbid out of range during load: %v versus %v",
						vbid, b.settings.NumPartitions)
					return false
				}
				return true
			})
		if err != nil {
			return err
		}
		if errVisit != nil {
			return errVisit
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
	vb, err := newVBucket(b, vbid, bs, &b.bucketItemBytes)
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

func (b *livebucket) Auth(passwordClearText []byte) bool {
	if b.settings == nil {
		return false
	}
	// TODO: Have real password hash functions and salt.
	if b.settings.PasswordHashFunc == "" &&
		b.settings.PasswordSalt == "" &&
		bytes.Equal([]byte(b.settings.PasswordHash), passwordClearText) {
		return true
	}
	return false
}

func (b *livebucket) SnapshotStats() StatsSnapshot {
	b.statLock.Lock()
	defer b.statLock.Unlock()

	b.stats.requests++

	return b.stats.Copy()
}

func (b *livebucket) sampleStats(t time.Time) {
	b.statLock.Lock()
	defer b.statLock.Unlock()

	currStats := AggregateStats(b, "")
	diffStats := &Stats{}
	diffStats.Add(currStats)
	diffStats.Sub(b.stats.Current)
	diffStats.Time = t.Unix()
	b.stats.Agg.AddSample(diffStats)
	b.stats.Current = currStats

	currBucketStoreStats := AggregateBucketStoreStats(b, "")
	diffBucketStoreStats := &BucketStoreStats{}
	diffBucketStoreStats.Add(currBucketStoreStats)
	diffBucketStoreStats.Sub(b.stats.BucketStore)
	diffBucketStoreStats.Time = t.Unix()
	b.stats.AggBucketStore.AddSample(diffBucketStoreStats)
	b.stats.BucketStore = currBucketStoreStats
	b.stats.LatestUpdate = t
}

func (b *livebucket) shouldContinueDoingStats(t time.Time) bool {
	b.statLock.Lock()
	defer b.statLock.Unlock()

	o := b.stats.requests
	b.stats.requests = 0

	return o > 0
}

func (b *livebucket) mkSampleStats() func(time.Time) bool {
	return func(t time.Time) bool {
		b.sampleStats(t)
		return true
	}
}

func (b *livebucket) mkQuiesceStats() func(time.Time) bool {
	return func(t time.Time) bool {
		keepGoing := b.shouldContinueDoingStats(t)
		if !keepGoing {
			statAggPeriodic.Unregister(b.availablech)
		}
		return keepGoing
	}
}

// Start the stats at the given interval.
func (b *livebucket) StartStats(d time.Duration) {
	b.statLock.Lock()
	defer b.statLock.Unlock()

	statAggPeriodic.Register(b.availablech, b.mkSampleStats())
	statAggPassivator.Register(b.availablech, b.mkQuiesceStats())
}

func (b *livebucket) StopStats() {
	b.statLock.Lock()
	defer b.statLock.Unlock()

	statAggPeriodic.Unregister(b.availablech)
	statAggPassivator.Unregister(b.availablech)
}

func (b *livebucket) StatAge() time.Duration {
	b.statLock.Lock()
	defer b.statLock.Unlock()

	b.stats.requests++

	return time.Since(b.stats.LatestUpdate)
}

func (b *livebucket) GetItemBytes() int64 {
	return atomic.LoadInt64(&b.bucketItemBytes)
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
