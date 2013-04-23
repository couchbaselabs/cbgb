package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
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
	STORE_FILE_SUFFIX   = "store"
	STORES_PER_BUCKET   = 1 // The # of *.store files per bucket (ignoring compaction).
	VBID_DDOC           = uint16(0xffff)
)

var broadcastMux = broadcast.NewMuxObserver(0, 0)

var statAggPeriodic *periodically
var statAggPassPeriodic *periodically

var bucketUnavailable = errors.New("Bucket unavailable")

type Bucket interface {
	Name() string
	Available() bool
	Compact() error
	Close() error
	Flush() error
	Load() error

	Subscribe(ch chan<- interface{})
	Unsubscribe(ch chan<- interface{})

	GetBucketDir() string
	GetBucketSettings() *BucketSettings

	CreateVBucket(vbid uint16) (*VBucket, error)
	DestroyVBucket(vbid uint16) (destroyed bool)
	GetVBucket(vbid uint16) (*VBucket, error)
	SetVBState(vbid uint16, newState VBState) error

	GetBucketStore(int) *bucketstore

	Auth([]byte) bool

	Statish

	GetDDocVBucket() *VBucket
	GetDDoc(ddocId string) ([]byte, error)
	SetDDoc(ddocId string, body []byte) error
	DelDDoc(ddocId string) error
	VisitDDocs(start []byte, visitor func(key []byte, data []byte) bool) error
	GetDDocs() *DDocs
	SetDDocs(old, val *DDocs) bool

	GetItemBytes() int64

	PushErr(err error)
	Errs() []error

	PushLog(msg string)
	Logs() []string
}

type livebucket struct {
	availablech  chan bool
	name         string
	dir          string
	settings     *BucketSettings
	vbuckets     [MAX_VBUCKETS]unsafe.Pointer // *vbucket
	vbucketDDoc  *VBucket
	bucketstores map[int]*bucketstore
	observer     broadcast.Broadcaster

	bucketItemBytes int64
	activity        int64 // To track quiescence opportunities.

	ddocs unsafe.Pointer // *DDocs, holding the json.Unmarshal'ed design docs.

	lock  sync.Mutex // Lock covers the fields below.
	logs  *Ring
	errs  *Ring
	stats BucketStatsSnapshot
}

func NewBucket(name, dirForBucket string, settings *BucketSettings) (
	b Bucket, err error) {
	fileNames, err := bucketFileNames(dirForBucket, settings)
	if err != nil {
		return nil, err
	}

	if settings.MemoryOnly < MemoryOnly_LEVEL_PERSIST_NOTHING {
		if err = settings.save(dirForBucket); err != nil {
			return nil, err
		}
	}

	aggStats := NewAggStats(func() Aggregatable {
		return &BucketStats{Time: int64(time.Now().Unix())}
	})
	aggBucketStoreStats := NewAggStats(func() Aggregatable {
		return &BucketStoreStats{Time: int64(time.Now().Unix())}
	})

	res := &livebucket{
		availablech:  make(chan bool),
		name:         name,
		dir:          dirForBucket,
		settings:     settings,
		bucketstores: make(map[int]*bucketstore),
		observer:     broadcastMux.Sub(),
		logs:         NewRing(10),
		errs:         NewRing(10),
		stats: BucketStatsSnapshot{
			CurBucket:      &BucketStats{},
			CurBucketStore: &BucketStoreStats{},
			AggBucket:      aggStats,
			AggBucketStore: aggBucketStoreStats,
		},
	}

	for i, fileName := range fileNames {
		p := filepath.Join(dirForBucket, fileName)
		bs, err := newBucketStore(p, *settings, nil)
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

func bucketFileNames(dirForBucket string, settings *BucketSettings) (
	fileNames []string, err error) {
	if settings.MemoryOnly < MemoryOnly_LEVEL_PERSIST_NOTHING {
		fileNames, err = latestStoreFileNames(dirForBucket,
			STORES_PER_BUCKET, STORE_FILE_SUFFIX)
		if err != nil {
			return nil, err
		}
	} else {
		fileNames = make([]string, STORES_PER_BUCKET)
		for i := 0; i < STORES_PER_BUCKET; i++ {
			fileNames[i] = makeStoreFileName(strconv.FormatInt(int64(i), 10),
				0, STORE_FILE_SUFFIX)
		}
	}
	return fileNames, nil
}

func (b *livebucket) Name() string {
	return b.name
}

func (b *livebucket) GetBucketDir() string {
	return b.dir
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
			vb := (*VBucket)(vbp)
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

func (b *livebucket) GetVBucket(vbid uint16) (*VBucket, error) {
	atomic.AddInt64(&b.activity, 1)
	// TODO: Revisit the available approach, as it feels racy.
	if b == nil || !b.Available() {
		return nil, bucketUnavailable
	}
	vbp := atomic.LoadPointer(&b.vbuckets[vbid])
	return (*VBucket)(vbp), nil
}

func (b *livebucket) casVBucket(vbid uint16, vb *VBucket, vbPrev *VBucket) bool {
	return atomic.CompareAndSwapPointer(&b.vbuckets[vbid],
		unsafe.Pointer(vbPrev), unsafe.Pointer(vb))
}

func (b *livebucket) CreateVBucket(vbid uint16) (*VBucket, error) {
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
	if vb, _ := b.GetVBucket(vbid); vb != nil {
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
	vb, _ := b.GetVBucket(vbid)
	if vb != nil {
		_, err := vb.SetVBState(newState, func(oldState VBState) {
			if vbx, _ := b.GetVBucket(vbid); vbx == vb {
				b.observer.Submit(vbucketChange{b, vbid, oldState, newState})
			}
		})
		return err
	}
	return errors.New("no vbucket during SetVBState()")
}

func (b *livebucket) Auth(passwordClearText []byte) bool {
	return b.settings.Auth(passwordClearText)
}

func (b *livebucket) SnapshotStats() StatsSnapshot {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.stats.requests++

	return b.stats.Copy()
}

func (b *livebucket) sampleStats(t time.Time) {
	b.lock.Lock()
	defer b.lock.Unlock()

	currBucketStats := AggregateBucketStats(b, "")
	diffBucketStats := &BucketStats{}
	diffBucketStats.Add(currBucketStats)
	diffBucketStats.Sub(b.stats.CurBucket)
	diffBucketStats.Time = t.Unix()
	b.stats.AggBucket.AddSample(diffBucketStats)
	b.stats.CurBucket = currBucketStats

	currBucketStoreStats := AggregateBucketStoreStats(b, "")
	diffBucketStoreStats := &BucketStoreStats{}
	diffBucketStoreStats.Add(currBucketStoreStats)
	diffBucketStoreStats.Sub(b.stats.CurBucketStore)
	diffBucketStoreStats.Time = t.Unix()
	b.stats.AggBucketStore.AddSample(diffBucketStoreStats)
	b.stats.CurBucketStore = currBucketStoreStats

	b.stats.LatestUpdate = t
}

func (b *livebucket) shouldContinueDoingStats(t time.Time) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

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
	b.lock.Lock()
	defer b.lock.Unlock()

	statAggPeriodic.Register(b.availablech, b.mkSampleStats())
	statAggPassPeriodic.Register(b.availablech, b.mkQuiesceStats())
}

func (b *livebucket) StopStats() {
	b.lock.Lock()
	defer b.lock.Unlock()

	statAggPeriodic.Unregister(b.availablech)
	statAggPassPeriodic.Unregister(b.availablech)
}

func (b *livebucket) StatAge() time.Duration {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.stats.requests++

	return time.Since(b.stats.LatestUpdate)
}

func (b *livebucket) GetItemBytes() int64 {
	return atomic.LoadInt64(&b.bucketItemBytes)
}

func (b *livebucket) PushErr(err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.errs.Push(err)
}

func (b *livebucket) Errs() []error {
	b.lock.Lock()
	defer b.lock.Unlock()

	res := make([]error, 0, len(b.errs.Items))
	b.errs.Visit(func(v interface{}) {
		if v != nil {
			res = append(res, v.(error))
		}
	})
	return res
}

func (b *livebucket) PushLog(msg string) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.logs.Push(msg)
}

func (b *livebucket) Logs() []string {
	b.lock.Lock()
	defer b.lock.Unlock()

	res := make([]string, 0, len(b.logs.Items))
	b.logs.Visit(func(v interface{}) {
		if v != nil && v != "" {
			res = append(res, v.(string))
		}
	})
	return res
}
