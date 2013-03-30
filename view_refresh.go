package cbgb

import (
	"fmt"
	"path"
	"sync/atomic"
	"time"
)

const (
	VIEWS_FILE_SUFFIX = "views"
)

// TODO: Make this configurable.
var viewRefresher = newPeriodically(10*time.Second, 5)

func (v *VBucket) markStale() {
	newval := atomic.AddInt64(&v.staleness, 1)
	if newval == 1 {
		viewRefresher.Register(v.available, v.mkViewRefreshFun())
	}
}

func (v *VBucket) mkViewRefreshFun() func(time.Time) bool {
	return func(t time.Time) bool {
		return v.periodicViewRefresh(t)
	}
}

func (v *VBucket) periodicViewRefresh(time.Time) bool {
	leftovers, _ := v.viewRefresh()
	return leftovers > 0
}

func (v *VBucket) viewRefresh() (int64, error) {
	d := atomic.LoadInt64(&v.staleness)
	_, err := v.getViewsStore()
	if err != nil {
		return 0, err
	}
	return atomic.AddInt64(&v.staleness, -d), nil
}

func (v *VBucket) getViewsStore() (res *bucketstore, err error) {
	v.Apply(func() {
		if v.viewsStore == nil {
			// TODO: Handle views file versioning / compaction.
			// TODO: Handle views file memory-only mode.
			// TODO: Handle views file load.
			ver := 0
			settings := v.parent.GetBucketSettings()
			fileName := fmt.Sprintf("%s_%d-%d.%s",
				settings.UUID, v.vbid, ver, VIEWS_FILE_SUFFIX)
			p := path.Join(v.parent.GetBucketDir(), fileName)
			v.viewsStore, err = newBucketStore(p, *settings)
		}
		res = v.viewsStore
	})
	return res, err
}
