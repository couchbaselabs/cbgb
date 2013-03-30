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
var viewsRefresher = newPeriodically(10*time.Second, 5)

func (v *VBucket) markStale() {
	newval := atomic.AddInt64(&v.staleness, 1)
	if newval == 1 {
		viewsRefresher.Register(v.available, v.mkViewsRefreshFun())
	}
}

func (v *VBucket) mkViewsRefreshFun() func(time.Time) bool {
	return func(t time.Time) bool {
		return v.periodicViewsRefresh(t)
	}
}

func (v *VBucket) periodicViewsRefresh(time.Time) bool {
	leftovers, _ := v.viewsRefresh()
	return leftovers > 0
}

func (v *VBucket) viewsRefresh() (int64, error) {
	d := atomic.LoadInt64(&v.staleness)
	ddocs := v.parent.GetDDocs()
	if ddocs != nil {
		vs, err := v.getViewsStore()
		if err != nil {
			return 0, err
		}
		for ddocId, ddoc := range *ddocs {
			for viewId, view := range ddoc.Views {
				if view.Map == "" {
					continue
				}
				// TODO: Switch to a hash of the map function for the
				// viewCollName in order to share and reuse any view
				// indexes.
				viewCollName := ddocId + "/" + viewId
				// TODO: Use JSON collator instead of bytes.Compare.
				vs.coll(viewCollName)
			}
		}
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
