package cbgb

import (
	"sync/atomic"
	"time"
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
	return 0, nil
}
