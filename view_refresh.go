package main

import (
	"encoding/base64"
	"encoding/json"
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
	v.viewsLock.Lock()
	defer v.viewsLock.Unlock()

	d := atomic.LoadInt64(&v.staleness)

	ddocs := v.parent.GetDDocs()
	if ddocs != nil {
		viewsStore, err := v.getViewsStore()
		if err != nil {
			return 0, err
		}
		backIndexStore := viewsStore.getPartitionStore(v.vbid)
		if backIndexStore == nil {
			return 0, fmt.Errorf("missing back index store, vbid: %v", v.vbid)
		}
		_, backIndexChanges := backIndexStore.colls()
		backIndexLastChange, err := backIndexChanges.MaxItem(true)
		if err != nil {
			return 0, err
		}
		var backIndexLastChangeCasBytes []byte
		if backIndexLastChange != nil {
			backIndexLastChangeCasBytes = backIndexLastChange.Key
		}
		errVisit := v.ps.visitChanges(backIndexLastChangeCasBytes, true,
			func(i *item) bool {
				err = v.viewsRefreshItem(ddocs, backIndexStore, i)
				if err != nil {
					return false
				}
				return true
			})
		if errVisit != nil {
			return 0, errVisit
		}
		if err != nil {
			return 0, err
		}
	}

	return atomic.AddInt64(&v.staleness, -d), nil
}

func (v *VBucket) viewsRefreshItem(ddocs *DDocs,
	backIndexStore *partitionstore, i *item) error {
	oldBackIndexItem, err := backIndexStore.get(i.key)
	if err != nil {
		return err
	}
	// TODO: One day do view hashing so view indexes are shared.
	viewEmits := map[string]ViewRows{} // Keyed by "ddocId/viewId".
	for ddocId, ddoc := range *ddocs {
		for viewId, view := range ddoc.Views {
			emits, err := v.execViewMapFunction(ddocId, ddoc, viewId, view, i)
			if err != nil {
				return err
			}
			viewEmits[ddocId+"/"+viewId] = emits
		}
	}
	j, err := json.Marshal(viewEmits)
	if err != nil {
		return err
	}
	newBackIndexItem := &item{
		key:  i.key,
		cas:  i.cas,
		data: j,
	}
	// TODO: Track size of backIndex as set() returns deltaItemBytes.
	_, err = backIndexStore.set(newBackIndexItem, oldBackIndexItem)
	if err != nil {
		return err
	}
	return nil
}

func (v *VBucket) execViewMapFunction(ddocId string, ddoc *DDoc,
	viewId string, view *View, i *item) (ViewRows, error) {
	pvmf, err := view.GetViewMapFunction()
	if err != nil {
		return nil, err
	}
	docId := string(i.key)
	docType := "json"
	var doc interface{}
	err = json.Unmarshal(i.data, &doc)
	if err != nil {
		doc = base64.StdEncoding.EncodeToString(i.data)
		docType = "base64"
	}
	odoc, err := OttoFromGo(pvmf.otto, doc)
	if err != nil {
		return nil, err
	}
	meta := map[string]interface{}{
		"id":   docId,
		"type": docType,
	}
	ometa, err := OttoFromGo(pvmf.otto, meta)
	if err != nil {
		return nil, err
	}
	_, err = pvmf.mapf.Call(pvmf.mapf, odoc, ometa)
	if err != nil {
		return nil, err
	}
	emits, err := pvmf.restartEmits()
	if err != nil {
		return nil, err
	}
	for _, emit := range emits {
		emit.Id = docId
	}
	return emits, nil
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
