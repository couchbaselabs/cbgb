//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"net/http"
	"sort"

	"github.com/couchbaselabs/walrus"
	"github.com/dustin/gomemcached"
	"github.com/robertkrimen/otto"
	"github.com/steveyen/gkvlite"
)

const maxViewErrors = 100

func couchDbGetView(w http.ResponseWriter, r *http.Request) {
	p, err := ParseViewParams(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("view param parsing err: %v", err), 400)
		return
	}

	vars, _, bucket, ddocId := checkDocId(w, r)
	if bucket == nil || ddocId == "" {
		return
	}
	viewId, ok := vars["viewId"]
	if !ok || viewId == "" {
		http.Error(w, "missing viewId from path", 400)
		return
	}
	ddocs := bucket.GetDDocs()
	if ddocs == nil {
		http.Error(w, "getDDocs nil", 500)
		return
	}
	ddocIdFull := "_design/" + ddocId
	ddoc, ok := (*ddocs)[ddocIdFull]
	if !ok {
		http.Error(w, fmt.Sprintf("design doc not found, ddocId: %v",
			ddocIdFull), 404)
		return
	}
	view, ok := ddoc.Views[viewId]
	if !ok {
		http.Error(w, fmt.Sprintf("view not found, viewId: %v, ddocId: %v",
			viewId, ddocIdFull), 404)
		return
	}

	in, out := MakeViewRowMerger(bucket)
	for vbid := 0; vbid < len(in); vbid++ {
		vb, err := bucket.GetVBucket(uint16(vbid))
		if err != nil {
			// TODO: Cleanup already in-flight merging goroutines?
			http.Error(w, fmt.Sprintf("GetVBucket err: %v", err), 404)
			return
		}
		go visitVIndex(vb, ddocId, viewId, p, in[vbid])
	}

	vr := &ViewResult{Rows: make([]*ViewRow, 0, 100)}
	for row := range out {
		vr.Rows = append(vr.Rows, row)
	}
	vr, err = processViewResult(bucket, vr, p)
	if err != nil {
		http.Error(w, fmt.Sprintf("processViewResult error: %v", err), 400)
		return
	}
	if view.Reduce == "" || p.Reduce == false {
		// TODO: Handle p.UpdateSeq.
		if p.IncludeDocs {
			vr, err = docifyViewResult(bucket, vr)
			if err != nil {
				http.Error(w, fmt.Sprintf("docifyViewResults error: %v", err), 500)
				return
			}
		}
	} else {
		vr, err = reduceViewResult(bucket, vr, p, view.Reduce)
		if err != nil {
			http.Error(w, fmt.Sprintf("reduceViewResult error: %v", err), 400)
			return
		}
	}
	skip := int(p.Skip)
	if skip > 0 {
		if skip > len(vr.Rows) {
			skip = len(vr.Rows)
		}
		vr.Rows = vr.Rows[skip:]
	}
	limit := int(p.Limit)
	if limit > 0 {
		if limit > len(vr.Rows) {
			limit = len(vr.Rows)
		}
		vr.Rows = vr.Rows[:limit]
	}
	vr.TotalRows = len(vr.Rows)

	jsonEncode(w, vr)
}

// Originally from github.com/couchbaselabs/walrus, but modified to
// use ViewParams.
func processViewResult(bucket Bucket, result *ViewResult,
	p *ViewParams) (*ViewResult, error) {
	if p.Key != nil {
		p.StartKey = p.Key
		p.EndKey = p.Key
	}

	if p.StartKey != nil {
		i := sort.Search(len(result.Rows), func(i int) bool {
			return walrus.CollateJSON(result.Rows[i].Key, p.StartKey) >= 0
		})
		if p.Descending {
			result.Rows = result.Rows[:i+1]
		} else {
			result.Rows = result.Rows[i:]
		}
	}

	if p.EndKey != nil {
		i := sort.Search(len(result.Rows), func(i int) bool {
			if p.InclusiveEnd {
				return walrus.CollateJSON(result.Rows[i].Key, p.EndKey) > 0
			}
			return walrus.CollateJSON(result.Rows[i].Key, p.EndKey) >= 0
		})
		if p.Descending {
			result.Rows = result.Rows[i:]
		} else {
			result.Rows = result.Rows[:i]
		}
	}

	if p.Descending {
		reverseViewRows(result.Rows)
	}

	return result, nil
}

// TODO: Allow reduction to be incremental w.r.t. memory usage, by
// reducing chunks at a time rather than needing all rows in memory.
func reduceViewResult(bucket Bucket, result *ViewResult,
	p *ViewParams, reduceFunction string) (*ViewResult, error) {
	groupLevel := 0
	if p.Group {
		groupLevel = 0x7fffffff
	}
	if p.GroupLevel > 0 {
		groupLevel = int(p.GroupLevel)
	}

	o := newReducer()
	fnv, err := OttoNewFunction(o, reduceFunction)
	if err != nil {
		return result, err
	}

	initialCapacity := 200

	results := make([]*ViewRow, 0, initialCapacity)
	groupKeys := make([]interface{}, 0, initialCapacity)
	groupValues := make([]interface{}, 0, initialCapacity)

	i := 0
	j := 0

	for i < len(result.Rows) {
		groupKeys = groupKeys[:0]
		groupValues = groupValues[:0]

		startRow := result.Rows[i]
		groupKey := ArrayPrefix(startRow.Key, groupLevel)

		for j = i; j < len(result.Rows); j++ {
			row := result.Rows[j]
			rowKey := ArrayPrefix(row.Key, groupLevel)
			if walrus.CollateJSON(groupKey, rowKey) < 0 {
				break
			}
			groupKeys = append(groupKeys, row.Key)
			groupValues = append(groupValues, row.Value)
		}
		i = j

		okeys, err := OttoFromGoArray(o, groupKeys)
		if err != nil {
			return result, err
		}
		ovalues, err := OttoFromGoArray(o, groupValues)
		if err != nil {
			return result, err
		}

		ores, err := fnv.Call(fnv, okeys, ovalues, otto.FalseValue())
		if err != nil {
			return result, fmt.Errorf("call reduce err: %v, reduceFunction: %v, %v, %v",
				err, reduceFunction, okeys, ovalues)
		}
		gres, err := ores.Export()
		if err != nil {
			return result, fmt.Errorf("converting reduce result err: %v", err)
		}

		results = append(results, &ViewRow{Key: groupKey, Value: gres})
	}

	result.Rows = results
	return result, nil
}

func reverseViewRows(r ViewRows) {
	num := len(r)
	mid := num / 2
	for i := 0; i < mid; i++ {
		r[i], r[num-i-1] = r[num-i-1], r[i]
	}
}

func docifyViewResult(bucket Bucket, result *ViewResult) (
	*ViewResult, error) {
	for _, row := range result.Rows {
		if row.Id != "" {
			res := GetItem(bucket, []byte(row.Id), VBActive)
			if res.Status == gomemcached.SUCCESS {
				var parsedDoc interface{}
				err := jsonUnmarshal(res.Body, &parsedDoc)
				if err == nil {
					row.Doc = &ViewDocValue{
						Meta: map[string]interface{}{
							"id":  row.Id,
							"rev": "0",
						},
						Json: parsedDoc,
					}
				} else {
					// TODO: Is this the right encoding for non-json?
					// no
					// row.Doc = Bytes(res.Body)
				}
			} // TODO: Handle else-case when no doc.
		}
	}
	return result, nil
}

func visitVIndex(vb *VBucket, ddocId string, viewId string, p *ViewParams,
	ch chan *ViewRow) error {
	defer close(ch)

	if vb == nil {
		return fmt.Errorf("no vbucket during visitVIndex(), ddocId: %v, viewId: %v",
			ddocId, viewId)
	}
	switch p.Stale {
	case "false":
		_, err := vb.viewsRefresh()
		if err != nil {
			return err
		}
	case "update_after":
		// Asynchronously start view updates after finishing
		// this request.
		defer func() { go vb.viewsRefresh() }()
	}
	viewsStore, err := vb.getViewsStore()
	if err != nil {
		return err
	}
	vindex := viewsStore.coll("_design/" + ddocId + "/" + viewId + VINDEX_COLL_SUFFIX)
	if vindex == nil {
		return fmt.Errorf("no vindex during visitVIndex(), ddocId: %v, viewId: %v",
			ddocId, viewId)
	}

	var begKey interface{}
	var begKeyBytes []byte
	var endKey interface{}

	if p.Key != nil {
		p.StartKey = p.Key
		p.EndKey = p.Key
	}
	if p.Descending {
		begKey = p.EndKey
		endKey = p.StartKey
	} else {
		begKey = p.StartKey
		endKey = p.EndKey
	}
	if begKey != nil {
		begKeyBytes, err = vindexKey(nil, begKey)
		if err != nil {
			return err
		}
	}
	if bytes.Equal(begKeyBytes, []byte("null\x00")) {
		begKeyBytes = nil
	}

	errVisit := vindex.VisitItemsAscend(begKeyBytes, true, func(i *gkvlite.Item) bool {
		docId, emitKey, err := vindexKeyParse(i.Key)
		if err != nil {
			return false
		}
		if endKey != nil && walrus.CollateJSON(emitKey, endKey) > 0 {
			return false
		}
		var emitValue interface{}
		err = jsonUnmarshal(i.Val, &emitValue)
		if err != nil {
			return false
		}
		ch <- &ViewRow{
			Id:    string(docId),
			Key:   emitKey,
			Value: emitValue,
		}
		return true
	})
	if p.Stale == "update_after" {
		vb.markStale()
	}
	if errVisit != nil {
		return errVisit
	}
	return err
}

func MakeViewRowMerger(bucket Bucket) ([]chan *ViewRow, chan *ViewRow) {
	out := make(chan *ViewRow)
	np := bucket.GetBucketSettings().NumPartitions
	if np == 1 {
		return []chan *ViewRow{out}, out
	}
	in := make([]chan *ViewRow, np)
	for vbid := 0; vbid < np; vbid++ {
		in[vbid] = make(chan *ViewRow)
	}
	go MergeViewRows(in, out)
	return in, out
}
