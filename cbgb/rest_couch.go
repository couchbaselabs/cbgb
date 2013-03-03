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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"

	"github.com/couchbaselabs/cbgb"
	"github.com/couchbaselabs/walrus"
	"github.com/dustin/gomemcached"
	"github.com/gorilla/mux"
	"github.com/robertkrimen/otto"
)

func restCouchAPI(r *mux.Router) *mux.Router {
	dbr := r.PathPrefix("/{db}/").Subrouter()

	dbr.Handle("/_design/{docId}/_view/{viewId}",
		http.HandlerFunc(couchDbGetView)).Methods("GET")

	dbr.Handle("/_design/{docId}",
		http.HandlerFunc(couchDbGetDesignDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_design/{docId}",
		http.HandlerFunc(couchDbPutDesignDoc)).Methods("PUT")
	dbr.Handle("/_design/{docId}",
		http.HandlerFunc(couchDbDelDesignDoc)).Methods("DELETE")

	dbr.Handle("/{docId}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD")
	dbr.Handle("/{docId}",
		http.HandlerFunc(couchDbPutDoc)).Methods("PUT")
	dbr.Handle("/{docId}",
		http.HandlerFunc(couchDbDelDoc)).Methods("DELETE")

	return dbr
}

func couchDbGetDesignDoc(w http.ResponseWriter, r *http.Request) {
	_, _, bucket, ddocId := checkDocId(w, r)
	if bucket == nil || ddocId == "" {
		return
	}
	body, err := bucket.GetDDoc("_design/" + ddocId)
	if err != nil {
		http.Error(w, fmt.Sprintf("getDDoc err: %v", err), 500)
		return
	}
	if body == nil {
		http.Error(w, "Not Found", 404)
		return
	}
	w.Write(body)
}

func couchDbPutDesignDoc(w http.ResponseWriter, r *http.Request) {
	_, _, bucket, ddocId := checkDocId(w, r)
	if bucket == nil || ddocId == "" {
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Bad Request, err: %v", err), 400)
		return
	}
	var into map[string]interface{}
	if err = json.Unmarshal(body, &into); err != nil {
		http.Error(w, fmt.Sprintf("Bad Request, err: %v", err), 400)
		return
	}
	if _, err = walrus.CheckDDoc(into); err != nil {
		http.Error(w, fmt.Sprintf("Bad Request, err: %v", err), 400)
		return
	}
	if err = bucket.SetDDoc("_design/"+ddocId, body); err != nil {
		http.Error(w, fmt.Sprintf("Internal Server Error, err: %v", err), 500)
		return
	}
	w.Write([]byte(http.StatusText(200)))
}

func couchDbDelDesignDoc(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "unimplemented", 501)
}

func couchDbGetDoc(w http.ResponseWriter, r *http.Request) {
	_, _, bucket, docId := checkDocId(w, r)
	if bucket == nil || docId == "" {
		return
	}
	res := cbgb.GetItem(bucket, []byte(docId), cbgb.VBActive)
	if res == nil || res.Status != gomemcached.SUCCESS {
		http.Error(w, "Not Found", 404)
		return
	}
	// TODO: Content Type, Accepts, much to leverage from sync_gateway.
	w.Write(res.Body)
}

func couchDbPutDoc(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "unimplemented", 501)
}

func couchDbDelDoc(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "unimplemented", 501)
}

func couchDbGetView(w http.ResponseWriter, r *http.Request) {
	p, err := cbgb.ParseViewParams(r)
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
	body, err := bucket.GetDDoc("_design/" + ddocId)
	if err != nil {
		http.Error(w, fmt.Sprintf("getDDoc err: %v", err), 500)
		return
	}
	if body == nil {
		http.Error(w, "design doc not found", 404)
		return
	}
	var ddoc walrus.DesignDoc
	if err = json.Unmarshal(body, &ddoc); err != nil {
		http.Error(w, fmt.Sprintf("could not unmarshal design doc, err: %v", err), 500)
		return
	}
	view, ok := ddoc.Views[viewId]
	if !ok {
		http.Error(w, "view not found", 404)
		return
	}
	if view.Map == "" {
		http.Error(w, "view map function missing", 400)
		return
	}
	mf, err := walrus.NewJSMapFunction(view.Map)
	if err != nil {
		http.Error(w, fmt.Sprintf("view map function error: %v", err), 400)
		return
	}
	defer mf.Stop()

	vr := &cbgb.ViewResult{Rows: make([]*cbgb.ViewRow, 0, 100)}
	for vbid := 0; vbid < cbgb.MAX_VBUCKETS; vbid++ {
		vb := bucket.GetVBucket(uint16(vbid))
		if vb != nil {
			var errVisit error
			err = vb.Visit(nil, func(key []byte, data []byte) bool {
				docId := string(key)
				var emits []walrus.ViewRow
				emits, errVisit = mf.CallFunction(string(data), docId)
				if errVisit != nil {
					return false
				}
				for _, emit := range emits {
					vr.Rows = append(vr.Rows, &cbgb.ViewRow{
						Id:    docId,
						Key:   emit.Key,
						Value: emit.Value,
					})
				}
				return true
			})
			if err != nil {
				http.Error(w, fmt.Sprintf("view visit error: %v",
					err), 400)
				return
			}
			if errVisit != nil {
				http.Error(w, fmt.Sprintf("view visit function error: %v",
					errVisit), 400)
				return
			}
		}
	}
	sort.Sort(vr.Rows)

	vr, err = processViewResult(bucket, vr, p)
	if err != nil {
		http.Error(w, fmt.Sprintf("processViewResult error: %v", err), 400)
		return
	}
	vr, err = reduceViewResult(bucket, vr, p, view.Reduce)
	if err != nil {
		http.Error(w, fmt.Sprintf("reduceViewResult error: %v", err), 400)
		return
	}
	vr.TotalRows = len(vr.Rows)

	jsonEncode(w, vr)
}

func checkDb(w http.ResponseWriter, r *http.Request) (
	vars map[string]string, bucketName string, bucket cbgb.Bucket) {
	vars = mux.Vars(r)
	bucketName, ok := vars["db"]
	if !ok {
		http.Error(w, "missing db parameter", 400)
		return vars, "", nil
	}
	bucket = buckets.Get(bucketName)
	if bucket == nil {
		http.Error(w, fmt.Sprintf("no db: %v", bucketName), 404)
		return vars, bucketName, nil
	}
	return vars, bucketName, bucket
}

func checkDocId(w http.ResponseWriter, r *http.Request) (
	vars map[string]string, bucketName string, bucket cbgb.Bucket, docId string) {
	vars, bucketName, bucket = checkDb(w, r)
	if bucket == nil {
		return vars, bucketName, bucket, ""
	}
	docId, ok := vars["docId"]
	if !ok {
		http.Error(w, "missing docId from path", 400)
		return vars, bucketName, bucket, ""
	}
	return vars, bucketName, bucket, docId
}

// Originally from github.com/couchbaselabs/walrus, but modified to
// use ViewParams.

func processViewResult(bucket cbgb.Bucket, result *cbgb.ViewResult,
	p *cbgb.ViewParams) (*cbgb.ViewResult, error) {
	// TODO: Handle p.Skip.
	// TODO: Handle p.UpdateSeq.

	if p.Descending {
		return result, fmt.Errorf("descending is not supported yet, sorry") // TODO.
	}

	if p.Key != nil {
		p.StartKey = p.Key
		p.EndKey = p.Key
	}

	if p.StartKey != nil {
		i := sort.Search(len(result.Rows), func(i int) bool {
			return walrus.CollateJSON(result.Rows[i].Key, p.StartKey) >= 0
		})
		result.Rows = result.Rows[i:]
	}

	// TODO: Does the limit get processed after reduce?
	if p.Limit > 0 && uint64(len(result.Rows)) > p.Limit {
		result.Rows = result.Rows[:p.Limit]
	}

	if p.EndKey != nil {
		i := sort.Search(len(result.Rows), func(i int) bool {
			if p.InclusiveEnd {
				return walrus.CollateJSON(result.Rows[i].Key, p.EndKey) > 0
			}
			return walrus.CollateJSON(result.Rows[i].Key, p.EndKey) >= 0
		})
		result.Rows = result.Rows[:i]
	}

	if p.IncludeDocs { // TODO.
		return result, fmt.Errorf("includeDocs is not supported yet, sorry") // TODO.
	}

	return result, nil
}

func reduceViewResult(bucket cbgb.Bucket, result *cbgb.ViewResult,
	p *cbgb.ViewParams, reduceFunction string) (*cbgb.ViewResult, error) {
	if reduceFunction == "" {
		return result, nil
	}
	if p.Reduce == false {
		return result, nil
	}

	o := otto.New()
	fnv, err := OttoNewFunction(o, reduceFunction)
	if err != nil {
		return result, err
	}

	keys := make([]interface{}, len(result.Rows))
	values := make([]interface{}, len(result.Rows))
	for i, row := range result.Rows {
		keys[i] = row.Key
		values[i] = row.Value
	}

	okeys, err := OttoFromGoArray(o, keys)
	if err != nil {
		return result, err
	}
	ovalues, err := OttoFromGoArray(o, values)
	if err != nil {
		return result, err
	}

	ores, err := fnv.Call(fnv, okeys, ovalues, otto.FalseValue())
	if err != nil {
		return result, fmt.Errorf("call reduce err: %v, reduceFunction: %v, %v, %v",
			err, reduceFunction, okeys, ovalues)
	}
	gres, err := OttoToGo(ores)
	if err != nil {
		return result, fmt.Errorf("converting reduce result err: %v", err)
	}

	result.Rows = []*cbgb.ViewRow{&cbgb.ViewRow{Value: gres}}
	return result, nil
}

// Originally from github.com/couchbaselabs/walrus, but capitalized.
// TODO: Push this back to walrus.
// Converts an Otto value to a Go value. Handles all JSON-compatible types.
func OttoToGo(value otto.Value) (interface{}, error) {
	if value.IsBoolean() {
		return value.ToBoolean()
	} else if value.IsNull() || value.IsUndefined() {
		return nil, nil
	} else if value.IsNumber() {
		return value.ToFloat()
	} else if value.IsString() {
		return value.ToString()
	} else {
		switch value.Class() {
		case "Array":
			return OttoToGoArray(value.Object())
		}
	}
	return nil, fmt.Errorf("Unsupported Otto value: %v", value)
}

// Originally from github.com/couchbaselabs/walrus, but capitalized.
// TODO: Push this back to walrus.
func OttoToGoArray(array *otto.Object) ([]interface{}, error) {
	lengthVal, err := array.Get("length")
	if err != nil {
		return nil, err
	}
	length, err := lengthVal.ToInteger()
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, length)
	for i := 0; i < int(length); i++ {
		item, err := array.Get(strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		result[i], err = OttoToGo(item)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func OttoFromGoArray(o *otto.Otto, arr []interface{}) (otto.Value, error) {
	jarr, err := json.Marshal(arr)
	if err != nil {
		return otto.UndefinedValue(),
			fmt.Errorf("could not jsonify arr, err: %v", err)
	}
	oarr, err := o.Object("({v:" + string(jarr) + "})")
	if err != nil {
		return otto.UndefinedValue(),
			fmt.Errorf("could not convert arr, err: %v", err)
	}
	ovarr, err := oarr.Get("v")
	if err != nil {
		return otto.UndefinedValue(),
			fmt.Errorf("could not convert oarr, err: %v", err)
	}
	if ovarr.Class() != "Array" {
		return otto.UndefinedValue(),
			fmt.Errorf("expected ovarr to be array, got: %#v, %v, jarr: %v",
				ovarr, ovarr.Class(), string(jarr))
	}
	return ovarr, nil
}

func OttoNewFunction(o *otto.Otto, f string) (otto.Value, error) {
	fn, err := o.Object("(" + f + ")")
	if err != nil {
		return otto.UndefinedValue(),
			fmt.Errorf("could not eval function, err: %v", err)
	}
	if fn.Class() != "Function" {
		return otto.UndefinedValue(),
			fmt.Errorf("fn not a function, was: %v", fn.Class())
	}
	fnv := fn.Value()
	if fnv.Class() != "Function" {
		return otto.UndefinedValue(),
			fmt.Errorf("fnv not a function, was: %v", fnv.Class())
	}
	return fnv, nil
}
