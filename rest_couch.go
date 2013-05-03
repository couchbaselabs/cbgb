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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/couchbaselabs/walrus"
	"github.com/dustin/gomemcached"
	"github.com/gorilla/mux"
)

func restCouchServe(rest string, staticPath string) {
	r := mux.NewRouter()
	restCouchAPI(r)
	log.Fatal(http.ListenAndServe(rest, authenticationFilter{r}))
}

func referencesVBucket(r *http.Request, rm *mux.RouteMatch) bool {
	return strings.Contains(strings.ToLower(r.RequestURI), "%2f")
}

func doesNotReferenceVBucket(r *http.Request, rm *mux.RouteMatch) bool {
	return !strings.Contains(strings.ToLower(r.RequestURI), "%2f")
}

func includesBucketUUID(r *http.Request, rm *mux.RouteMatch) bool {
	return strings.Contains(strings.ToLower(r.RequestURI), "%3b")
}

func restCouchAPI(r *mux.Router) *mux.Router {
	r.Handle("/{db};{bucketUUID}",
		http.HandlerFunc(couchDbGetDb)).Methods("GET", "HEAD").
		MatcherFunc(includesBucketUUID)
	r.Handle("/{db}",
		http.HandlerFunc(couchDbGetDb)).Methods("GET", "HEAD")

	dbr := r.PathPrefix("/{db}/").Subrouter()

	dbr.Handle("/",
		http.HandlerFunc(couchDbGetDb)).
		Methods("GET", "HEAD")

	dbr.Handle("/_all_docs",
		http.HandlerFunc(deadlinedHandler(time.Second, couchDbAllDocs))).
		Methods("GET")

	dbr.Handle("/_design/{docId}/_view/{viewId}",
		http.HandlerFunc(deadlinedHandler(time.Second, couchDbGetView))).
		Methods("GET")

	dbr.Handle("/_design/{docId}",
		http.HandlerFunc(couchDbGetDesignDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_design/{docId}",
		http.HandlerFunc(couchDbPutDesignDoc)).Methods("PUT")
	dbr.Handle("/_design/{docId}",
		http.HandlerFunc(couchDbDelDesignDoc)).Methods("DELETE")

	dbr.Handle("/{docId}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD").
		MatcherFunc(doesNotReferenceVBucket)
	dbr.Handle("/{docId}",
		http.HandlerFunc(couchDbPutDoc)).Methods("PUT").
		MatcherFunc(doesNotReferenceVBucket)
	dbr.Handle("/{docId}",
		http.HandlerFunc(couchDbDelDoc)).Methods("DELETE").
		MatcherFunc(doesNotReferenceVBucket)

	dbr.Handle("/{vbucket};{bucketUUID}",
		http.HandlerFunc(couchDbGetDb)).Methods("GET", "HEAD").
		MatcherFunc(referencesVBucket).
		MatcherFunc(includesBucketUUID)
	dbr.Handle("/{vbucket}",
		http.HandlerFunc(couchDbGetDb)).Methods("GET", "HEAD").
		MatcherFunc(referencesVBucket)

	dbr.Handle("/{vbucket};{bucketUUID}/{docId}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD").
		MatcherFunc(referencesVBucket).
		MatcherFunc(includesBucketUUID)
	dbr.Handle("/{vbucket}/{docId}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD").
		MatcherFunc(referencesVBucket)

	dbr.Handle("/{vbucket};{bucketUUID}/_local/{docId}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD").
		MatcherFunc(referencesVBucket).
		MatcherFunc(includesBucketUUID)
	dbr.Handle("/{vbucket}/_local/{docId}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD").
		MatcherFunc(referencesVBucket)
	dbr.Handle("/{vbucket};{bucketUUID}/_local/{docId}/{source}/{destination}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD").
		MatcherFunc(referencesVBucket).
		MatcherFunc(includesBucketUUID)
	dbr.Handle("/{vbucket}/_local/{docId}/{source}/{destination}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD").
		MatcherFunc(referencesVBucket)

	dbr.Handle("/{vbucket};{bucketUUID}/_revs_diff",
		http.HandlerFunc(couchDbRevsDiff)).Methods("POST").
		MatcherFunc(referencesVBucket).
		MatcherFunc(includesBucketUUID)
	dbr.Handle("/{vbucket}/_revs_diff",
		http.HandlerFunc(couchDbRevsDiff)).Methods("POST").
		MatcherFunc(referencesVBucket)

	dbr.Handle("/{vbucket};{bucketUUID}/_bulk_docs",
		http.HandlerFunc(couchDbBulkDocs)).Methods("POST").
		MatcherFunc(referencesVBucket).
		MatcherFunc(includesBucketUUID)
	dbr.Handle("/{vbucket}/_bulk_docs",
		http.HandlerFunc(couchDbBulkDocs)).Methods("POST").
		MatcherFunc(referencesVBucket)

	dbr.Handle("/{vbucket};{bucketUUID}/_ensure_full_commit",
		http.HandlerFunc(couchDbEnsureFullCommit)).Methods("POST").
		MatcherFunc(referencesVBucket).
		MatcherFunc(includesBucketUUID)
	dbr.Handle("/{vbucket}/_ensure_full_commit",
		http.HandlerFunc(couchDbEnsureFullCommit)).Methods("POST").
		MatcherFunc(referencesVBucket)

	return dbr
}

func couchDbGetDesignDoc(w http.ResponseWriter, r *http.Request) {
	_, _, bucket, ddocId := checkDocId(w, r)
	if bucket == nil || ddocId == "" {
		return
	}
	ddocIdFull := "_design/" + ddocId
	body, err := bucket.GetDDoc(ddocIdFull)
	if err != nil {
		http.Error(w, fmt.Sprintf("getDDoc err: %v, ddocIdFull: %v",
			err, ddocIdFull), 500)
		return
	}
	if body == nil {
		http.Error(w, "Not Found", 404)
		return
	}
	// w.Header().Add("X-Couchbase-Meta", walrus.MakeMeta(ddocIdFull))
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
		http.Error(w, fmt.Sprintf("SetDDoc err: %v", err), 400)
		return
	}
	w.WriteHeader(201)
}

func couchDbDelDesignDoc(w http.ResponseWriter, r *http.Request) {
	_, _, bucket, ddocId := checkDocId(w, r)
	if bucket == nil || ddocId == "" {
		return
	}
	if err := bucket.DelDDoc("_design/" + ddocId); err != nil {
		http.Error(w, fmt.Sprintf("DelDDoc err: %v", err), 400)
		return
	}
}

func couchDbGetDb(w http.ResponseWriter, r *http.Request) {
	_, bucketName, bucket := checkDb(w, r)
	if bucket == nil {
		return
	}
	if r.Method == "HEAD" {
		return
	}
	jsonEncode(w, map[string]interface{}{
		"db_name": bucketName,
	})
}

// TODO this implementation does no conflict resolution
// only suitable for one way replications
func couchDbRevsDiff(w http.ResponseWriter, r *http.Request) {
	_, _, bucket := checkDb(w, r)
	if bucket == nil {
		return
	}
	revsDiffRequest := map[string]interface{}{}
	d := json.NewDecoder(r.Body)
	err := d.Decode(&revsDiffRequest)
	if err != nil {
		http.Error(w, fmt.Sprintf("Unable to parse _revs_diff body as JSON: %v", err), 500)
		return
	}

	revsDiffResponse := map[string]interface{}{}
	for key, val := range revsDiffRequest {
		revsDiffResponse[key] = map[string]interface{}{"missing": val}
	}
	jsonEncode(w, revsDiffResponse)
}

type BulkDocsItemMeta struct {
	Id         string  `json:"id"`
	Rev        string  `json:"rev"`
	Expiration float64 `json:"expiration"`
	Flags      float64 `json:"flags"`
	Deleted    bool    `json:"deleted,omitempty"`
	Att_reason string  `json:"att_reason,omitempty"`
}

type BulkDocsItem struct {
	Meta   BulkDocsItemMeta `json:"meta"`
	Base64 string           `json:"base64"`
}

type BulkDocsRequest struct {
	Docs []BulkDocsItem `json:"docs"`
}

// TODO this implementation uses the wrong memcached commands
// it should use commands that can overwrite the metadata
// for now this is done with the wrong commands just to get
// data transfer up and running
func couchDbBulkDocs(w http.ResponseWriter, r *http.Request) {
	_, _, bucket := checkDb(w, r)
	if bucket == nil {
		return
	}

	var bulkDocsRequest BulkDocsRequest
	d := json.NewDecoder(r.Body)
	err := d.Decode(&bulkDocsRequest)
	if err != nil {
		http.Error(w, fmt.Sprintf("Unable to parse _bulk_docs body as JSON: %v",
			err), 500)
		return
	}

	bulkDocsResponse := make([]map[string]interface{}, 0, len(bulkDocsRequest.Docs))
	for _, doc := range bulkDocsRequest.Docs {
		key := []byte(doc.Meta.Id)
		vbucketId := VBucketIdForKey(key, bucket.GetBucketSettings().NumPartitions)
		vbucket, _ := bucket.GetVBucket(vbucketId)
		if vbucket == nil {
			http.Error(w, fmt.Sprintf("Invalid vbucket for this key: %v - %v",
				key, err), 500)
			return
		}

		val, err := base64.StdEncoding.DecodeString(doc.Base64)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error decoding base64 data "+
				"_bulk_docs body as JSON for key: %v - %v",
				key, err), 500)
			return
		}

		response := vbucket.Dispatch(nil, &gomemcached.MCRequest{
			Opcode:  gomemcached.SET,
			VBucket: vbucketId,
			Key:     key,
			Body:    val,
		})

		// TODO proper error handling
		// for now we just bail if anything ever goes wrong
		if response.Status != gomemcached.SUCCESS {
			log.Printf("Got error writing data: %v - %v",
				string(key), string(response.Body))
			http.Error(w, "Internal Error", 500)
			return
		} else {
			// TODO return actual revision created here we
			// just lie and pretend to have created the
			// requested revsion (we did not)
			bulkDocsResponse = append(bulkDocsResponse,
				map[string]interface{}{
					"id":  doc.Meta.Id,
					"rev": doc.Meta.Rev})
		}
	}
	w.WriteHeader(201)
	jsonEncode(w, bulkDocsResponse)
}

func couchDbEnsureFullCommit(w http.ResponseWriter, r *http.Request) {
	_, _, bucket := checkDb(w, r)
	if bucket == nil {
		return
	}
	err := bucket.Flush()
	if err != nil {
		http.Error(w,
			fmt.Sprintf("_ensure_full_commit bucket.Flush() err: %v", err), 500)
		return
	}
	w.WriteHeader(201)
	jsonEncode(w, map[string]interface{}{"ok": true})
}

func couchDbGetDoc(w http.ResponseWriter, r *http.Request) {
	_, _, bucket, docId := checkDocId(w, r)
	if bucket == nil || docId == "" {
		return
	}
	res := GetItem(bucket, []byte(docId), VBActive)
	if res == nil || res.Status != gomemcached.SUCCESS {
		http.Error(w, `{"error": "not_found", "reason": "missing"}`, 404)
		return
	}
	// TODO: Content Type, Accepts, much to leverage from sync_gateway.
	// w.Header().Add("X-Couchbase-Meta", walrus.MakeMeta(docId))
	w.Write(res.Body)
}

func couchDbPutDoc(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "unimplemented", 501)
}

func couchDbDelDoc(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "unimplemented", 501)
}

func checkDb(w http.ResponseWriter, r *http.Request) (
	vars map[string]string, bucketName string, bucket Bucket) {

	vars = mux.Vars(r)
	bucketName, ok := vars["db"]
	if !ok {
		http.Error(w, "missing db parameter", 400)
		return vars, "", nil
	}

	if !currentUser(r).canAccess(bucketName) {
		http.Error(w, "Access denied", 403)
		return vars, "", nil
	}

	bucket = buckets.Get(bucketName)
	if bucket == nil {
		http.Error(w, fmt.Sprintf("no db: %v", bucketName), 404)
		return vars, bucketName, nil
	}

	bucketUUID, ok := vars["bucketUUID"]
	if ok {
		// if it contains a bucket UUID, it MUST match
		actualBucketUUID := bucket.GetBucketSettings().UUID
		if bucketUUID != actualBucketUUID {
			http.Error(w, fmt.Sprintf("uuids_dont_match"), 404)
			return vars, bucketName, nil
		}
	}

	vbucketString, ok := vars["vbucket"]
	if ok {
		// if the request contains a vbucket specification
		// ensure that it refers to a valid vbucket
		// we don't return it because none of our functionality will use it
		bucketName = bucketName + "%2f" + vbucketString
		if vbucketString == "master" {
			return vars, bucketName, bucket
		}
		vbucketIdFull, err := strconv.ParseUint(vbucketString, 10, 16)
		if err != nil {
			http.Error(w, fmt.Sprintf("no db: %v", bucketName), 404)
			return vars, bucketName, nil
		}
		vbucketId := uint16(vbucketIdFull)
		vbucket, _ := bucket.GetVBucket(vbucketId)
		if vbucket == nil {
			http.Error(w, fmt.Sprintf("no db: %v", bucketName), 404)
			return vars, bucketName, nil
		}
	}
	return vars, bucketName, bucket
}

func checkDocId(w http.ResponseWriter, r *http.Request) (
	vars map[string]string, bucketName string, bucket Bucket, docId string) {
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

func couchDbAllDocs(w http.ResponseWriter, r *http.Request) {
	_, _, bucket := checkDb(w, r)
	if bucket == nil {
		return
	}
	_, err := ParseViewParams(r) // TODO: Handle params.
	if err != nil {
		http.Error(w, fmt.Sprintf("param parsing err: %v", err), 400)
		return
	}
	in, out := MakeViewRowMerger(bucket)
	for vbid := 0; vbid < len(in); vbid++ {
		vb, _ := bucket.GetVBucket(uint16(vbid))
		go visitVBucketAllDocs(vb, in[vbid])
	}
	w.Write([]byte(`{"rows":[`))
	i := 0
	for vr := range out {
		j, err := json.Marshal(vr)
		if err == nil {
			if i > 0 {
				w.Write([]byte(",\n"))
			}
			_, err = w.Write(j)
			if err == nil {
				i++
			}
		} // TODO: else, json marshalling and Write error handling.
	}
	w.Write([]byte(fmt.Sprintf(`],"total_rows":%v}`, i)))
}

func visitVBucketAllDocs(vb *VBucket, ch chan *ViewRow) {
	defer close(ch)

	if vb == nil {
		return
	}
	vb.Visit(nil, func(key []byte, data []byte) bool {
		docId := string(key)
		docType := "json"
		var doc interface{}
		err := json.Unmarshal(data, &doc)
		if err != nil {
			doc = base64.StdEncoding.EncodeToString(data)
			docType = "base64"
		}
		// TODO: The couchdb spec emits Value instead of Doc.
		ch <- &ViewRow{
			Id:  docId,
			Key: docId,
			Doc: &ViewDocValue{
				Meta: map[string]interface{}{
					"id":   docId,
					"type": docType,
					// TODO: rev.
				},
				Json: doc,
			},
		}
		return true
	})
}
