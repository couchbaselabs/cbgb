package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/couchbaselabs/cbgb"
	"github.com/couchbaselabs/walrus"
	"github.com/dustin/gomemcached"
	"github.com/gorilla/mux"
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
	if bucket == nil || len(ddocId) <= 0 {
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
	if bucket == nil || len(ddocId) <= 0 {
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
	if err = bucket.SetDDoc("_design/"+ddocId, body); err != nil {
		http.Error(w, fmt.Sprintf("Internal Server Error, err: %v", err), 500)
		return
	}
}

func couchDbDelDesignDoc(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "unimplemented", 501)
}

func couchDbGetDoc(w http.ResponseWriter, r *http.Request) {
	_, _, bucket, docId := checkDocId(w, r)
	if bucket == nil || len(docId) <= 0 {
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
	vars, _, bucket, ddocId := checkDocId(w, r)
	if bucket == nil || len(ddocId) <= 0 {
		return
	}
	viewId, ok := vars["viewId"]
	if !ok {
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
		http.Error(w, "could not unmarshal design doc", 500)
		return
	}
	_, ok = ddoc.Views[viewId]
	if !ok {
		http.Error(w, "view not found", 404)
		return
	}
	http.Error(w, "unimplemented", 501)
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
