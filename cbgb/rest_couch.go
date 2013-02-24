package main

import (
	"fmt"
	"net/http"

	"github.com/couchbaselabs/cbgb"
	"github.com/dustin/gomemcached"
	"github.com/gorilla/mux"
)

func restCouch(r *mux.Router) {
	dbr := r.PathPrefix("/{db}/").Subrouter()

	dbr.Handle("/_design/{docid}",
		http.HandlerFunc(couchDbGetDesignDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_design/{docid}",
		http.HandlerFunc(couchDbPutDesignDoc)).Methods("PUT")
	dbr.Handle("/_design/{docid}",
		http.HandlerFunc(couchDbDelDesignDoc)).Methods("DELETE")

	dbr.Handle("/{docid}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD")
	dbr.Handle("/{docid}",
		http.HandlerFunc(couchDbPutDoc)).Methods("PUT")
	dbr.Handle("/{docid}",
		http.HandlerFunc(couchDbDelDoc)).Methods("DELETE")
}

func couchDbGetDesignDoc(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "unimplemented", 501)
}

func couchDbPutDesignDoc(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "unimplemented", 501)
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
	docId, ok := vars["docid"]
	if !ok {
		http.Error(w, "missing docid parameter", 400)
		return vars, bucketName, bucket, ""
	}
	return vars, bucketName, bucket, docId
}
