package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/couchbaselabs/cbgb"
	"github.com/gorilla/mux"
)

func parseBucketName(w http.ResponseWriter, r *http.Request) (string, cbgb.Bucket) {
	bucketName, ok := mux.Vars(r)["bucketName"]
	if !ok {
		http.Error(w, "missing bucketName parameter", 400)
		return "", nil
	}
	bucket := buckets.Get(bucketName)
	if bucket == nil {
		http.Error(w, "no bucket with that bucketName", 404)
		return bucketName, nil
	}
	return bucketName, bucket
}

func getIntValue(r *http.Request, name string, def int64) int64 {
	valstr := r.FormValue(name)
	if valstr == "" {
		return def
	}
	val, err := strconv.ParseInt(valstr, 10, 64)
	if err != nil {
		return def
	}
	return val
}

func jsonEncode(w io.Writer, i interface{}) error {
	if headered, ok := w.(http.ResponseWriter); ok {
		headered.Header().Set("Cache-Control", "no-cache")
		headered.Header().Set("Content-type", "application/json")
	}
	err := json.NewEncoder(w).Encode(i)
	if err != nil {
		http.Error(w.(http.ResponseWriter), err.Error(), 500)
	}
	return err
}
