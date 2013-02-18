package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/couchbaselabs/cbgb"
	"github.com/gorilla/mux"
)

func restMain(rest string, staticPath string) {
	r := mux.NewRouter()
	r.HandleFunc("/api/buckets",
		restGetBuckets).Methods("GET")
	r.HandleFunc("/api/buckets",
		restPostBucket).Methods("POST")
	r.HandleFunc("/api/buckets/{bucketName}",
		restGetBucket).Methods("GET")
	r.HandleFunc("/api/buckets/{bucketName}",
		restDeleteBucket).Methods("DELETE")
	r.HandleFunc("/api/buckets/{bucketName}/flushDirty",
		restPostBucketFlushDirty).Methods("POST")
	r.HandleFunc("/api/buckets/{bucketName}/stats",
		restGetBucketStats).Methods("GET")
	r.HandleFunc("/api/profile",
		restProfile).Methods("POST")
	r.HandleFunc("/api/settings",
		restGetSettings).Methods("GET")
	r.PathPrefix("/static/").Handler(
		http.StripPrefix("/static/",
			http.FileServer(http.Dir(staticPath))))
	r.Handle("/",
		http.RedirectHandler("/static/app.html", 302))
	log.Printf("listening rest on: %v", rest)
	log.Fatal(http.ListenAndServe(rest, r))
}

func restGetSettings(w http.ResponseWriter, r *http.Request) {
	mustEncode(w, map[string]interface{}{
		"startTime":         startTime,
		"addr":              *addr,
		"data":              *data,
		"rest":              *rest,
		"defaultBucketName": *defaultBucketName,
		"bucketSettings":    bucketSettings,
	})
}

func restGetBuckets(w http.ResponseWriter, r *http.Request) {
	mustEncode(w, buckets.GetNames())
}

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

func restPostBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := r.FormValue("bucketName")
	if len(bucketName) < 1 {
		http.Error(w, "bucket name is too short or is missing", 400)
		return
	}
	match, err := regexp.MatchString("^[A-Za-z0-9\\-_]+$", bucketName)
	if err != nil || !match {
		http.Error(w,
			fmt.Sprintf("illegal bucket name: %v, err: %v", bucketName, err), 400)
		return
	}
	_, err = createBucket(bucketName, *defaultPartitions)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("create bucket error; name: %v, err: %v", bucketName, err), 500)
		return
	}
	http.Redirect(w, r, "/api/buckets/"+bucketName, 303)
}

func restGetBucket(w http.ResponseWriter, r *http.Request) {
	bucketName, bucket := parseBucketName(w, r)
	if bucket == nil {
		return
	}
	partitions := map[string]interface{}{}
	for vbid := uint16(0); vbid < uint16(cbgb.MAX_VBUCKETS); vbid++ {
		vb := bucket.GetVBucket(vbid)
		if vb != nil {
			partitions[strconv.Itoa(int(vbid))] = vb.Meta()
		}
	}
	mustEncode(w, map[string]interface{}{
		"name":       bucketName,
		"partitions": partitions,
	})
}

func restDeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucketName, bucket := parseBucketName(w, r)
	if bucket == nil {
		return
	}
	buckets.Close(bucketName, true)
}

func restPostBucketFlushDirty(w http.ResponseWriter, r *http.Request) {
	bucketName, bucket := parseBucketName(w, r)
	if bucket == nil {
		return
	}
	if err := bucket.Flush(); err != nil {
		http.Error(w, fmt.Sprintf("error flushing bucket: %v, err: %v",
			bucketName, err), 500)
	}
}

func restGetBucketStats(w http.ResponseWriter, r *http.Request) {
	_, bucket := parseBucketName(w, r)
	if bucket == nil {
		return
	}
	buckets.StatsApply(func() {
		mustEncode(w, map[string]interface{}{
			"totals": map[string]interface{}{
				"bucketStats":      bucket.GetLastStats(),
				"bucketStoreStats": bucket.GetLastBucketStoreStats(),
			},
			"diffs": map[string]interface{}{
				"bucketStats":      bucket.GetAggStats(),
				"bucketStoreStats": bucket.GetAggBucketStoreStats(),
			},
			"levels": cbgb.AggStatsLevels,
		})
	})
}

// To start a profiling...
//    curl -X POST http://127.0.0.1:8077/api/profile -d secs=5
// To analyze a profiling...
//    go tool pprof ./cbgb/cbgb run.pprof
func restProfile(w http.ResponseWriter, r *http.Request) {
	fname := "./run.pprof"
	secs, err := strconv.Atoi(r.FormValue("secs"))
	if err != nil || secs <= 0 {
		http.Error(w, "incorrect or missing secs parameter", 400)
		return
	}
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		http.Error(w, fmt.Sprintf("couldn't create file: %v, err: %v",
			fname, err), 500)
		return
	}

	pprof.StartCPUProfile(f)
	go func() {
		<-time.After(time.Duration(secs) * time.Second)
		pprof.StopCPUProfile()
		f.Close()
	}()
}

func mustEncode(w io.Writer, i interface{}) {
	if headered, ok := w.(http.ResponseWriter); ok {
		headered.Header().Set("Cache-Control", "no-cache")
		headered.Header().Set("Content-type", "application/json")
	}
	e := json.NewEncoder(w)
	if err := e.Encode(i); err != nil {
		panic(err)
	}
}
