package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/couchbaselabs/cbgb"
	"github.com/gorilla/mux"
)

func restMain(rest string, staticPath string) {
	r := mux.NewRouter()
	r.HandleFunc("/api/buckets",
		restGetBuckets).Methods("GET")
	r.HandleFunc("/api/buckets/{bucketName}",
		restGetBucket).Methods("GET")
	r.HandleFunc("/api/buckets/{bucketName}/stats",
		restGetBucketStats).Methods("GET")
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
