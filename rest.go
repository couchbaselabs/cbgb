// Copyright (c) 2013 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License. You
// may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

var statsSnapshotStart time.Duration = time.Second * 30
var statsSnapshotDelay time.Duration = time.Millisecond * 2100

func restAPI(r *mux.Router) {
	sr := r.PathPrefix("/_api/").Subrouter()
	sr.HandleFunc("/buckets",
		restGetBuckets).Methods("GET")
	sr.HandleFunc("/buckets/{bucketname}",
		withBucketAccess(restGetBucket)).Methods("GET")
	sr.HandleFunc("/buckets/{bucketname}",
		withBucketAccess(restDeleteBucket)).Methods("DELETE")
	sr.HandleFunc("/buckets/{bucketname}/compact",
		withBucketAccess(restPostBucketCompact)).Methods("POST")
	sr.HandleFunc("/buckets/{bucketname}/flushDirty",
		withBucketAccess(restPostBucketFlushDirty)).Methods("POST")
	sr.HandleFunc("/buckets/{bucketname}/stats",
		withBucketAccess(restGetBucketStats)).Methods("GET")
	sr.HandleFunc("/buckets/{bucketname}/errs",
		withBucketAccess(restGetBucketErrs)).Methods("GET")
	sr.HandleFunc("/buckets/{bucketname}/logs",
		withBucketAccess(restGetBucketLogs)).Methods("GET")

	sra := r.PathPrefix("/_api/").MatcherFunc(adminRequired).Subrouter()
	sra.HandleFunc("/buckets", restPostBucket).Methods("POST")
	sra.HandleFunc("/bucketsRescan", restPostBucketsRescan).Methods("POST")
	sra.HandleFunc("/bucketPath", restGetBucketPath).Methods("GET")
	sra.HandleFunc("/profile/cpu", restProfileCPU).Methods("POST")
	sra.HandleFunc("/profile/memory", restProfileMemory).Methods("POST")
	sra.HandleFunc("/runtime", restGetRuntime).Methods("GET")
	sra.HandleFunc("/runtime/memStats", restGetRuntimeMemStats).Methods("GET")
	sra.HandleFunc("/runtime/gc", restPostRuntimeGC).Methods("POST")
	sra.HandleFunc("/settings", restGetSettings).Methods("GET")
	sra.HandleFunc("/stats", restGetStats).Methods("GET")

	r.PathPrefix("/_api/").HandlerFunc(authError)
}

func initStatic(r *mux.Router,
	staticPrefix, staticPath, staticCachePath string) error {
	if strings.HasPrefix(staticPath, "http://") {
		zs, err := zipStatic(staticPath, staticCachePath)
		if err == nil {
			r.PathPrefix(staticPrefix).Handler(
				http.StripPrefix(staticPrefix, zs))
		} else {
			log.Printf("Couldn't initialize remote static content: %v", err)
			r.PathPrefix(staticPrefix).Handler(&lastResortHandler{})
		}
	} else {
		r.PathPrefix(staticPrefix).Handler(
			http.StripPrefix(staticPrefix,
				http.FileServer(http.Dir(staticPath))))
	}
	return nil
}

func parseBucketName(w http.ResponseWriter, vars map[string]string) (
	string, Bucket) {
	bucketName, ok := vars["bucketname"]
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

// For settings that are constant throughout server process lifetime.
func restGetSettings(w http.ResponseWriter, r *http.Request) {
	m := map[string]interface{}{}
	flag.VisitAll(func(f *flag.Flag) {
		if f.Name != "adminUser" && f.Name != "adminPass" {
			m[f.Name] = f.Value
		}
	})
	jsonEncode(w, m)
}

func restGetStats(w http.ResponseWriter, r *http.Request) {
	st := snapshotServerStats()
	if time.Since(st.LatestUpdateTime()) > statsSnapshotStart {
		statAggPeriodic.Register(serverStatsAvailableCh, sampleServerStats)
		// Delay slightly to catch up the stats.
		time.Sleep(statsSnapshotDelay)
		st = snapshotServerStats()
	}
	jsonEncode(w, st.ToMap())
}

func restGetBuckets(w http.ResponseWriter, r *http.Request) {
	u := currentUser(r)
	if string(u) == "" && !u.isAdmin() {
		authError(w, r)
		return
	}
	bn := []string{}
	for _, n := range buckets.GetNames() {
		if u.canAccess(n) {
			bn = append(bn, n)
		}
	}
	jsonEncode(w, bn)
}

func restPostBucketsRescan(w http.ResponseWriter, r *http.Request) {
	err := buckets.Load(true)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("rescanning/reloading buckets directory err: %v", err), 500)
		return
	}
	http.Redirect(w, r, "/_api/buckets", 303)
}

// Computes/hashes the bucket's subdir given a bucket name...
//    curl http://127.0.0.1:8091/_api/bucketPath?name=default
func restGetBucketPath(w http.ResponseWriter, r *http.Request) {
	bucketName := r.FormValue("name")
	if len(bucketName) < 1 {
		http.Error(w, "bucket name is too short or is missing", 400)
		return
	}
	path, err := BucketPath(bucketName)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("could not compute BucketPath for name: %v, err: %v",
				bucketName, err), 400)
		return
	}
	w.Write([]byte(path))
}

func restPostBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := r.FormValue("name")
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

	bSettings := bucketSettings.Copy()
	bucketPassword := r.FormValue("password")
	if bucketPassword != "" {
		bSettings.PasswordHash = bucketPassword
	}
	bSettings.QuotaBytes = getIntValue(r.Form, "quotaBytes",
		bucketSettings.QuotaBytes)
	bSettings.MemoryOnly = int(getIntValue(r.Form, "memoryOnly",
		int64(bucketSettings.MemoryOnly)))

	_, err = createBucket(bucketName, bSettings)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("create bucket error; name: %v, err: %v", bucketName, err), 500)
		return
	}
	http.Redirect(w, r, "/_api/buckets/"+bucketName, 303)
}

func restGetBucket(w http.ResponseWriter, r *http.Request) {
	bucketName, bucket := parseBucketName(w, mux.Vars(r))
	if bucket == nil {
		return
	}
	partitions := map[string]interface{}{}
	settings := bucket.GetBucketSettings()
	for vbid := uint16(0); vbid < uint16(settings.NumPartitions); vbid++ {
		vb, _ := bucket.GetVBucket(vbid)
		if vb != nil {
			vbm := vb.Meta()
			if vbm != nil {
				partitions[strconv.Itoa(int(vbm.Id))] = vbm
			}
		}
	}
	vb := bucket.GetDDocVBucket()
	if vb != nil {
		vbm := vb.Meta()
		if vbm != nil {
			partitions[strconv.Itoa(int(vbm.Id))] = vbm
		}
	}
	jsonEncode(w, map[string]interface{}{
		"name":       bucketName,
		"itemBytes":  bucket.GetItemBytes(),
		"settings":   settings.SafeView(),
		"partitions": partitions,
	})
}

func restDeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucketName, bucket := parseBucketName(w, mux.Vars(r))
	if bucket == nil {
		return
	}
	err := buckets.Close(bucketName, true)
	if err != nil {
		http.Error(w, fmt.Sprintf("error deleting bucket: %v, err: %v",
			bucketName, err), 400)
	}

	w.WriteHeader(204)
}

func restPostBucketCompact(w http.ResponseWriter, r *http.Request) {
	bucketName, bucket := parseBucketName(w, mux.Vars(r))
	if bucket == nil {
		return
	}
	if err := bucket.Compact(); err != nil {
		http.Error(w, fmt.Sprintf("error compacting bucket: %v, err: %v",
			bucketName, err), 500)
	}
}

func restPostBucketFlushDirty(w http.ResponseWriter, r *http.Request) {
	bucketName, bucket := parseBucketName(w, mux.Vars(r))
	if bucket == nil {
		return
	}
	if err := bucket.Flush(); err != nil {
		http.Error(w, fmt.Sprintf("error flushing bucket: %v, err: %v",
			bucketName, err), 500)
	}
}

func restGetBucketStats(w http.ResponseWriter, r *http.Request) {
	_, bucket := parseBucketName(w, mux.Vars(r))
	if bucket == nil {
		return
	}
	st := bucket.SnapshotStats()
	if time.Since(st.LatestUpdateTime()) > statsSnapshotStart {
		bucket.StartStats(time.Second)
		// Delay slightly to catch up the stats.
		time.Sleep(statsSnapshotDelay)
		st = bucket.SnapshotStats()
	}
	jsonEncode(w, st.ToMap())
}

func restGetBucketErrs(w http.ResponseWriter, r *http.Request) {
	_, bucket := parseBucketName(w, mux.Vars(r))
	if bucket == nil {
		return
	}
	jsonEncode(w, bucket.Errs())
}

func restGetBucketLogs(w http.ResponseWriter, r *http.Request) {
	_, bucket := parseBucketName(w, mux.Vars(r))
	if bucket == nil {
		return
	}
	jsonEncode(w, bucket.Logs())
}

// To start a cpu profiling...
//    curl -X POST http://127.0.0.1:8091/_api/profile/cpu -d secs=5
// To analyze a profiling...
//    go tool pprof ./cbgb run-cpu.pprof
func restProfileCPU(w http.ResponseWriter, r *http.Request) {
	fname := "./run-cpu.pprof"
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
		time.Sleep(time.Duration(secs) * time.Second)
		pprof.StopCPUProfile()
		f.Close()
	}()
}

// To grab a memory profiling...
//    curl -X POST http://127.0.0.1:8091/_api/profile/memory
// To analyze a profiling...
//    go tool pprof ./cbgb run-memory.pprof
func restProfileMemory(w http.ResponseWriter, r *http.Request) {
	fname := "./run-memory.pprof"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		http.Error(w, fmt.Sprintf("couldn't create file: %v, err: %v",
			fname, err), 500)
		return
	}
	defer f.Close()
	pprof.WriteHeapProfile(f)
}

func restGetRuntime(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, map[string]interface{}{
		"version":   VERSION,
		"startTime": startTime,
		"arch":      runtime.GOARCH,
		"os":        runtime.GOOS,
		"numCPU":    runtime.NumCPU(),
		"go": map[string]interface{}{
			"GOMAXPROCS":     runtime.GOMAXPROCS(0),
			"GOROOT":         runtime.GOROOT(),
			"version":        runtime.Version(),
			"numGoroutine":   runtime.NumGoroutine(),
			"numCgoCall":     runtime.NumCgoCall(),
			"compiler":       runtime.Compiler,
			"memProfileRate": runtime.MemProfileRate,
		},
	})
}

func restGetRuntimeMemStats(w http.ResponseWriter, r *http.Request) {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	jsonEncode(w, memStats)
}

func restPostRuntimeGC(w http.ResponseWriter, r *http.Request) {
	runtime.GC()
}
