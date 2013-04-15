package main

import (
	"flag"
	"fmt"
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

var bucketStatsSnapshotStart time.Duration = time.Second * 30
var bucketStatsSnapshotDelay time.Duration = time.Millisecond * 2100

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

	r.PathPrefix("/_api/").HandlerFunc(authError)
}

func initStatic(r *mux.Router, staticPrefix, staticPath string) error {
	if strings.HasPrefix(staticPath, "http://") {
		zs, err := zipStatic(staticPath)
		if err != nil {
			return err
		}
		r.PathPrefix(staticPrefix).Handler(
			http.StripPrefix(staticPrefix, zs))
	} else {
		r.PathPrefix(staticPrefix).Handler(
			http.StripPrefix(staticPrefix,
				http.FileServer(http.Dir(staticPath))))
	}
	return nil
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
	if time.Since(st.LatestUpdateTime()) > bucketStatsSnapshotStart {
		bucket.StartStats(time.Second)
		// Go ahead and let this delay slightly to catch up
		// the stats.
		time.Sleep(bucketStatsSnapshotDelay)
		st = bucket.SnapshotStats()
	}
	jsonEncode(w, st.ToMap())
}

// To start a cpu profiling...
//    curl -X POST http://127.0.0.1:8091/_api/profile/cpu -d secs=5
// To analyze a profiling...
//    go tool pprof ./cbgb/cbgb run-cpu.pprof
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
		<-time.After(time.Duration(secs) * time.Second)
		pprof.StopCPUProfile()
		f.Close()
	}()
}

// To grab a memory profiling...
//    curl -X POST http://127.0.0.1:8091/_api/profile/memory
// To analyze a profiling...
//    go tool pprof ./cbgb/cbgb run-memory.pprof
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
