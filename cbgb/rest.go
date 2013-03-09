package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/couchbaselabs/cbgb"
	"github.com/gorilla/mux"
)

func restMain(rest string, staticPath string) {
	r := mux.NewRouter()
	restAPI(r, staticPath)
	restNSAPI(r)
	restCouchAPI(r)
	r.Handle("/",
		http.RedirectHandler("/_static/app.html", 302))
	log.Printf("listening rest on: %v", rest)
	log.Fatal(http.ListenAndServe(rest, r))
}

func restAPI(r *mux.Router, staticPath string) {
	r.HandleFunc("/_api/buckets",
		restGetBuckets).Methods("GET")
	r.HandleFunc("/_api/buckets",
		restPostBucket).Methods("POST")
	r.HandleFunc("/_api/buckets/{bucketName}",
		restGetBucket).Methods("GET")
	r.HandleFunc("/_api/buckets/{bucketName}",
		restDeleteBucket).Methods("DELETE")
	r.HandleFunc("/_api/buckets/{bucketName}/compact",
		restPostBucketCompact).Methods("POST")
	r.HandleFunc("/_api/buckets/{bucketName}/flushDirty",
		restPostBucketFlushDirty).Methods("POST")
	r.HandleFunc("/_api/buckets/{bucketName}/stats",
		restGetBucketStats).Methods("GET")
	r.HandleFunc("/_api/profile/cpu",
		restProfileCPU).Methods("POST")
	r.HandleFunc("/_api/profile/memory",
		restProfileMemory).Methods("POST")
	r.HandleFunc("/_api/runtime",
		restGetRuntime).Methods("GET")
	r.HandleFunc("/_api/runtime/memStats",
		restGetRuntimeMemStats).Methods("GET")
	r.HandleFunc("/_api/runtime/gc",
		restPostRuntimeGC).Methods("POST")
	r.HandleFunc("/_api/settings",
		restGetSettings).Methods("GET")
	r.PathPrefix("/_static/").Handler(
		http.StripPrefix("/_static/",
			http.FileServer(http.Dir(staticPath))))
}

func restGetSettings(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, map[string]interface{}{
		"addr":              *addr,
		"data":              *data,
		"rest":              *rest,
		"defaultBucketName": *defaultBucketName,
		"bucketSettings":    bucketSettings,
	})
}

func restGetBuckets(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, buckets.GetNames())
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

	bSettings.QuotaBytes = getIntValue(r, "quota", bucketSettings.QuotaBytes)

	bSettings.MemoryOnly = int(getIntValue(r, "memoryOnly",
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
	bucketName, bucket := parseBucketName(w, r)
	if bucket == nil {
		return
	}
	partitions := map[string]interface{}{}
	settings := bucket.GetBucketSettings()
	for vbid := uint16(0); vbid < uint16(settings.NumPartitions); vbid++ {
		vb := bucket.GetVBucket(vbid)
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
	bucketName, bucket := parseBucketName(w, r)
	if bucket == nil {
		return
	}
	buckets.Close(bucketName, true)
}

func restPostBucketCompact(w http.ResponseWriter, r *http.Request) {
	bucketName, bucket := parseBucketName(w, r)
	if bucket == nil {
		return
	}
	if err := bucket.Compact(); err != nil {
		http.Error(w, fmt.Sprintf("error compacting bucket: %v, err: %v",
			bucketName, err), 500)
	}
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
	st := bucket.GetStats()
	if time.Since(st.LatestUpdate) > time.Second*30 {
		bucket.StartStats(time.Second)
		// Go ahead and let this delay slightly to catch up
		// the stats.
		time.Sleep(time.Millisecond * 2100)
		st = bucket.GetStats()
	}
	jsonEncode(w, map[string]interface{}{
		"totals": map[string]interface{}{
			"bucketStats":      st.Current,
			"bucketStoreStats": st.BucketStore,
		},
		"diffs": map[string]interface{}{
			"bucketStats":      st.Agg,
			"bucketStoreStats": st.AggBucketStore,
		},
		"levels": cbgb.AggStatsLevels,
	})
}

// To start a cpu profiling...
//    curl -X POST http://127.0.0.1:8077/_api/profile/cpu -d secs=5
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
//    curl -X POST http://127.0.0.1:8077/_api/profile/memory
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
