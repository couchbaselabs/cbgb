package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/couchbaselabs/cbgb"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/gorilla/mux"
)

var toplevelPool = couchbase.Pools{
	ImplementationVersion: cbgb.VERSION + "-cbgb",
	IsAdmin:               true, // TODO: Need real auth.
	UUID:                  cbgb.CreateNewUUID(),
	Pools: []couchbase.RestPool{
		{
			Name:         "default",
			StreamingURI: "/poolsStreaming/default",
			URI:          "/pools/default",
		},
	}}

func notImplemented(w http.ResponseWriter, r *http.Request) {
	log.Printf("Request for %v:%v", r.Method, r.URL.Path)
	http.Error(w, "Not implemented", 501)
}

func restNSVersion(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, map[string]interface{}{
		"implementationVersion": cbgb.VERSION + "-cbgb",
	})
}

func restNSPools(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, &toplevelPool)
}

func getNSNodeList(host, bucket string) []couchbase.Node {
	port, err := strconv.Atoi((*addr)[strings.LastIndex(*addr, ":")+1:])
	if err != nil {
		log.Fatalf("Unable to determine port to advertise")
	}
	couchApiBaseHost := strings.Split(host, ":")[0]
	couchApiBasePort := strings.Split(*rest, ":")[1]
	couchApiBase := couchApiBaseHost + ":" + couchApiBasePort + "/" + bucket
	return []couchbase.Node{
		couchbase.Node{
			ClusterCompatibility: 131072,
			ClusterMembership:    "active",
			CouchAPIBase:         "http://" + couchApiBase,
			Hostname:             host,
			Ports: map[string]int{
				"direct": port,
				"proxy":  0,
			},
			Status:  "healthy",
			Version: cbgb.VERSION + "-cbgb",
		},
	}
}

func restNSPoolsDefault(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, map[string]interface{}{
		"buckets": map[string]interface{}{
			"uri": "/pools/default/buckets",
		},
		"name":  "default",
		"nodes": getNSNodeList(r.Host, ""),
		"stats": map[string]interface{}{"uri": "/pools/default/stats"},
	})
}

func getBindAddress(host string) string {
	if strings.Index(*addr, ":") > 0 {
		return *addr
	}
	n, _, err := net.SplitHostPort(host)
	if err != nil {
		return *addr
	}
	return n + *addr
}

func getNSBucket(host, bucketName, uuid string) (*couchbase.Bucket, error) {
	b := buckets.Get(bucketName)
	if b == nil {
		return nil, fmt.Errorf("No such bucket: %v", bucketName)
	}
	bs := b.GetBucketSettings()
	bucketUUID := bs.UUID
	if uuid != "" && uuid != bucketUUID {
		return nil, fmt.Errorf("Bucket uuid does not match the requested.")
	}
	rv := &couchbase.Bucket{
		AuthType:     "sasl",
		Capabilities: []string{"couchapi"},
		Type:         "membase",
		Name:         bucketName,
		NodeLocator:  "vbucket",
		Nodes:        getNSNodeList(host, bucketName),
		Replicas:     1,
		URI:          "/pools/default/buckets/" + bucketName + "?bucket_uuid=" + bucketUUID,
		StreamingURI: "/poolsStreaming/default/buckets/" + bucketName,
		UUID:         bucketUUID,
	}
	// TODO: Perhaps dynamically generate a SASL password here, such
	// based on server start time.
	if bs.PasswordHashFunc == "" && bs.PasswordSalt == "" {
		rv.Password = bs.PasswordHash // The json saslPassword field.
	}
	rv.VBucketServerMap.HashAlgorithm = "CRC"
	rv.VBucketServerMap.NumReplicas = 1
	rv.VBucketServerMap.ServerList = []string{getBindAddress(host)}

	np := bs.NumPartitions
	rv.VBucketServerMap.VBucketMap = make([][]int, np)
	for i := 0; i < np; i++ {
		rv.VBucketServerMap.VBucketMap[i] = []int{0, -1}
	}
	return rv, nil
}

func getNSBucketDDocs(host, bucketName, uuid string) (interface{}, error) {
	b := buckets.Get(bucketName)
	if b == nil {
		return nil, fmt.Errorf("No such bucket: %v", bucketName)
	}
	bucketUUID := b.GetBucketSettings().UUID
	if uuid != "" && uuid != bucketUUID {
		return nil, fmt.Errorf("Bucket uuid does not match the requested.")
	}
	rows := make([]interface{}, 0)
	var errVisit, errJson error
	errVisit = b.VisitDDocs(nil, func(key []byte, data []byte) bool {
		var j interface{}
		errJson = json.Unmarshal(data, &j)
		if errJson != nil {
			return false
		}
		rows = append(rows,
			map[string]interface{}{
				"doc": map[string]interface{}{
					"json": j,
					"meta": map[string]interface{}{
						"id": string(key),
						// TODO: "rev" meta field.
					},
				},
			})
		return true
	})
	if errVisit != nil {
		return nil, fmt.Errorf("VisitDDocs err: %v", errVisit)
	}
	if errJson != nil {
		return nil, fmt.Errorf("json parse err: %v", errJson)
	}
	rv := map[string]interface{}{}
	rv["rows"] = rows
	return rv, nil
}

func restNSBucket(w http.ResponseWriter, r *http.Request) {
	b, err := getNSBucket(r.Host, mux.Vars(r)["bucketname"],
		r.FormValue("bucket_uuid"))
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	jsonEncode(w, &b)
}

// Wraps any REST response to make a "streaming" version.
func restNSStreaming(orig func(http.ResponseWriter,
	*http.Request),) func(http.ResponseWriter, *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Not flushable", 500)
			log.Printf("Can't flush %v", w)
			return
		}

		for {
			orig(w, r)
			f.Flush()
			_, err := w.Write([]byte("\n\n\n\n"))
			if err != nil {
				log.Printf("Error sending streaming result: %v", err)
				return
			}
			f.Flush()
			time.Sleep(time.Second * 30)
		}
	}
}

func restNSBucketDDocs(w http.ResponseWriter, r *http.Request) {
	rows, err := getNSBucketDDocs(r.Host, mux.Vars(r)["bucketname"],
		r.FormValue("bucket_uuid"))
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	jsonEncode(w, &rows)
}

func restNSBucketList(w http.ResponseWriter, r *http.Request) {
	rv := []*couchbase.Bucket{}

	for _, bn := range buckets.GetNames() {
		b, err := getNSBucket(r.Host, bn, "")
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		rv = append(rv, b)
	}
	jsonEncode(w, &rv)
}

func restNSSettingsStats(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, map[string]interface{}{"sendStats": false})
}

func restNSAPI(r *mux.Router) {
	ns_server_paths := []string{
		"/pools/default/buckets/{bucketname}/statsDirectory",
		"/pools/default/buckets/{bucketname}/stats",
		"/pools/default/buckets/{bucketname}/nodes",
		"/pools/default/buckets/{bucketname}/nodes/{node}/stats",
		"/pools/default/buckets/{bucketname}/localRandomKey",
		"/pools/default/bucketsStreaming/{bucketname}",
		"/pools/default/stats",
		"/poolsStreaming",
	}

	// Init the 501s from above
	for _, p := range ns_server_paths {
		r.HandleFunc(p, notImplemented).Methods("GET")
	}

	r.HandleFunc("/versions", restNSVersion)
	r.HandleFunc("/pools", restNSPools)
	r.HandleFunc("/pools/default", restNSPoolsDefault)
	r.HandleFunc("/pools/default/buckets/{bucketname}", restNSBucket)
	r.HandleFunc("/pools/default/buckets", restNSBucketList)
	r.HandleFunc("/pools/default/buckets/{bucketname}/ddocs", restNSBucketDDocs)
	r.HandleFunc("/poolsStreaming/default", restNSStreaming(restNSPoolsDefault))
	r.HandleFunc("/poolsStreaming/default/buckets/{bucketname}",
		restNSStreaming(restNSBucket))
	r.HandleFunc("/settings/stats", restNSSettingsStats)
}

func restNSServe(restNS string, staticPathNS string) {
	r := mux.NewRouter()
	restNSAPI(r)
	initStatic(r, "/", staticPathNS)
	log.Printf("listening rest-ns on: %v", restNS)
	log.Fatal(http.ListenAndServe(restNS, r))
}
