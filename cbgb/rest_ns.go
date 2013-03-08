package main

import (
	"log"
	"net/http"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/gorilla/mux"
)

var toplevelPool = couchbase.Pools{
	ImplementationVersion: "1.0-cbgb",
	IsAdmin:               false,
	UUID:                  "abc",
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

func restNSPools(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, &toplevelPool)
}

func restNSPoolsDefault(w http.ResponseWriter, r *http.Request) {
	nodeList := []couchbase.Node{
		couchbase.Node{
			ClusterCompatibility: 131072,
			ClusterMembership:    "active",
			CouchAPIBase:         "http://localhost:8077/", // XXX: FIXTERMINATE
			Hostname:             "127.0.0.1:8091",         // XXX: FIXTERMINATE
			Ports:                map[string]int{"direct": 11211},
			Status:               "healthy",
			Version:              "1.0.0-cbgb",
		},
	}
	jsonEncode(w, map[string]interface{}{
		"buckets": map[string]interface{}{"uri": "/pools/default/buckets"},
		"name":    "default",
		"nodes":   nodeList,
		"stats":   map[string]interface{}{"uri": "/pools/default/stats"},
	})
}

func restNSAPI(r *mux.Router) {

	ns_server_paths := []string{
		"/pools/default/buckets/{bucketname}/statsDirectory",
		"/pools/default/buckets/{bucketname}/stats",
		"/pools/default/buckets/{bucketname}/nodes",
		"/pools/default/buckets/{bucketname}/nodes/{node}/stats",
		"/pools/default/buckets/{bucketname}/ddocs",
		"/pools/default/buckets/{bucketname}/localRandomKey",
		"/pools/default/bucketsStreaming/{bucketname}",
		"/pools/default/buckets/{bucketname}",
		"/pools/default/stats",
		"/pools/default/buckets",
		"/poolsStreaming",
	}

	// Init the 501s from above
	for _, p := range ns_server_paths {
		r.HandleFunc(p, notImplemented).Methods("GET")
	}

	r.HandleFunc("/pools", restNSPools)
	r.HandleFunc("/pools/default", restNSPoolsDefault)
}
