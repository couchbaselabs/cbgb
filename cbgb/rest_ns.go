package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"

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

func getNSNodeList(host string) []couchbase.Node {
	return []couchbase.Node{
		couchbase.Node{
			ClusterCompatibility: 131072,
			ClusterMembership:    "active",
			CouchAPIBase:         "http://" + host + "/",
			Hostname:             host,
			Ports:                map[string]int{"direct": 11211},
			Status:               "healthy",
			Version:              "1.0.0-cbgb",
		},
	}
}

func restNSPoolsDefault(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, map[string]interface{}{
		"buckets": map[string]interface{}{"uri": "/pools/default/buckets"},
		"name":    "default",
		"nodes":   getNSNodeList(r.Host),
		"stats":   map[string]interface{}{"uri": "/pools/default/stats"},
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

func getNSBucket(host, bucketName string) (*couchbase.Bucket, error) {
	b := buckets.Get(bucketName)
	if b == nil {
		return nil, fmt.Errorf("No such bucket: %v", bucketName)
	}
	rv := &couchbase.Bucket{
		AuthType:     "sasl",
		Capabilities: []string{"couchapi"},
		Type:         "membase",
		Name:         bucketName,
		NodeLocator:  "vbucket",
		Nodes:        getNSNodeList(host),
		Replicas:     1,
		URI:          "/pools/default/buckets/" + bucketName,
	}
	rv.VBucketServerMap.HashAlgorithm = "CRC"
	rv.VBucketServerMap.NumReplicas = 1
	rv.VBucketServerMap.ServerList = []string{getBindAddress(host)}
	rv.VBucketServerMap.VBucketMap = [][]int{{0}}
	return rv, nil
}

func restNSBucket(w http.ResponseWriter, r *http.Request) {
	b, err := getNSBucket(r.Host, mux.Vars(r)["bucketname"])
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	jsonEncode(w, &b)
}

func restNSBucketList(w http.ResponseWriter, r *http.Request) {
	rv := []*couchbase.Bucket{}

	for _, bn := range buckets.GetNames() {
		b, err := getNSBucket(r.Host, bn)
		if err != nil {
			http.Error(w, err.Error(), 404)
			return
		}
		rv = append(rv, b)
	}
	jsonEncode(w, &rv)
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
		"/pools/default/stats",
		"/poolsStreaming",
	}

	// Init the 501s from above
	for _, p := range ns_server_paths {
		r.HandleFunc(p, notImplemented).Methods("GET")
	}

	r.HandleFunc("/pools", restNSPools)
	r.HandleFunc("/pools/default", restNSPoolsDefault)
	r.HandleFunc("/pools/default/buckets/{bucketname}", restNSBucket)
	r.HandleFunc("/pools/default/buckets", restNSBucketList)
}
