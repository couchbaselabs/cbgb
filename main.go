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
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/daaku/go.flagbytes"
)

var VERSION = "0.0.0"

var startTime = time.Now()

var verbose = flag.Bool("v", true,
	"Amount of logging")
var addr = flag.String("addr", ":11210",
	"Data protocol listen address")
var data = flag.String("data", "./tmp",
	"Data directory")
var restCouch = flag.String("rest-couch", ":8092",
	"REST couch protocol listen address")
var restNS = flag.String("rest-ns", ":8091",
	"REST NS protocol listen address")
var staticPath = flag.String("static-path", "http://cbgb.io/static.zip",
	"Path to static web UI content")
var defaultBucketName = flag.String("default-bucket-name", DEFAULT_BUCKET_NAME,
	`Name of the default bucket ("" disables)`)
var defaultNumPartitions = flag.Int("default-num-partitions", 1,
	"Number of partitions for default bucket")
var defaultQuotaBytes = flagbytes.Bytes("default-quota", "100MB",
	"Quota for default bucket")
var defaultPersistence = flag.Int("default-persistence", 2,
	"Persistence level for default bucket")
var quiesceFreq = flag.Duration("quiesce-freq", time.Minute*5,
	"Bucket quiescence frequency")
var expireFreq = flag.Duration("expire-freq", time.Minute*5,
	"Expiration scanner frequency")
var persistFreq = flag.Duration("persist-freq", time.Second*5,
	"Persistence frequency")
var viewRefreshFreq = flag.Duration("view-refresh-freq", time.Second*10,
	"View refresh frequency")
var statAggFreq = flag.Duration("stat-agg-freq", time.Second*1,
	"Stat aggregation frequency")
var statAggPassFreq = flag.Duration("stat-agg-pass-freq", time.Minute*5,
	"Stat aggregation passivation frequency")
var maxConns = flag.Int("max-conns", 800,
	"Max number of connections")
var fileServiceWorkers = flag.Int("file-service-workers", 32,
	"Number of file service workers")
var compactEvery = flag.Int("compact-every", 10000,
	"Compact file after this many writes")
var logSyslog = flag.Bool("syslog", false, "Log to syslog")
var logPlain = flag.Bool("log-no-ts", false, "Log without timestamps")

var buckets *Buckets
var bucketSettings *BucketSettings

func usage() {
	fmt.Fprintf(os.Stderr, "cbgb - version %s\n", VERSION)
	fmt.Fprintf(os.Stderr, "\nusage: %s <flags>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nflags:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\npersistence levels:\n")
	fmt.Fprintf(os.Stderr, "  2: metadata persisted and ops persisted\n")
	fmt.Fprintf(os.Stderr, "  1: metadata persisted and ops not persisted\n")
	fmt.Fprintf(os.Stderr, "  0: nothing persisted\n")
}

func initPeriodically() {
	// TODO: The periodically's have # workers that could be configured.
	quiescePeriodic = newPeriodically(*quiesceFreq, 1)
	expirePeriodic = newPeriodically(*expireFreq, 2)
	persistPeriodic = newPeriodically(*persistFreq, 5)
	viewRefreshPeriodic = newPeriodically(*viewRefreshFreq, 5)
	statAggPeriodic = newPeriodically(*statAggFreq, 10)
	statAggPassPeriodic = newPeriodically(*statAggPassFreq, 10)
	fileService = NewFileService(*fileServiceWorkers)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	initLogger(*logSyslog)

	go dumpOnSignalForPlatform()

	initAdmin()
	initPeriodically()

	if !*verbose {
		log.SetOutput(ioutil.Discard)
	}

	log.Printf("cbgb - version %v", VERSION)

	if *adminUser == "" && *adminPass == "" {
		log.Printf("-------------------------------------------------------")
		log.Printf("warning: running openly without any adminUser/adminPass")
		log.Printf("warning: please don't run this way in production usage")
		log.Printf("-------------------------------------------------------")
	}

	bss := &BucketSettings{
		NumPartitions: *defaultNumPartitions,
		QuotaBytes:    int64(*defaultQuotaBytes),
		MemoryOnly:    MemoryOnly_LEVEL_PERSIST_NOTHING - *defaultPersistence,
	}
	bs, err := NewBuckets(*data, bss)
	if err != nil {
		log.Fatalf("error: could not make buckets: %v, data dir: %v", err, *data)
	}
	log.Printf("data directory: %s", *data)

	buckets = bs
	bucketSettings = bss

	defaultEventManager = eventManager{make(chan statusEvent, 20)}
	go defaultEventManager.deliverEvents(*eventUrl)

	mainServer(*defaultBucketName, *addr, *maxConns, *restCouch, *restNS,
		*staticPath, filepath.Join(*data, ".staticCache"))

	// Let goroutines do their work.
	select {}
}

func mainServer(defaultBucketName string, addr string, maxConns int,
	restCouch string, restNS string,
	staticPath string, staticCachePath string) {
	if buckets.Get(defaultBucketName) == nil && defaultBucketName != "" {
		_, err := createBucket(defaultBucketName, bucketSettings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: could not create default bucket: %s, err: %v",
				defaultBucketName, err)
			os.Exit(1)
		}
	}
	if addr != "" {
		_, err := StartServer(addr, maxConns, buckets, defaultBucketName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: could not start server: %v\n", err)
			os.Exit(1)
		}
	}
	log.Printf("primary connections...")
	if restNS != "" {
		go restNSServe(restNS, staticPath, staticCachePath)
		hp := strings.Split(restNS, ":")
		log.Printf("  connect your couchbase client to: http://HOST:%s/pools/default",
			hp[len(hp)-1])
		log.Printf("  web admin U/I available on: http://HOST:%s",
			hp[len(hp)-1])
	}
	log.Printf("secondary connections...")
	if restCouch != "" {
		go restCouchServe(restCouch, staticPath)
		log.Printf("  view listening: %s", restCouch)
	}
	log.Printf("  data listening: %s", addr)
}

func createBucket(bucketName string, bucketSettings *BucketSettings) (
	Bucket, error) {
	log.Printf("creating bucket: %v, numPartitions: %v",
		bucketName, bucketSettings.NumPartitions)

	bucket, err := buckets.New(bucketName, bucketSettings)
	if err != nil {
		return nil, err
	}

	for vbid := 0; vbid < bucketSettings.NumPartitions; vbid++ {
		bucket.CreateVBucket(uint16(vbid))
		bucket.SetVBState(uint16(vbid), VBActive)
	}

	if err = bucket.Flush(); err != nil {
		return nil, err
	}

	return bucket, nil
}

func dumpOnSignal(signals ...os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	for _ = range c {
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
	}
}
