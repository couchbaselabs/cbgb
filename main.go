package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/daaku/go.flagbytes"
)

var mutationLogCh = make(chan interface{}, 1024)

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
var statAggFreq = flag.Duration("stat-agg-freq", time.Second*10,
	"Stat aggregation frequency")
var statAggPassFreq = flag.Duration("stat-agg-pass-freq", time.Minute*5,
	"Stat aggregation passivation frequency")

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
	bucketCloser = newPeriodically(*quiesceFreq, 1)
	expirerPeriod = newPeriodically(*expireFreq, 2)
	persistRunner = newPeriodically(*persistFreq, 5)
	viewsRefresher = newPeriodically(*viewRefreshFreq, 5)
	statAggPeriodic = newPeriodically(*statAggFreq, 10)
	statAggPassivator = newPeriodically(*statAggPassFreq, 10)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	initPeriodically()

	if !*verbose {
		log.SetOutput(ioutil.Discard)
	}

	log.Printf("cbgb - version %v", VERSION)

	go MutationLogger(mutationLogCh)

	bucketSettings = &BucketSettings{
		NumPartitions: *defaultNumPartitions,
		QuotaBytes:    int64(*defaultQuotaBytes),
		MemoryOnly:    MemoryOnly_LEVEL_PERSIST_NOTHING - *defaultPersistence,
	}
	buckets, err := NewBuckets(*data, bucketSettings)
	if err != nil {
		log.Fatalf("error: could not make buckets: %v, data directory: %v", err, *data)
	}

	log.Printf("loading buckets from: %v", *data)
	if err = buckets.Load(false); err != nil {
		log.Fatalf("error: could not load buckets: %v, data directory: %v", err, *data)
	}

	mainServer(buckets, *defaultBucketName, *addr, *restCouch, *restNS,
		*staticPath, filepath.Join(*data, ".staticCache"))
}

func mainServer(buckets *Buckets, defaultBucketName string,
	addr string, restCouch string, restNS string,
	staticPath string, staticCachePath string) {
	if buckets.Get(defaultBucketName) == nil && defaultBucketName != "" {
		_, err := createBucket(defaultBucketName, bucketSettings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: could not create default bucket: %s, err: %v",
				defaultBucketName, err)
			os.Exit(1)
		}
	}

	if _, err := StartServer(addr, buckets, defaultBucketName); err != nil {
		fmt.Fprintf(os.Stderr, "error: could not start server: %v\n", err)
		os.Exit(1)
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

	// Let goroutines do their work.
	select {}
}

func createBucket(bucketName string, bucketSettings *BucketSettings) (
	Bucket, error) {
	log.Printf("creating bucket: %v, numPartitions: %v",
		bucketName, bucketSettings.NumPartitions)

	bucket, err := buckets.New(bucketName, bucketSettings)
	if err != nil {
		return nil, err
	}

	bucket.Subscribe(mutationLogCh)

	for vbid := 0; vbid < bucketSettings.NumPartitions; vbid++ {
		bucket.CreateVBucket(uint16(vbid))
		bucket.SetVBState(uint16(vbid), VBActive)
	}

	if err = bucket.Flush(); err != nil {
		return nil, err
	}

	return bucket, nil
}
