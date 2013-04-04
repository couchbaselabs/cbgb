package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/daaku/go.flagbytes"
)

var mutationLogCh = make(chan interface{}, 1024)

var startTime = time.Now()

var verbose = flag.Bool("v", true, "amount of logging")
var addr = flag.String("addr", ":11211", "data protocol listen address")
var data = flag.String("data", "./tmp", "data directory")
var restCouch = flag.String("rest-couch",
	":8092", "rest couch protocol listen address")
var restNS = flag.String("rest-ns",
	":8091", "rest NS protocol listen address")
var staticPath = flag.String("static-path", "http://cbgb.io/static.zip",
	"path to static web UI content")
var defaultBucketName = flag.String("default-bucket-name",
	DEFAULT_BUCKET_NAME, `name of the default bucket ("" disables)`)
var numPartitions = flag.Int("num-partitions",
	1, "default number of partitions for new buckets")
var defaultQuotaBytes = flagbytes.Bytes("default-quota",
	"100MB", "quota for default bucket")
var defaultPersistence = flag.Int("default-persistence",
	2, "persistence level for default bucket")
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

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			"usage: %s <flags>\n",
			os.Args[0])
		fmt.Fprintf(os.Stderr, "\nflags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\npersistence levels:\n")
		fmt.Fprintf(os.Stderr, "  2: metadata persisted and ops persisted\n")
		fmt.Fprintf(os.Stderr, "  1: metadata persisted and ops not persisted\n")
		fmt.Fprintf(os.Stderr, "  0: nothing persisted\n")
		os.Exit(1)
	}
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
	flag.Parse()
	args := flag.Args()

	initPeriodically()

	if !*verbose {
		log.SetOutput(ioutil.Discard)
	}

	log.Printf("cbgb - %v", VERSION)
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("  %v=%v", f.Name, f.Value)
	})
	log.Printf("  %v", args)

	go MutationLogger(mutationLogCh)

	var err error

	bucketSettings = &BucketSettings{
		NumPartitions: *numPartitions,
		QuotaBytes:    int64(*defaultQuotaBytes),
		MemoryOnly:    MemoryOnly_LEVEL_PERSIST_NOTHING - *defaultPersistence,
	}
	buckets, err = NewBuckets(*data, bucketSettings)
	if err != nil {
		log.Fatalf("error: could not make buckets: %v, data directory: %v", err, *data)
	}

	log.Printf("loading buckets from: %v", *data)
	err = buckets.Load(false)
	if err != nil {
		log.Fatalf("error: could not load buckets: %v, data directory: %v", err, *data)
	}

	mainServer(buckets, *defaultBucketName, *addr, *restCouch, *restNS, *staticPath)
}

func mainServer(buckets *Buckets, defaultBucketName string,
	addr string, restCouch string, restNS string, staticPath string) {
	if buckets.Get(defaultBucketName) == nil && defaultBucketName != "" {
		_, err := createBucket(defaultBucketName, bucketSettings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: could not create default bucket: %s, err: %v",
				defaultBucketName, err)
			os.Exit(1)
		}
	}

	log.Printf("listening data on: %v", addr)
	if _, err := StartServer(addr, buckets, defaultBucketName); err != nil {
		fmt.Fprintf(os.Stderr, "error: could not start server: %v\n", err)
		os.Exit(1)
	}

	if restCouch != "" {
		go restCouchServe(restCouch, staticPath)
	}
	if restNS != "" {
		go restNSServe(restNS, staticPath)
	}

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
