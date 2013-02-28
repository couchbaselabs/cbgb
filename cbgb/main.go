package main

import (
	"flag"
	"log"
	"time"

	"github.com/couchbaselabs/cbgb"
)

var mutationLogCh = make(chan interface{})

var startTime = time.Now()

var addr = flag.String("addr", ":11211", "data protocol listen address")
var data = flag.String("data", "./tmp", "data directory")
var rest = flag.String("rest", ":DISABLED", "rest protocol listen address")
var staticPath = flag.String("static-path",
	"static",
	"path to static content")
var defaultBucketName = flag.String("default-bucket-name",
	cbgb.DEFAULT_BUCKET_NAME,
	"name of the default bucket")
var flushInterval = flag.Duration("flush-interval",
	10*time.Second,
	"duration between flushing or persisting mutations to storage")
var sleepInterval = flag.Duration("sleep-interval",
	2*time.Minute,
	"duration until files are closed (to be reopened on the next request)")
var compactInterval = flag.Duration("compact-interval",
	10*time.Minute,
	"duration until files are compacted")
var purgeTimeout = flag.Duration("purge-timeout",
	10*time.Second,
	"duration until unused files are purged after compaction")
var defaultPartitions = flag.Int("default-partitions",
	1, "default number of partitions for new buckets")

var buckets *cbgb.Buckets
var bucketSettings *cbgb.BucketSettings

func main() {
	flag.Parse()

	log.Printf("cbgb")
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("  %v=%v", f.Name, f.Value)
	})

	go cbgb.MutationLogger(mutationLogCh)

	var err error

	bucketSettings = &cbgb.BucketSettings{
		NumPartitions:   *defaultPartitions,
		FlushInterval:   *flushInterval,
		SleepInterval:   *sleepInterval,
		CompactInterval: *compactInterval,
		PurgeTimeout:    *purgeTimeout,
	}
	buckets, err = cbgb.NewBuckets(*data, bucketSettings)
	if err != nil {
		log.Fatalf("Could not make buckets: %v, data directory: %v", err, *data)
	}

	log.Printf("loading buckets from: %v", *data)
	err = buckets.Load()
	if err != nil {
		log.Printf("Could not load buckets: %v, data directory: %v", err, *data)
	}

	if buckets.Get(*defaultBucketName) == nil {
		_, err := createBucket(*defaultBucketName, bucketSettings)
		if err != nil {
			log.Fatalf("Error creating default bucket: %s, err: %v",
				*defaultBucketName, err)
		}
	}

	log.Printf("listening data on: %v", *addr)
	if _, err := cbgb.StartServer(*addr, buckets, *defaultBucketName); err != nil {
		log.Fatalf("Error starting server: %s", err)
	}

	if *rest != ":DISABLED" {
		restMain(*rest, *staticPath)
	}

	// Let goroutines do their work.
	select {}
}

func createBucket(bucketName string, bucketSettings *cbgb.BucketSettings) (
	cbgb.Bucket, error) {
	log.Printf("creating bucket: %v, numPartitions: %v",
		bucketName, bucketSettings.NumPartitions)

	bucket, err := buckets.New(bucketName, bucketSettings)
	if err != nil {
		return nil, err
	}

	bucket.Subscribe(mutationLogCh)

	for vbid := 0; vbid < bucketSettings.NumPartitions; vbid++ {
		bucket.CreateVBucket(uint16(vbid))
		bucket.SetVBState(uint16(vbid), cbgb.VBActive)
	}

	if err = bucket.Flush(); err != nil {
		return nil, err
	}

	return bucket, nil
}
