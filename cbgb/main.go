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
var staticPath = flag.String("static-path",
	"static",
	"path to static content")
var defaultPartitions = flag.Int("default-partitions",
	cbgb.MAX_VBUCKETS,
	"default number of partitions for new buckets")

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
		log.Printf("creating default bucket: %v", *defaultBucketName)
		defaultBucket, err := buckets.New(*defaultBucketName)
		if err != nil {
			log.Fatalf("Error creating default bucket: %s, %v",
				*defaultBucketName, err)
		}

		defaultBucket.Subscribe(mutationLogCh)

		for vbid := 0; vbid < *defaultPartitions; vbid++ {
			defaultBucket.CreateVBucket(uint16(vbid))
			defaultBucket.SetVBState(uint16(vbid), cbgb.VBActive)
		}

		if err = defaultBucket.Flush(); err != nil {
			log.Fatalf("Error flushing default bucket: %s, %v",
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
