package main

import (
	"flag"
	"log"
	"time"

	"github.com/couchbaselabs/cbgb"
)

var mutationLogCh = make(chan interface{})

func main() {
	bind := flag.String("bind", ":11211", "memcached listen port")
	data := flag.String("data", "./tmp", "data directory")
	defaultBucketName := flag.String("default-bucket-name",
		cbgb.DEFAULT_BUCKET_NAME,
		"name of the default bucket")
	flushInterval := flag.Duration("flush-interval",
		10 * time.Second,
		"duration between flushing or persisting mutations to storage")
	sleepInterval := flag.Duration("sleep-interval",
		2 * time.Minute,
		"duration until files are closed (to be reopened on the next request)")
	compactInterval := flag.Duration("compact-interval",
		10 * time.Minute,
		"duration until files are compacted")
	purgeTimeout := flag.Duration("purge-timeout",
		10 * time.Second,
		"duration until unused files are purged after compaction")

	flag.Parse()

	log.Printf("cbgb")
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("  %v=%v", f.Name, f.Value)
	})

	go cbgb.MutationLogger(mutationLogCh)

	buckets, err := cbgb.NewBuckets(*data,
		&cbgb.BucketSettings{
			FlushInterval:   *flushInterval,
			SleepInterval:   *sleepInterval,
			CompactInterval: *compactInterval,
			PurgeTimeout:    *purgeTimeout,
		})
	if err != nil {
		log.Fatalf("Could not make buckets: %v, data directory: %v", err, *data)
	}

	log.Printf("loading buckets")
	err = buckets.Load()
	if err != nil {
		log.Printf("Could not load buckets: %v, data directory: %v", err, *data)
	}

	if buckets.Get(*defaultBucketName) == nil {
		log.Printf("creating default bucket: %v", *defaultBucketName)
		defaultBucket, err := buckets.New(*defaultBucketName)
		if err != nil {
			log.Fatalf("Error creating default bucket: %s, %v", *defaultBucketName, err)
		}

		defaultBucket.Subscribe(mutationLogCh)

		for vbid := 0; vbid < cbgb.MAX_VBUCKETS; vbid++ {
			defaultBucket.CreateVBucket(uint16(vbid))
			defaultBucket.SetVBState(uint16(vbid), cbgb.VBActive)
		}
	}

	log.Printf("starting server")
	if _, err := cbgb.StartServer(*bind, buckets, *defaultBucketName); err != nil {
		log.Fatalf("Error starting server: %s", err)
	}

	// Let goroutines do their work.
	select {}
}
