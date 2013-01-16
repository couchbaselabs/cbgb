package main

import (
	"flag"
	"log"
	"time"

	"github.com/couchbaselabs/cbgb"
)

var mutationLogCh = make(chan interface{})

func main() {
	addr := flag.String("bind", ":11211", "memcached listen port")
	data := flag.String("data", "./tmp", "data directory")
	defaultBucketName := flag.String("default-bucket-name",
		cbgb.DEFAULT_BUCKET_NAME, "name of the default bucket")
	flushInterval := flag.Int("flush-interval",
		10, "seconds between flushing or persisting mutations to storage")

	flag.Parse()

	go cbgb.MutationLogger(mutationLogCh)

	buckets, err := cbgb.NewBuckets(*data, time.Second * time.Duration(*flushInterval))
	if err != nil {
		log.Fatalf("Could not make buckets: %v, data directory: %v", err, *data)
	}

	err = buckets.Load()
	if err != nil {
		log.Printf("Could not load buckets: %v, data directory: %v", err, *data)
	}

	if buckets.Get(*defaultBucketName) == nil {
		defaultBucket, err := buckets.New(*defaultBucketName)
		if err != nil {
			log.Fatalf("Error creating default bucket: %s, %v", *defaultBucketName, err)
		}

		defaultBucket.Subscribe(mutationLogCh)
		defaultBucket.CreateVBucket(0)
		defaultBucket.SetVBState(0, cbgb.VBActive)
	}

	if _, err := cbgb.StartServer(*addr, buckets, *defaultBucketName); err != nil {
		log.Fatalf("Error starting server: %s", err)
	}

	// Let goroutines do their work.
	select {}
}
