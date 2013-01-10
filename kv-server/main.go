package main

import (
	"flag"
	"log"

	"github.com/couchbaselabs/cbgb"
)

var mutationLogCh = make(chan interface{})

func main() {
	addr := flag.String("bind", ":11211", "memcached listen port")
	data := flag.String("data", "./tmp", "data directory")

	flag.Parse()

	go cbgb.MutationLogger(mutationLogCh)

	buckets, err := cbgb.NewBuckets(*data)
	if err != nil {
		log.Fatalf("Could not make buckets: %v, data directory: %v", err, *data)
	}

	defaultBucket := buckets.New(cbgb.DEFAULT_BUCKET_KEY)

	defaultBucket.Subscribe(mutationLogCh)
	defaultBucket.CreateVBucket(0)
	defaultBucket.SetVBState(0, cbgb.VBActive)

	_, err := cbgb.StartServer(*addr, buckets)
	if err != nil {
		log.Fatalf("Got an error:  %s", err)
	}

	// Let goroutines do their work.
	select {}
}
