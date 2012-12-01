package main

import (
	"flag"
	"fmt"
	"log"
	"time"
)

const (
	VERSION     = "0.0.0"
	MAX_VBUCKET = 1024
)

var mutationLogCh = make(chan interface{})

var serverStart = time.Now()

func mutationLogger(ch chan interface{}) {
	for i := range ch {
		switch o := i.(type) {
		case mutation:
			log.Printf("Mutation: %v", o)
		case bucketChange:
			if o.deleted {
				// Dropped it
			} else {
				vb := o.getVBucket()
				if vb != nil {
					vb.observer.Register(ch)
				}
			}
			log.Printf("Bucket change: %v", o)
		default:
			panic(fmt.Sprintf("Unhandled item to log %T: %v", i, i))
		}
	}
}

func main() {
	addr := flag.String("bind", ":11211", "memcached listen port")

	flag.Parse()

	go mutationLogger(mutationLogCh)

	defaultBucket := newBucket()
	defaultBucket.observer.Register(mutationLogCh)
	defaultBucket.createVBucket(0)

	_, err := startMCServer(*addr, defaultBucket)
	if err != nil {
		log.Fatalf("Got an error:  %s", err)
	}

	// Let goroutines do their work.
	select {}
}
