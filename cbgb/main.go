package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/couchbaselabs/cbgb"
	"github.com/daaku/go.flagbytes"
)

var mutationLogCh = make(chan interface{}, 1024)

var startTime = time.Now()

var verbosity = flag.Int("verbosity", -1, "amount of logging")
var addr = flag.String("addr", ":11211", "data protocol listen address")
var data = flag.String("data", "./tmp", "data directory")
var rest = flag.String("rest", ":DISABLED", "rest protocol listen address")
var staticPath = flag.String("static-path",
	"static", "path to static content")
var defaultBucketName = flag.String("default-bucket-name",
	cbgb.DEFAULT_BUCKET_NAME, `name of the default bucket ("" disables)`)
var numPartitions = flag.Int("num-partitions",
	1, "default number of partitions for new buckets")
var quota = flagbytes.Bytes("default-quota", "100MB", "quota for default bucket")
var defaultMemoryOnly = flag.Int("default-mem-only",
	0, "memory only level for default bucket")

var buckets *cbgb.Buckets
var bucketSettings *cbgb.BucketSettings

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])

		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nMemory Levels:\n")
		fmt.Fprintf(os.Stderr, " - 0: Fully Resident\n")
		fmt.Fprintf(os.Stderr, " - 1: Ops not persisted, only partition states\n")
		fmt.Fprintf(os.Stderr, " - 2: Fully non-resident\n")
		os.Exit(1)
	}
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) > 0 || *verbosity == 0 {
		// There was a sub-command, so turn off logging unless explicitly wanted.
		log.SetOutput(ioutil.Discard)
	}

	log.Printf("cbgb")
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("  %v=%v", f.Name, f.Value)
	})
	log.Printf("  %v", args)

	go cbgb.MutationLogger(mutationLogCh)

	var err error

	bucketSettings = &cbgb.BucketSettings{
		NumPartitions: *numPartitions,
		QuotaBytes:    int64(*quota),
		MemoryOnly:    *defaultMemoryOnly,
	}
	buckets, err = cbgb.NewBuckets(*data, bucketSettings)
	if err != nil {
		log.Fatalf("error: could not make buckets: %v, data directory: %v", err, *data)
	}

	log.Printf("loading buckets from: %v", *data)
	err = buckets.Load(false)
	if err != nil {
		log.Fatalf("error: could not load buckets: %v, data directory: %v", err, *data)
	}

	if buckets.Get(*defaultBucketName) == nil &&
		*defaultBucketName != "" {
		_, err := createBucket(*defaultBucketName, bucketSettings)
		if err != nil {
			log.Fatalf("error: could not create default bucket: %s, err: %v",
				*defaultBucketName, err)
		}
	}

	if len(args) <= 0 || args[0] == "server" {
		mainServer(buckets, *defaultBucketName, *addr, *rest, *staticPath)
		return
	}
	if args[0] == "bucket-path" {
		mainBucketPath(buckets, args)
		return
	}

	log.Fatalf("error: unknown command: %v", args[0])
}

func mainServer(buckets *cbgb.Buckets, defaultBucketName string,
	addr string, rest string, staticPath string) {
	log.Printf("listening data on: %v", addr)
	if _, err := cbgb.StartServer(addr, buckets, defaultBucketName); err != nil {
		log.Fatalf("error: could not start server: %s", err)
	}

	if rest != ":DISABLED" {
		restMain(rest, staticPath)
	}

	// Let goroutines do their work.
	select {}
}

func mainBucketPath(buckets *cbgb.Buckets, args []string) {
	if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "error: need bucket name for %v\n", args[0])
		os.Exit(1)
	}
	fmt.Printf("%v\n", buckets.Path(args[1]))
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
