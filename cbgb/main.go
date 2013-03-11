package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
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
	"static", "path to static web UI content")
var defaultBucketName = flag.String("default-bucket-name",
	cbgb.DEFAULT_BUCKET_NAME, `name of the default bucket ("" disables)`)
var numPartitions = flag.Int("num-partitions",
	1, "default number of partitions for new buckets")
var defaultQuotaBytes = flagbytes.Bytes("default-quota",
	"100MB", "quota for default bucket")
var defaultPersistence = flag.Int("default-persistence",
	2, "persistence level for default bucket")

var buckets *cbgb.Buckets
var bucketSettings *cbgb.BucketSettings

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			"usage: %s <flags> [command [command-specific-params...]]:\n",
			os.Args[0])
		fmt.Fprintf(os.Stderr, "\nflags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nverbosity levels:\n")
		fmt.Fprintf(os.Stderr, "  1: verbose logging\n")
		fmt.Fprintf(os.Stderr, "  0: nothing logged\n")
		fmt.Fprintf(os.Stderr, " -1: server command logs; other commands don't log\n")
		fmt.Fprintf(os.Stderr, "\npersistence levels:\n")
		fmt.Fprintf(os.Stderr, "  2: metadata persisted and ops persisted\n")
		fmt.Fprintf(os.Stderr, "  1: metadata persisted and ops not persisted\n")
		fmt.Fprintf(os.Stderr, "  0: nothing persisted\n")
		fmt.Fprintf(os.Stderr, "\ncommands:\n")
		cmds := []string{"server (default command)"}
		for cmd, _ := range mainCmds {
			cmds = append(cmds, cmd)
		}
		sort.Strings(cmds)
		for _, cmd := range cmds {
			fmt.Fprintf(os.Stderr, "  %v\n", cmd)
		}
		os.Exit(1)
	}
}

func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) > 0 || *verbosity == 0 {
		log.SetOutput(ioutil.Discard)
	}
	if *verbosity > 0 || (*verbosity == -1 && (len(args) < 1 || args[0] == "server")) {
		log.SetOutput(os.Stderr)
	}

	log.Printf("cbgb - %v", cbgb.VERSION)
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("  %v=%v", f.Name, f.Value)
	})
	log.Printf("  %v", args)

	go cbgb.MutationLogger(mutationLogCh)

	var err error

	bucketSettings = &cbgb.BucketSettings{
		NumPartitions: *numPartitions,
		QuotaBytes:    int64(*defaultQuotaBytes),
		MemoryOnly:    cbgb.MemoryOnly_LEVEL_PERSIST_NOTHING - *defaultPersistence,
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

	if len(args) <= 0 || args[0] == "server" {
		mainServer(buckets, *defaultBucketName, *addr, *rest, *staticPath)
		return
	}
	if mainCmd, ok := mainCmds[args[0]]; ok {
		mainCmd(buckets, args)
		return
	}

	fmt.Fprintf(os.Stderr, "error: unknown command: %v\n", args[0])
	os.Exit(1)
}

var mainCmds = map[string]func(*cbgb.Buckets, []string){
	"bucket-path": mainBucketPath,
	"bucket-list": mainBucketList,
	"version":     mainVersion,
}

func mainServer(buckets *cbgb.Buckets, defaultBucketName string,
	addr string, rest string, staticPath string) {
	if buckets.Get(defaultBucketName) == nil && defaultBucketName != "" {
		_, err := createBucket(defaultBucketName, bucketSettings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: could not create default bucket: %s, err: %v",
				defaultBucketName, err)
			os.Exit(1)
		}
	}

	log.Printf("listening data on: %v", addr)
	if _, err := cbgb.StartServer(addr, buckets, defaultBucketName); err != nil {
		fmt.Fprintf(os.Stderr, "error: could not start server: %v\n", err)
		os.Exit(1)
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

func mainBucketList(buckets *cbgb.Buckets, args []string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "error: extra params for %v\n", args[0])
		os.Exit(1)
	}
	bucketNames, err := buckets.LoadNames()
	if err != nil {
		fmt.Fprintf(os.Stderr, "buckets.LoadNames() err: %v\n", err)
		os.Exit(1)
	}
	sort.Strings(bucketNames)
	for _, bucketName := range bucketNames {
		fmt.Printf("%v\n", bucketName)
	}
}

func mainVersion(buckets *cbgb.Buckets, args []string) {
	fmt.Printf("%v\n", cbgb.VERSION)
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
