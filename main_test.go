package main

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestCreateBucket(t *testing.T) {
	d, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(d)
	var err error
	bucketSettings = &BucketSettings{NumPartitions: 1}
	buckets, _ = NewBuckets(d, bucketSettings)

	bucket, err := createBucket("default", bucketSettings)
	if err != nil {
		t.Errorf("createBucket err: %v", err)
	}
	if bucket == nil {
		t.Errorf("createBucket didn't work")
	}

	_, err = createBucket("default", bucketSettings)
	if err == nil {
		t.Errorf("2nd createBucket surprisingly worked")
	}
}

func TestUsage(t *testing.T) {
	s := os.Stderr
	defer func() { os.Stderr = s }()
	f, _ := ioutil.TempFile("", "test-main-init")
	defer f.Close()
	os.Stderr = f
	usage()
	fi, _ := f.Stat()
	if fi.Size() <= 0 {
		t.Errorf("expected some usage, got none")
	}
}

func TestMainServer(t *testing.T) {
	d, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(d)

	bucketSettings = &BucketSettings{NumPartitions: 1}
	buckets, _ = NewBuckets(d, bucketSettings)

	mainServer("default", "", 100, "", "", "static", "")
}
