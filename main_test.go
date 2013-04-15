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
