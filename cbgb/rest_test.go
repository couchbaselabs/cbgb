package main

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/couchbaselabs/cbgb"
	"github.com/dustin/gomemcached"
	"github.com/gorilla/mux"
)

func testSetupBuckets(t *testing.T) (string, *cbgb.Buckets) {
	d, _ := ioutil.TempDir("./tmp", "test")
	var err error
	bucketSettings = &cbgb.BucketSettings{
		FlushInterval:   10 * time.Second,
		SleepInterval:   10 * time.Second,
		CompactInterval: 10 * time.Second,
	}
	buckets, err = cbgb.NewBuckets(d, bucketSettings)
	if err != nil {
		t.Fatalf("testSetupBuckets failed: %v", err)
	}
	if buckets == nil {
		t.Fatalf("testSetupBuckets had nil buckets")
	}
	return d, buckets
}

func testSetupDefaultBucket(t *testing.T, vbid uint16) (string, *cbgb.Buckets, cbgb.Bucket) {
	d, buckets := testSetupBuckets(t)
	bucket, err := buckets.New("default", bucketSettings)
	if err != nil {
		t.Fatalf("testSetupDefaultBucket failed: %v", err)
	}
	_, err = bucket.CreateVBucket(vbid)
	if err != nil {
		t.Fatalf("testSetupDefaultBucket CreateVBucket failed: %v", err)
	}
	err = bucket.SetVBState(vbid, cbgb.VBActive)
	if err != nil {
		t.Fatalf("testSetupDefaultBucket SetVBState failed: %v", err)
	}
	return d, buckets, bucket
}

func testSetupMux(dir string) *mux.Router {
	mr := mux.NewRouter()
	restAPI(mr, dir)
	restCouchAPI(mr)
	return mr
}

func TestRestAPIBuckets(t *testing.T) {
	d, _ := testSetupBuckets(t)
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	rr := httptest.NewRecorder()
	r, _ := http.NewRequest("GET", "http://127.0.0.1/_api/buckets", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to work, got: %#v, %v",
			rr, rr.Body.String())
	}
}

func TestCouchDocGet(t *testing.T) {
	d, _, bucket := testSetupDefaultBucket(t, uint16(528)) // "hello" hash is 528.
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	rr := httptest.NewRecorder()
	r, _ := http.NewRequest("GET", "http://127.0.0.1/default/hello", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 404 {
		t.Errorf("expected req to 404, got: %#v, %v",
			rr, rr.Body.String())
	}

	res := cbgb.SetItem(bucket, []byte("hello"), []byte("world"), cbgb.VBActive)
	if res == nil || res.Status != gomemcached.SUCCESS {
		t.Errorf("expected SetItem to work, got: %v", res)
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET", "http://127.0.0.1/default/hello", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 404, got: %#v, %v",
			rr, rr.Body.String())
	}
}
