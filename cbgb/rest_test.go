package main

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/couchbaselabs/cbgb"
	"github.com/gorilla/mux"
)

func testSetupBuckets(t *testing.T) (string, *cbgb.Buckets) {
	d, _ := ioutil.TempDir("./tmp", "test")
	var err error
	buckets, err = cbgb.NewBuckets(d,
		&cbgb.BucketSettings{
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Second,
			CompactInterval: 10 * time.Second,
		})
	if err != nil {
		t.Fatalf("testSetupBuckets failed: %v", err)
	}
	if buckets == nil {
		t.Fatalf("testSetupBuckets had nil buckets")
	}
	return d, buckets
}

func TestRestAPIBuckets(t *testing.T) {
	d, _ := testSetupBuckets(t)
	defer os.RemoveAll(d)

	mr := mux.NewRouter()
	restAPI(mr, d)
	restCouchAPI(mr)

	rr := httptest.NewRecorder()
	r, _ := http.NewRequest("GET", "http://127.0.0.1/_api/buckets", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to work, got: %#v, %v",
			rr, rr.Body.String())
	}
}
