package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/couchbaselabs/cbgb"
	"github.com/dustin/gomemcached"
	"github.com/gorilla/mux"
)

// Don't do any normal logging while running tests.
func init() {
	log.SetOutput(ioutil.Discard)
}

func testSetupBuckets(t *testing.T, numPartitions int) (string, *cbgb.Buckets) {
	d, _ := ioutil.TempDir("./tmp", "test")
	var err error
	bucketSettings = &cbgb.BucketSettings{
		NumPartitions: numPartitions,
		SleepInterval: 10 * time.Second,
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

func testSetupDefaultBucket(t *testing.T, numPartitions int,
	vbid uint16) (string, *cbgb.Buckets, cbgb.Bucket) {
	d, buckets := testSetupBuckets(t, numPartitions)
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
	d, _ := testSetupBuckets(t, 1)
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
	// "hello" hash is 528 with 1024 vbuckets.
	d, _, bucket := testSetupDefaultBucket(t, 1024, uint16(528))
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
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
}

func TestCouchPutDDoc(t *testing.T) {
	testCouchPutDDoc(t, 1)
	testCouchPutDDoc(t, cbgb.MAX_VBUCKETS)
}

func testCouchPutDDoc(t *testing.T, numPartitions int) {
	d, _, _ := testSetupDefaultBucket(t, numPartitions, uint16(0))
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	d0 := []byte(`{
		"_id":"_design/d0",
		"language": "not-javascript",
		"views": {
			"v0": {
				"map": "function(doc) { emit(doc.amount, null) }"
			}
		}
    }`)
	rr := httptest.NewRecorder()
	r, _ := http.NewRequest("PUT",
		"http://127.0.0.1/default/_design/d0",
		bytes.NewBuffer([]byte(d0)))
	mr.ServeHTTP(rr, r)
	if rr.Code != 400 {
		t.Errorf("expected req to 400, got: %#v, %v",
			rr, rr.Body.String())
	}

	d0 = []byte{}
	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("PUT",
		"http://127.0.0.1/default/_design/d0",
		bytes.NewBuffer([]byte(d0)))
	mr.ServeHTTP(rr, r)
	if rr.Code != 400 {
		t.Errorf("expected req to 400, got: %#v, %v",
			rr, rr.Body.String())
	}

	d0 = []byte(`{
		"_id":"_design/d0",
		"views": {
			"v0": {
				"map": "function(doc) { emit(doc.amount, null) }"
			}
		}
    }`)
	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("PUT",
		"http://127.0.0.1/default/_design/d0",
		bytes.NewBuffer([]byte(d0)))
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
}

func TestCouchViewBasic(t *testing.T) {
	d, _, bucket := testSetupDefaultBucket(t, 1, uint16(0))
	defer os.RemoveAll(d)
	mr := testSetupMux(d)

	rr := httptest.NewRecorder()
	r, _ := http.NewRequest("GET", "http://127.0.0.1/default/hello", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 404 {
		t.Errorf("expected req to 404, got: %#v, %v",
			rr, rr.Body.String())
	}

	d0 := []byte(`{
		"_id":"_design/d0",
		"language": "javascript",
		"views": {
			"v0": {
				"map": "function(doc) { emit(doc.amount, null) }"
			}
		}
    }`)

	err := bucket.SetDDoc("_design/d0", d0)
	if err != nil {
		t.Errorf("expecting SetDDoc to work, got: %v", err)
	}

	dx, err := bucket.GetDDoc("_design/d0")
	if err != nil {
		t.Errorf("expecting GetDDoc to work, got: %v", err)
	}
	if !bytes.Equal(dx, d0) {
		t.Errorf("not the same doc, want: %v, got: %v", d0, dx)
	}

	var res *gomemcached.MCResponse
	nitems := 0

	res = cbgb.SetItem(bucket, []byte("a"), []byte(`{"amount": 1}`),
		cbgb.VBActive)
	if res == nil || res.Status != gomemcached.SUCCESS {
		t.Errorf("expected SetItem to work, got: %v", res)
	}
	nitems++
	res = cbgb.SetItem(bucket, []byte("b"), []byte(`{"amount": 3}`),
		cbgb.VBActive)
	if res == nil || res.Status != gomemcached.SUCCESS {
		t.Errorf("expected SetItem to work, got: %v", res)
	}
	nitems++
	res = cbgb.SetItem(bucket, []byte("c"), []byte(`{"amount": 4}`),
		cbgb.VBActive)
	if res == nil || res.Status != gomemcached.SUCCESS {
		t.Errorf("expected SetItem to work, got: %v", res)
	}
	nitems++
	res = cbgb.SetItem(bucket, []byte("d"), []byte(`{"amount": 2}`),
		cbgb.VBActive)
	if res == nil || res.Status != gomemcached.SUCCESS {
		t.Errorf("expected SetItem to work, got: %v", res)
	}
	nitems++

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/not-a-design-doc/_view/v0", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 404 {
		t.Errorf("expected req to 404, got: %#v, %v",
			rr, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/not-a-view", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 404 {
		t.Errorf("expected req to 404, got: %#v, %v",
			rr, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	r, _ = http.NewRequest("GET",
		"http://127.0.0.1/default/_design/d0/_view/v0", nil)
	mr.ServeHTTP(rr, r)
	if rr.Code != 200 {
		t.Errorf("expected req to 200, got: %#v, %v",
			rr, rr.Body.String())
	}
	dd := &cbgb.ViewResult{}
	err = json.Unmarshal(rr.Body.Bytes(), dd)
	if err != nil {
		t.Errorf("expected good view result, got: %v", err)
	}
	if dd.TotalRows != nitems {
		t.Errorf("expected %v rows, got: %v, %v, %v",
			nitems, dd.TotalRows, dd, rr.Body.String())
	}
	k := []string{"a", "d", "b", "c"}
	a := []int{1, 2, 3, 4}
	for i, row := range dd.Rows {
		if k[i] != row.Id {
			t.Errorf("expected row %#v to match k %#v, i %v", row, k[i], i)
		}
		if a[i] != int(row.Key.(float64)) {
			t.Errorf("expected row %#v to match a %#v, i %v", row, a[i], i)
		}
	}
}
