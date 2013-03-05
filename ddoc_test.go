package cbgb

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestGetSetDDoc(t *testing.T) {
	d, err := ioutil.TempDir("./tmp", "test")
	if err != nil {
		t.Fatalf("Expected TempDir to work, got: %v", err)
	}
	defer os.RemoveAll(d)

	bs, err := NewBuckets(d,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer bs.CloseAll()

	b, _ := bs.New("thebucket", bs.settings)

	if b.GetDDocVBucket() == nil {
		t.Errorf("expected to have a ddoc vbucket")
	}

	v, err := b.GetDDoc("hi")
	if err != nil {
		t.Errorf("expecting no ddoc get error for missing ddoc")
	}
	if v != nil {
		t.Errorf("expecting no ddoc for missing ddoc")
	}
	err = b.SetDDoc("hi", []byte("hello"))
	if err != nil {
		t.Errorf("not expecting an error")
	}
	if v != nil {
		t.Errorf("not expecting an error")
	}
	v, err = b.GetDDoc("hi")
	if err != nil {
		t.Errorf("not expecting an error")
	}
	if !bytes.Equal(v, []byte("hello")) {
		t.Errorf("expecting ddocs to be the same")
	}

	if err = b.Flush(); err != nil {
		t.Errorf("expected flush to work")
	}

	b2, err := NewBuckets(d,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer b2.CloseAll()
	if err != nil {
		t.Fatalf("Expected NewBuckets() to work on temp dir")
	}
	err = b2.Load()
	if err != nil {
		t.Errorf("expected re-Buckets.Load() to fail")
	}
	b = b2.Get("thebucket")
	if b == nil {
		t.Errorf("expected the bucket")
	}
	if b.GetDDocVBucket() == nil {
		t.Errorf("expected to have a ddoc vbucket")
	}
	v, err = b.GetDDoc("hi")
	if err != nil {
		t.Errorf("not expecting an error")
	}
	if !bytes.Equal(v, []byte("hello")) {
		t.Errorf("expecting ddocs to be the same")
	}
	v, err = b.GetDDoc("not-a-ddoc")
	if err != nil {
		t.Errorf("expecting no ddoc get error for missing ddoc")
	}
	if v != nil {
		t.Errorf("expecting no ddoc for missing ddoc")
	}
}
