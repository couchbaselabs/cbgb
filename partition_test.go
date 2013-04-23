package main

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/steveyen/gkvlite"
)

func TestPartitionStoreEmptyVisit(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	b, err := NewBucket("test", testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Fatalf("Expected NewBucket() to work")
	}

	vb, err := b.CreateVBucket(0)
	keys, changes := vb.ps.colls()

	numVisits := 0
	err = vb.ps.visit(changes, nil, false, func(i *gkvlite.Item) bool {
		numVisits++
		return true
	})
	if numVisits != 0 {
		t.Errorf("expected only 0 change(s), got: %v", numVisits)
	}

	b.SetVBState(0, VBActive)
	b.Flush()

	err = vb.ps.visit(keys, nil, false, func(i *gkvlite.Item) bool {
		t.Errorf("didn't expect visit to visit any keys, got: %v", i)
		return true
	})
	if err != nil {
		t.Errorf("expected partitionstore.visit to work even when empty, got: %v", err)
	}

	numVisits = 0
	err = vb.ps.visit(changes, nil, false, func(i *gkvlite.Item) bool {
		numVisits++
		return true
	})
	if numVisits != 1 {
		t.Errorf("expected only 1 change(s), got: %v", numVisits)
	}
}

func testFillColl(x *gkvlite.Collection, arr []string) {
	for i, s := range arr {
		x.SetItem(&gkvlite.Item{
			Key:      []byte(s),
			Val:      []byte(s),
			Priority: int32(i),
		})
	}
}

func testCheckColl(t *testing.T, x *gkvlite.Collection, start string, arr []string, cb func(i *gkvlite.Item)) {
	n := 0
	err := x.VisitItemsAscend([]byte(start), true, func(i *gkvlite.Item) bool {
		if cb != nil {
			cb(i)
		}
		if n >= len(arr) {
			t.Errorf("visited more than expected: %v, saw: %v", len(arr), n+1)
		}
		if string(i.Key) != arr[n] {
			t.Errorf("expected visit item: %v, saw: %v", arr[n], i)
		}
		n++
		return true
	})
	if err != nil {
		t.Errorf("expected no visit error, got: %v", err)
	}
	if n != len(arr) {
		t.Errorf("expected # visit callbacks: %v, saw: %v", len(arr), n)
	}
}
