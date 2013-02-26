package cbgb

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/steveyen/gkvlite"
)

func TestPartitionStoreEmptyVisit(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	b, err := NewBucket(testBucketDir,
		&BucketSettings{
			FlushInterval:   10 * time.Second,
			SleepInterval:   10 * time.Second,
			CompactInterval: 10 * time.Second,
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
