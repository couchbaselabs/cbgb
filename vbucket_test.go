package cbgb

import (
	"io/ioutil"
	"os"

	"testing"
)

func TestVBucketHash(t *testing.T) {
	vbid := VBucketIdForKey([]byte("hello"), 1024)
	if vbid != 528 {
		t.Errorf("expected vbid of 528, got: %v", vbid)
	}
}

func TestVBKeyRangeEqual(t *testing.T) {
	if (&VBKeyRange{}).Equal(nil) {
		t.Errorf("expected non-nil to not equal nil")
	}
}

func TestItemsStatPersists(t *testing.T) {
	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)

	b0, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b0.Close()

	r0 := &reqHandler{currentBucket: b0}
	vb0, _ := b0.CreateVBucket(2)
	b0.SetVBState(2, VBActive)

	if 0 != vb0.stats.Items {
		t.Errorf("expected to have 0 items initially")
	}

	testLoadInts(t, r0, 2, 5)
	testExpectInts(t, r0, 2, []int{0, 1, 2, 3, 4}, "initial data load")

	if 5 != vb0.stats.Items {
		t.Errorf("expected to have 5 items")
	}

	err = b0.Flush()
	if err != nil {
		t.Errorf("expected Flush to work, got: %v", err)
	}

	if 5 != vb0.stats.Items {
		t.Errorf("expected to have 5 items still after flushing")
	}

	b1, err := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	if err != nil {
		t.Errorf("expected NewBucket to work, got: %v", err)
	}
	defer b1.Close()

	b1.Load()
	vb1 := b1.GetVBucket(2)
	if 5 != vb1.stats.Items {
		t.Errorf("expected to have 5 items after loading, got: %v", vb1.stats.Items)
	}
}
