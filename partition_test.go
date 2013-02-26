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

func TestCollRangeCopy(t *testing.T) {
	s, _ := gkvlite.NewStore(nil)
	x := s.SetCollection("x", nil)
	testFillColl(x, []string{"b", "c", "d"})

	d, _ := gkvlite.NewStore(nil)
	dx := d.SetCollection("dx", nil)
	err := collRangeCopy(x, dx, []byte("a"), []byte("a"), []byte("z"))
	testCheckColl(t, dx, "a", []string{"b", "c", "d"}, nil)

	d, _ = gkvlite.NewStore(nil)
	dx = d.SetCollection("dx", nil)
	err = collRangeCopy(x, dx, []byte("a"), []byte("a"), []byte("a"))
	if err != nil {
		t.Errorf("expected collRangeCopy to not have error")
	}
	testCheckColl(t, dx, "a", []string{}, nil)

	d, _ = gkvlite.NewStore(nil)
	dx = d.SetCollection("dx", nil)
	err = collRangeCopy(x, dx, []byte("z"), []byte("z"), []byte("a"))
	if err != nil {
		t.Errorf("expected collRangeCopy to not have error")
	}
	testCheckColl(t, dx, "a", []string{}, nil)

	d, _ = gkvlite.NewStore(nil)
	dx = d.SetCollection("dx", nil)
	err = collRangeCopy(x, dx, []byte("a"), []byte("a"), []byte("b"))
	if err != nil {
		t.Errorf("expected collRangeCopy to not have error")
	}
	testCheckColl(t, dx, "a", []string{}, nil)

	d, _ = gkvlite.NewStore(nil)
	dx = d.SetCollection("dx", nil)
	err = collRangeCopy(x, dx, []byte("d"), []byte("z"), []byte("z"))
	if err != nil {
		t.Errorf("expected collRangeCopy to not have error")
	}
	testCheckColl(t, dx, "a", []string{}, nil)

	d, _ = gkvlite.NewStore(nil)
	dx = d.SetCollection("dx", nil)
	err = collRangeCopy(x, dx, []byte("d"), []byte("d"), []byte("z"))
	if err != nil {
		t.Errorf("expected collRangeCopy to not have error")
	}
	testCheckColl(t, dx, "a", []string{"d"}, nil)

	d, _ = gkvlite.NewStore(nil)
	dx = d.SetCollection("dx", nil)
	err = collRangeCopy(x, dx, []byte("a"), []byte("c"), []byte("d"))
	if err != nil {
		t.Errorf("expected collRangeCopy to not have error")
	}
	testCheckColl(t, dx, "a", []string{"c"}, nil)

	d, _ = gkvlite.NewStore(nil)
	dx = d.SetCollection("dx", nil)
	err = collRangeCopy(x, dx, []byte("a"), []byte("b"), []byte("c"))
	if err != nil {
		t.Errorf("expected collRangeCopy to not have error")
	}
	testCheckColl(t, dx, "a", []string{"b"}, nil)
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
