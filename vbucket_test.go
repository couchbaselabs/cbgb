package cbgb

import (
	"testing"
)

func TestVBucketHash(t *testing.T) {
	vbid := VBucketIdForKey([]byte("hello"), 1024)
	if vbid != 528 {
		t.Errorf("expected vbid of 528, got: %v", vbid)
	}
}
