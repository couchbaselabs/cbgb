package cbgb

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dustin/gomemcached"
)

func TestSplitRange(t *testing.T) {
	vb0 := []int{0}
	vb1 := []int{1}

	testBucketDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testBucketDir)
	testBucket, _ := NewBucket(testBucketDir,
		&BucketSettings{
			NumPartitions: MAX_VBUCKETS,
		})
	defer testBucket.Close()
	rh := reqHandler{currentBucket: testBucket}
	testBucket.CreateVBucket(0)
	testBucket.SetVBState(0, VBActive)

	tests := []struct {
		vbid int
		op   gomemcached.CommandCode
		key  string
		val  string

		expStatus gomemcached.Status
		expActive []int
	}{
		{0, SPLIT_RANGE, "", "NO_BODY",
			gomemcached.EINVAL, vb0},
		{0, SPLIT_RANGE, "", ``,
			gomemcached.EINVAL, vb0},
		{0, SPLIT_RANGE, "", `{`,
			gomemcached.EINVAL, vb0},
		{0, SPLIT_RANGE, "", `{}`,
			gomemcached.EINVAL, vb0},
		{0, SPLIT_RANGE, "", `"this is unexpected"`,
			gomemcached.EINVAL, vb0},
		{0, SPLIT_RANGE, "", `{"splits":[]}`,
			gomemcached.EINVAL, vb0},
		{0, SPLIT_RANGE, "", `{"splits":[{"vbucketId":0}]}`,
			gomemcached.EINVAL, vb0},
		{0, SPLIT_RANGE, "", fmt.Sprintf(`{"splits":[{"vbucketId":%v}]}`, MAX_VBUCKETS+1),
			gomemcached.EINVAL, vb0},
		{0, SPLIT_RANGE, "", `{"splits":[{"vbucketId":1}]}`,
			gomemcached.SUCCESS, vb1},
		{0, SPLIT_RANGE, "", `{"splits":[{"vbucketId":1}]}`,
			gomemcached.EINVAL, vb1},
		{1, SPLIT_RANGE, "", `{"splits":[{"vbucketId":0},{"vbucketId":2}]}`,
			gomemcached.SUCCESS, []int{0, 2}},
		{1, SPLIT_RANGE, "", `{"splits":[{"vbucketId":0},{"vbucketId":2}]}`,
			gomemcached.EINVAL, []int{0, 2}},
		{0, SPLIT_RANGE, "", `{"splits":[{"vbucketId":1},{"vbucketId":2}]}`,
			gomemcached.EINVAL, []int{0, 2}},
		{0, SPLIT_RANGE, "", `{"splits":[{"vbucketId":3},{"vbucketId":1}]}`,
			gomemcached.SUCCESS, []int{1, 2, 3}},
	}

	for testNum, x := range tests {
		req := &gomemcached.MCRequest{
			Opcode:  x.op,
			VBucket: uint16(x.vbid),
			Key:     []byte(x.key),
			Body:    []byte(x.val),
		}
		if x.val == "NO_BODY" {
			req.Body = nil
		}
		res := rh.HandleMessage(nil, nil, req)
		if res.Status != x.expStatus {
			t.Errorf("%v: Expected %v for %v:%v, got: %v, body: %v",
				testNum, x.expStatus, x.op, x.key, res.Status, string(res.Body))
		}
		last := -1
		for _, v := range x.expActive {
			vb := testBucket.GetVBucket(uint16(v))
			if vb == nil {
				t.Errorf("%v: Expected vbucket %v but was nil", testNum, v)
			} else if vb.GetVBState() != VBActive {
				t.Errorf("%v: Expected vbucket %v to be active but was %v",
					testNum, v, testBucket.GetVBucket(uint16(v)).GetVBState())
			}
			for {
				last++
				if last >= v {
					break
				}
				vb := testBucket.GetVBucket(uint16(last))
				if vb != nil && vb.GetVBState() == VBActive {
					t.Errorf("%v: Expected vbucket %v to not be active, but it is",
						testNum, last)
				}
			}
		}
	}
}
