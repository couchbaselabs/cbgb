package cbgb

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/dustin/gomemcached"
	"github.com/steveyen/gkvlite"
)

type VBSplitRangePart struct {
	VBucketId       int   `json:"vbucketId"`
	MinKeyInclusive Bytes `json:"minKeyInclusive"`
	MaxKeyExclusive Bytes `json:"maxKeyExclusive"`
}

type VBSplitRange struct {
	Splits []VBSplitRangePart `json:"splits"`
}

type VBSplitRangeParts []VBSplitRangePart

func (sr VBSplitRangeParts) Len() int {
	return len(sr)
}

func (sr VBSplitRangeParts) Less(i, j int) bool {
	return sr[i].VBucketId < sr[j].VBucketId
}

func (sr VBSplitRangeParts) Swap(i, j int) {
	sr[i], sr[j] = sr[j], sr[i]
}

func vbSplitRange(v *VBucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	if req.Body != nil {
		sr := &VBSplitRange{}
		if err := json.Unmarshal(req.Body, sr); err != nil {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("Error decoding split-range json: %v, err: %v",
					string(req.Body), err)),
			}
		}
		return v.splitRange(sr)
	}
	return &gomemcached.MCResponse{Status: gomemcached.EINVAL}
}

func (v *VBucket) splitRange(sr *VBSplitRange) (res *gomemcached.MCResponse) {
	// Spliting to just 1 new destination vbucket is allowed.  It's
	// equivalent to re-numbering a vbucket with a different
	// vbucket-id.
	if len(sr.Splits) < 1 {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
			Body:   []byte("Error need at least 1 splits"),
		}
	}

	// Sort the splits by vbucket-id so that duplicate checks are easy
	// and so that our upcoming "lock" visits will avoid deadlocking.
	sort.Sort(VBSplitRangeParts(sr.Splits))

	// Validate the splits.
	max := -1
	for _, split := range sr.Splits {
		if split.VBucketId < 0 || split.VBucketId >= MAX_VBUCKETS {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("vbucket id %v out of range",
					split.VBucketId)),
			}
		}
		if split.VBucketId <= max || uint16(split.VBucketId) == v.vbid {
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("vbucket id %v is duplicate",
					split.VBucketId)),
			}
		}
		max = split.VBucketId
	}

	// TODO: Validate that split ranges are non-overlapping and fully
	// covering v's existing range.

	return v.splitRangeActual(sr.Splits)
}

func (v *VBucket) splitRangeActual(splits []VBSplitRangePart) (res *gomemcached.MCResponse) {
	res = &gomemcached.MCResponse{Status: gomemcached.EINVAL}

	var transferSplits func(int)
	transferSplits = func(splitIdx int) {
		log.Printf("transferSplits %v", splitIdx)

		if splitIdx >= len(splits) {
			// We reach this recursion base-case after all our newly
			// created destination vbuckets and source v have been
			// visited (we've "locked" all their goroutines), so mark
			// success so our unwinding code can complete the split
			// transfer.
			res = &gomemcached.MCResponse{}
			return
		}

		vbid := uint16(splits[splitIdx].VBucketId)
		var vb *VBucket
		created := false
		if vbid != v.Meta().Id {
			vb, _ = v.parent.CreateVBucket(vbid)
			created = true
		}
		if vb == nil {
			vb, _ = v.parent.GetVBucket(vbid)
		}

		// TODO: Possible race here, in-between creation and access,
		// where an adversary could delete the vbucket at the wrong time?

		if vb != nil {
			vb.Apply(func() {
				if parseVBState(vb.Meta().State) == VBDead {
					transferSplits(splitIdx + 1)
					if res.Status == gomemcached.SUCCESS {
						err := v.rangeCopyTo(vb,
							splits[splitIdx].MinKeyInclusive,
							splits[splitIdx].MaxKeyExclusive)
						if err != nil {
							res = &gomemcached.MCResponse{
								Status: gomemcached.TMPFAIL,
							}
						}
						// TODO: poke observers on vb's changed state.
					}
				} else {
					res = &gomemcached.MCResponse{
						Status: gomemcached.EINVAL,
						Body: []byte(fmt.Sprintf("Error split-range, vbucket: %v,"+
							" state not initially dead or was incorrect,"+
							" was: %v, req: %v", vbid, vb.Meta().State, splits)),
					}
				}
			})
			if res.Status != gomemcached.SUCCESS && created {
				// TODO: Cleanup the vbucket that we created if error.
			}
		} else {
			res = &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("Error split-range,"+
					" tried to create/get vbucket: %v, req: %v",
					vbid, splits)),
			}
		}
	}

	transferSplits(0)

	if res.Status == gomemcached.SUCCESS {
		v.Apply(func() {
			meta := v.Meta()
			meta.State = VBDead.String()
			meta.KeyRange = &VBKeyRange{}
			// TODO: Need to flush?
			// TODO: Need a new 'purge vbucket' command?
		})
	}
	return
}

func (v *VBucket) rangeCopyTo(dst *VBucket,
	minKeyInclusive []byte, maxKeyExclusive []byte) (err error) {
	// TODO: Should this be under src and/or dst Apply()/Mutate() protection?
	// TODO: The bucketstore might want to compact the dst in the middle of copying?

	srcKeys, srcChanges := v.ps.colls()

	dst.ps.mutate(func(dstKeys, dstChanges *gkvlite.Collection) {
		var hadKeys, hadChanges bool

		hadKeys, err = rangeCopy(srcKeys, dstKeys,
			minKeyInclusive, maxKeyExclusive)
		if err == nil {
			hadChanges, err = rangeCopy(srcChanges, dstChanges,
				minKeyInclusive, maxKeyExclusive)
		}
		if err != nil {
			return
		}

		dstMeta := dst.Meta().Copy()
		dstMeta.update(v.Meta())
		dstMeta.KeyRange = &VBKeyRange{
			MinKeyInclusive: minKeyInclusive,
			MaxKeyExclusive: maxKeyExclusive,
		}
		dstMeta.MetaCas = dstMeta.LastCas
		atomic.StorePointer(&dst.meta, unsafe.Pointer(dstMeta))

		if hadKeys || hadChanges {
			dst.bs.dirty(true)
		}
	})

	return err
}
