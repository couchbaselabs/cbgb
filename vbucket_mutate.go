package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/dustin/gomemcached"
)

func vbMutate(v *VBucket, w io.Writer,
	req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	atomic.AddInt64(&v.stats.Mutations, 1)

	cmd := updateMutationStats(req.Opcode, &v.stats)

	if len(req.Body) > MAX_ITEM_DATA_LENGTH {
		return &gomemcached.MCResponse{
			Status: gomemcached.E2BIG,
			Body: []byte(fmt.Sprintf("data too big: %v, key: %v",
				len(req.Body), req.Key)),
		}
	}

	if cmd == gomemcached.ADD && req.Cas != 0 {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
			Body:   []byte("CAS should be 0 for ADD request"),
		}
	}

	var deltaItemBytes int64
	var itemOld, itemNew *item
	var itemCas uint64
	var aval uint64
	var err error
	now := time.Now()

	v.Apply(func() {
		itemOld, err = v.getUnexpired(req.Key, now)
		if err != nil {
			res = &gomemcached.MCResponse{
				Status: gomemcached.TMPFAIL,
				Body:   []byte(fmt.Sprintf("Store get itemOld error %v", err)),
			}
			return
		}

		res, err = vbMutateValidate(v, w, req, cmd, itemOld)
		if err != nil {
			return
		}

		itemCas = atomic.AddUint64(&v.Meta().LastCas, 1)

		res, itemNew, aval, err = vbMutateItemNew(v, w, req, cmd, itemCas, itemOld)
		if err != nil {
			return
		}

		quotaBytes := v.parent.GetBucketSettings().QuotaBytes
		if quotaBytes > 0 {
			nb := atomic.LoadInt64(v.bucketItemBytes)
			nb = nb + itemNew.NumBytes()
			if itemOld != nil {
				nb = nb - itemOld.NumBytes()
			}
			if nb >= quotaBytes {
				res = &gomemcached.MCResponse{
					Status: gomemcached.E2BIG,
					Body: []byte(fmt.Sprintf("quota reached: %v, key: %v",
						quotaBytes, req.Key)),
				}
				return
			}
		}

		deltaItemBytes, err = v.ps.set(itemNew, itemOld)
		if err != nil {
			res = &gomemcached.MCResponse{
				Status: gomemcached.TMPFAIL,
				Body:   []byte(fmt.Sprintf("Store set error %v", err)),
			}
		} else {
			if !req.Opcode.IsQuiet() {
				res = &gomemcached.MCResponse{Cas: itemCas}
				if cmd == gomemcached.INCREMENT || cmd == gomemcached.DECREMENT {
					res.Body = make([]byte, 8)
					binary.BigEndian.PutUint64(res.Body, aval)
				}
			}
		}
	})

	if err != nil {
		if err != ignore {
			atomic.AddInt64(&v.stats.StoreErrors, 1)
		}
	} else {
		if itemOld != nil {
			atomic.AddInt64(&v.stats.Updates, 1)
		} else {
			atomic.AddInt64(&v.stats.Creates, 1)
			atomic.AddInt64(&v.stats.Items, 1)
		}
		atomic.AddInt64(&v.stats.IncomingValueBytes, int64(len(req.Body)))
		atomic.AddInt64(&v.stats.ItemBytes, deltaItemBytes)
		atomic.AddInt64(v.bucketItemBytes, deltaItemBytes)
	}

	if err == nil {
		v.markStale()
		v.observer.Submit(mutation{v.vbid, req.Key, itemCas, false})
	}

	return res
}

func vbMutateValidate(v *VBucket, w io.Writer, req *gomemcached.MCRequest,
	cmd gomemcached.CommandCode, itemOld *item) (*gomemcached.MCResponse, error) {
	if cmd == gomemcached.ADD && itemOld != nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.KEY_EEXISTS,
			Body:   []byte("ADD error because item exists"),
		}, ignore
	}
	if cmd == gomemcached.REPLACE && itemOld == nil {
		return &gomemcached.MCResponse{
			Status: gomemcached.KEY_ENOENT,
			Body:   []byte("REPLACE error because item does not exist"),
		}, ignore
	}
	if req.Cas != 0 && (itemOld == nil || itemOld.cas != req.Cas) {
		return &gomemcached.MCResponse{
			Status: gomemcached.EINVAL,
			Body:   []byte("CAS mismatch"),
		}, ignore
	}
	return nil, nil
}

func vbMutateItemNew(v *VBucket, w io.Writer, req *gomemcached.MCRequest,
	cmd gomemcached.CommandCode, itemCas uint64, itemOld *item) (*gomemcached.MCResponse,
	*item, uint64, error) {

	var flag, exp uint32
	var aval uint64
	var err error

	if cmd == gomemcached.INCREMENT || cmd == gomemcached.DECREMENT {
		if len(req.Extras) != 8+8+4 { // amount, initial, exp
			return &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body: []byte(fmt.Sprintf("wrong extras size for incr/decr: %v on key %v",
					len(req.Extras), req.Key)),
			}, nil, 0, ignore
		}
		exp = binary.BigEndian.Uint32(req.Extras[16:])
	} else {
		if len(req.Extras) == 8 {
			flag = binary.BigEndian.Uint32(req.Extras)
			exp = binary.BigEndian.Uint32(req.Extras[4:])
		}
	}

	itemNew := &item{
		key:  req.Key,
		flag: flag,
		exp:  computeExp(exp, time.Now),
		cas:  itemCas,
	}

	if cmd == gomemcached.INCREMENT || cmd == gomemcached.DECREMENT {
		amount := binary.BigEndian.Uint64(req.Extras)
		initial := binary.BigEndian.Uint64(req.Extras[8:])

		if itemOld != nil {
			aval, err = strconv.ParseUint(string(itemOld.data), 10, 64)
			if err != nil {
				return &gomemcached.MCResponse{
					Status: gomemcached.EINVAL,
					Body:   []byte(fmt.Sprintf("atoi current value err: %v", err)),
				}, nil, 0, ignore
			}
			if cmd == gomemcached.INCREMENT {
				aval += amount
			} else {
				if amount > aval {
					amount = aval
				}
				aval -= amount
			}
		} else {
			if initial == (^uint64(0)) {
				return &gomemcached.MCResponse{
					Status: gomemcached.KEY_ENOENT,
				}, nil, 0, ignore
			}
			aval = initial
		}
		itemNew.data = []byte(strconv.FormatUint(aval, 10))
	} else {
		itemNew.data = req.Body
		if itemOld != nil &&
			(cmd == gomemcached.APPEND || cmd == gomemcached.PREPEND) {
			itemNewLen := len(req.Body) + len(itemOld.data)
			itemNew.data = make([]byte, itemNewLen)
			if cmd == gomemcached.APPEND {
				copy(itemNew.data[0:len(itemOld.data)], itemOld.data)
				copy(itemNew.data[len(itemOld.data):itemNewLen], req.Body)
			} else {
				copy(itemNew.data[0:len(req.Body)], req.Body)
				copy(itemNew.data[len(req.Body):itemNewLen], itemOld.data)
			}
		}
	}

	if itemNew.exp != 0 {
		expirable := atomic.AddInt64(&v.stats.Expirable, 1)
		if expirable == 1 {
			expirerPeriod.Register(v.available, v.mkVBucketSweeper())
		}
	}

	return nil, itemNew, aval, nil
}

func vbDelete(v *VBucket, w io.Writer, req *gomemcached.MCRequest) (res *gomemcached.MCResponse) {
	atomic.AddInt64(&v.stats.Deletes, 1)

	var deltaItemBytes int64
	var prevItem *item
	var cas uint64
	var err error
	now := time.Now()

	v.Apply(func() {
		prevItem, err = v.getUnexpired(req.Key, now)
		if err != nil {
			res = &gomemcached.MCResponse{
				Status: gomemcached.TMPFAIL,
				Body:   []byte(fmt.Sprintf("Store get prevItem error %v", err)),
			}
			return
		}
		if req.Cas != 0 && (prevItem == nil || prevItem.cas != req.Cas) {
			res = &gomemcached.MCResponse{
				Status: gomemcached.EINVAL,
				Body:   []byte("CAS mismatch"),
			}
			return
		}
		if prevItem == nil {
			if req.Opcode.IsQuiet() {
				return
			}
			res = &gomemcached.MCResponse{Status: gomemcached.KEY_ENOENT}
			return
		}

		cas = atomic.AddUint64(&v.Meta().LastCas, 1)

		deltaItemBytes, err = v.ps.del(req.Key, cas, prevItem)
		if err != nil {
			res = &gomemcached.MCResponse{
				Status: gomemcached.TMPFAIL,
				Body:   []byte(fmt.Sprintf("Store del error %v", err)),
			}
		} else {
			if !req.Opcode.IsQuiet() {
				res = &gomemcached.MCResponse{Cas: cas}
			}
		}
	})

	if err != nil {
		atomic.AddInt64(&v.stats.StoreErrors, 1)
	} else if prevItem != nil {
		atomic.AddInt64(&v.stats.Items, -1)
		atomic.AddInt64(&v.stats.ItemBytes, deltaItemBytes)
		atomic.AddInt64(v.bucketItemBytes, deltaItemBytes)
	}

	if err == nil && prevItem != nil {
		v.markStale()
		v.observer.Submit(mutation{v.vbid, req.Key, cas, true})
	}

	return res
}

func (v *VBucket) mkVBucketSweeper() func(time.Time) bool {
	return func(time.Time) bool {
		return v.expirationScan()
	}
}

func (v *VBucket) expirationScan() bool {
	now := time.Now()
	var cleaned int64
	err := v.ps.visitItems(nil, false, func(i *item) bool {
		if i.isExpired(now) {
			err := v.expire(i.key, now)
			if err != nil {
				return false
			}
			cleaned++
		}
		return true
	})

	remaining := atomic.AddInt64(&v.stats.Expirable, -cleaned)
	log.Printf("expirationScan complete, cleaned %v items, %v remaining, err: %v",
		cleaned, remaining, err)
	return remaining > 0
}

func computeExp(exp uint32, tsrc func() time.Time) uint32 {
	var rv uint32
	switch {
	case exp == 0, exp > 30*86400:
		// Absolute time in seconds since epoch.
		rv = exp
	default:
		// Relative time from now.
		now := tsrc()
		rv = uint32(now.Add(time.Duration(exp) * time.Second).Unix())
	}
	return rv
}

func (v *VBucket) getUnexpired(key []byte, now time.Time) (*item, error) {
	i, err := v.ps.get(key)
	if err != nil || i == nil {
		return nil, err
	}
	if i.isExpired(now) {
		go v.expire(key, now)
		return nil, nil
	}
	return i, nil
}

// Invoked when the caller believes the item has expired.  We double
// check here in case some concurrent race has mutated the item.
func (v *VBucket) expire(key []byte, now time.Time) (err error) {
	var deltaItemBytes int64
	var expireCas uint64

	v.Apply(func() {
		var i *item
		i, err = v.ps.get(key)
		if err != nil || i == nil {
			return
		}
		if i.isExpired(now) {
			expireCas = atomic.AddUint64(&v.Meta().LastCas, 1)
			deltaItemBytes, err = v.ps.del(key, expireCas, i)
		}
	})

	atomic.AddInt64(&v.stats.ItemBytes, deltaItemBytes)
	atomic.AddInt64(v.bucketItemBytes, deltaItemBytes)

	if err == nil && expireCas != 0 {
		v.markStale()
		v.observer.Submit(mutation{v.vbid, key, expireCas, true})
	}

	return err
}
