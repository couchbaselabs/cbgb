package main

import (
	"sync/atomic"
)

type BucketStoreStats struct {
	Time int64 `json:"time"`

	Flushes       int64 `json:"flushes"`
	Reads         int64 `json:"reads"`
	Writes        int64 `json:"writes"`
	Stats         int64 `json:"stats"`
	Compacts      int64 `json:"compacts"`
	LastCompactAt int64 `json:"lastCompactAt"`

	FlushErrors   int64 `json:"flushErrors"`
	ReadErrors    int64 `json:"readErrors"`
	WriteErrors   int64 `json:"writeErrors"`
	StatErrors    int64 `json:"statErrors"`
	CompactErrors int64 `json:"compactErrors"`

	ReadBytes  int64 `json:"readBytes"`
	WriteBytes int64 `json:"writeBytes"`

	FileSize   int64 `json:"fileSize"`
	NodeAllocs int64 `json:"nodeAllocs"`
}

func (bss *BucketStoreStats) Add(in *BucketStoreStats) {
	bss.Op(in, addInt64)
}

func (bss *BucketStoreStats) Sub(in *BucketStoreStats) {
	bss.Op(in, subInt64)
}

func (bss *BucketStoreStats) Op(in *BucketStoreStats, op func(int64, int64) int64) {
	bss.Flushes = op(bss.Flushes, atomic.LoadInt64(&in.Flushes))
	bss.Reads = op(bss.Reads, atomic.LoadInt64(&in.Reads))
	bss.Writes = op(bss.Writes, atomic.LoadInt64(&in.Writes))
	bss.Stats = op(bss.Stats, atomic.LoadInt64(&in.Stats))
	bss.Compacts = op(bss.Compacts, atomic.LoadInt64(&in.Compacts))
	bss.FlushErrors = op(bss.FlushErrors, atomic.LoadInt64(&in.FlushErrors))
	bss.ReadErrors = op(bss.ReadErrors, atomic.LoadInt64(&in.ReadErrors))
	bss.WriteErrors = op(bss.WriteErrors, atomic.LoadInt64(&in.WriteErrors))
	bss.StatErrors = op(bss.StatErrors, atomic.LoadInt64(&in.StatErrors))
	bss.CompactErrors = op(bss.CompactErrors, atomic.LoadInt64(&in.CompactErrors))
	bss.ReadBytes = op(bss.ReadBytes, atomic.LoadInt64(&in.ReadBytes))
	bss.WriteBytes = op(bss.WriteBytes, atomic.LoadInt64(&in.WriteBytes))
	bss.FileSize = op(bss.FileSize, atomic.LoadInt64(&in.FileSize))
	bss.NodeAllocs = op(bss.NodeAllocs, atomic.LoadInt64(&in.NodeAllocs))
}

func (bss *BucketStoreStats) Aggregate(in Aggregatable) {
	if in == nil {
		return
	}
	bss.Add(in.(*BucketStoreStats))
}

func (bss *BucketStoreStats) Equal(in *BucketStoreStats) bool {
	return bss.Flushes == atomic.LoadInt64(&in.Flushes) &&
		bss.Reads == atomic.LoadInt64(&in.Reads) &&
		bss.Writes == atomic.LoadInt64(&in.Writes) &&
		bss.Stats == atomic.LoadInt64(&in.Stats) &&
		bss.Compacts == atomic.LoadInt64(&in.Compacts) &&
		bss.FlushErrors == atomic.LoadInt64(&in.FlushErrors) &&
		bss.ReadErrors == atomic.LoadInt64(&in.ReadErrors) &&
		bss.WriteErrors == atomic.LoadInt64(&in.WriteErrors) &&
		bss.StatErrors == atomic.LoadInt64(&in.StatErrors) &&
		bss.CompactErrors == atomic.LoadInt64(&in.CompactErrors) &&
		bss.ReadBytes == atomic.LoadInt64(&in.ReadBytes) &&
		bss.WriteBytes == atomic.LoadInt64(&in.WriteBytes) &&
		bss.FileSize == atomic.LoadInt64(&in.FileSize) &&
		bss.NodeAllocs == atomic.LoadInt64(&in.NodeAllocs)
}
