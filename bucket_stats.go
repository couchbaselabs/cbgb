package cbgb

import (
	"time"
)

type BucketStatsSnapshot struct {
	Current        *Stats
	BucketStore    *BucketStoreStats
	Agg            *AggStats
	AggBucketStore *AggStats
	LatestUpdate   time.Time

	requests int
}

func (bss *BucketStatsSnapshot) LatestUpdateTime() time.Time {
	return bss.LatestUpdate
}

func (bss *BucketStatsSnapshot) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"totals": map[string]interface{}{
			"bucketStats":      bss.Current,
			"bucketStoreStats": bss.BucketStore,
		},
		"diffs": map[string]interface{}{
			"bucketStats":      bss.Agg,
			"bucketStoreStats": bss.AggBucketStore,
		},
		"levels": AggStatsLevels,
	}
}

func (b *BucketStatsSnapshot) Copy() *BucketStatsSnapshot {
	return &BucketStatsSnapshot{
		&(*b.Current),
		&(*b.BucketStore),
		&(*b.Agg),
		&(*b.AggBucketStore),
		b.LatestUpdate,
		0,
	}
}
