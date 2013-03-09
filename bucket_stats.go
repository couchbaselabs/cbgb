package cbgb

import (
	"time"
)

type BucketStats struct {
	Current        *Stats
	BucketStore    *BucketStoreStats
	Agg            *AggStats
	AggBucketStore *AggStats
	LatestUpdate   time.Time

	requests int
}

func (b BucketStats) Copy() BucketStats {
	return BucketStats{
		&(*b.Current),
		&(*b.BucketStore),
		&(*b.Agg),
		&(*b.AggBucketStore),
		b.LatestUpdate,
		0,
	}
}
