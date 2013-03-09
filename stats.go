package cbgb

import (
	"encoding/json"
	"time"
)

// Interface for things that interact with stats.
type Statish interface {
	SnapshotStats() StatsSnapshot

	StartStats(d time.Duration)
	StopStats()
	StatAge() time.Duration
}

type StatsSnapshot interface {
	LatestUpdateTime() time.Time
	ToMap() map[string]interface{}
}

// The Aggregatable/AggStats machinery is intended to be generic.
type Aggregatable interface {
	Aggregate(input Aggregatable)
}

var AggStatsLevels = []struct {
	Name       string `json:"name"`
	NumSamples int    `json:"numSamples"` // # historical samples to keep at this level.
}{
	{"second", 60}, // 60 seconds in a minute.
	{"minute", 60}, // 60 minutes in an hour.
	{"hour", 24},   // 24 hours in a day.
	{"day", 1},     // Just track 1 day's worth of aggregate for now.
}

type AggStats struct {
	creator func() Aggregatable
	Levels  []*AggStatsSample `json:"levels"`
	Counts  []uint64          `json:"counts"` // Total # samples at respective level.
}

type AggStatsSample struct {
	next  *AggStatsSample
	Stats Aggregatable
}

func NewAggStats(creator func() Aggregatable) *AggStats {
	res := &AggStats{
		creator: creator,
		Levels:  make([]*AggStatsSample, len(AggStatsLevels)),
		Counts:  make([]uint64, len(AggStatsLevels)),
	}

	// Initialize ring at each level.
	for i, level := range AggStatsLevels {
		var first *AggStatsSample
		var last *AggStatsSample
		for j := 0; j < level.NumSamples; j++ {
			last = &AggStatsSample{next: last}
			if j == 0 {
				first = last
			}
		}
		first.next = last
		res.Levels[i] = last
	}

	return res
}

func (a *AggStats) AddSample(s Aggregatable) {
	a.Levels[0].Stats = s
	a.Levels[0] = a.Levels[0].next
	a.Counts[0]++

	// Propagate aggregate samples up to higher granularity levels.
	for i, level := range AggStatsLevels {
		if level.NumSamples <= 1 {
			break
		}
		if a.Counts[i]%uint64(level.NumSamples) != uint64(0) {
			break
		}
		a.Levels[i+1].Stats = AggregateSamples(a.creator(), a.Levels[i])
		a.Levels[i+1] = a.Levels[i+1].next
		a.Counts[i+1]++
	}
}

func AggregateSamples(agg Aggregatable, start *AggStatsSample) Aggregatable {
	c := start
	for {
		agg.Aggregate(c.Stats)
		c = c.next
		if c == start {
			break
		}
	}
	return agg
}

// Oldest entries appear first.
func (a *AggStatsSample) MarshalJSON() ([]byte, error) {
	r := make([]Aggregatable, 0, 60)
	c := a
	for {
		if c.Stats != nil {
			r = append(r, c.Stats)
		}
		c = c.next
		if c == a {
			break
		}
	}
	return json.Marshal(r)
}
