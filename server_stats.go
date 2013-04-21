package main

import (
	"sync"
	"sync/atomic"
	"time"
)

var serverStats = &ServerStats{}
var serverStatsSnapshot = &ServerStatsSnapshot{
	CurServer: &ServerStats{},
	AggServer: NewAggStats(func() Aggregatable {
		return &ServerStats{Time: int64(time.Now().Unix())}
	}),
}
var serverStatsLock sync.Mutex
var serverStatsAvailableCh = make(chan bool)

type ServerStatsSnapshot struct {
	CurServer    *ServerStats
	AggServer    *AggStats
	LatestUpdate time.Time

	requests int
}

func (sss *ServerStatsSnapshot) LatestUpdateTime() time.Time {
	return sss.LatestUpdate
}

func (sss *ServerStatsSnapshot) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"totals": map[string]interface{}{
			"serverStats": sss.CurServer,
		},
		"diffs": map[string]interface{}{
			"serverStats": sss.AggServer,
		},
		"levels": AggStatsLevels,
	}
}

func (b *ServerStatsSnapshot) Copy() *ServerStatsSnapshot {
	return &ServerStatsSnapshot{
		&(*b.CurServer),
		&(*b.AggServer),
		b.LatestUpdate,
		0,
	}
}

type ServerStats struct {
	Time int64 `json:"time"`

	AcceptedConns int64 `json:"acceptedConns"`
	ClosedConns   int64 `json:"closedConns"`
	OpenConns     int64 `json:"openConns"`
}

func (sx *ServerStats) Add(in *ServerStats) {
	sx.Op(in, addInt64)
}

func (sx *ServerStats) Sub(in *ServerStats) {
	sx.Op(in, subInt64)
}

func (sx *ServerStats) Op(in *ServerStats, op func(int64, int64) int64) {
	sx.AcceptedConns = op(sx.AcceptedConns, atomic.LoadInt64(&in.AcceptedConns))
	sx.ClosedConns = op(sx.ClosedConns, atomic.LoadInt64(&in.ClosedConns))
	sx.OpenConns = op(sx.OpenConns, atomic.LoadInt64(&in.OpenConns))
}

func (sx *ServerStats) Aggregate(in Aggregatable) {
	if in == nil {
		return
	}
	sx.Add(in.(*ServerStats))
}

func (sx *ServerStats) Equal(in *ServerStats) bool {
	return sx.AcceptedConns == atomic.LoadInt64(&in.AcceptedConns) &&
		sx.ClosedConns == atomic.LoadInt64(&in.ClosedConns) &&
		sx.OpenConns == atomic.LoadInt64(&in.OpenConns)
}

func sampleServerStats(t time.Time) bool {
	serverStatsLock.Lock()
	defer serverStatsLock.Unlock()

	currServerStats := &ServerStats{}
	currServerStats.Add(serverStats)
	diffServerStats := &ServerStats{}
	diffServerStats.Add(currServerStats)
	diffServerStats.Sub(serverStatsSnapshot.CurServer)
	diffServerStats.Time = t.Unix()
	serverStatsSnapshot.AggServer.AddSample(diffServerStats)
	serverStatsSnapshot.CurServer = currServerStats
	serverStatsSnapshot.LatestUpdate = t

	return true
}

func snapshotServerStats() StatsSnapshot {
	serverStatsLock.Lock()
	defer serverStatsLock.Unlock()

	serverStatsSnapshot.requests++

	return serverStatsSnapshot.Copy()
}
