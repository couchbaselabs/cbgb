package main

import (
	"sync/atomic"
)

type ServerStats struct {
	AcceptedConns int64 `json:"acceptedConns"`
	ClosedConns   int64 `json:"closedConns"`
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
}

func (sx *ServerStats) Aggregate(in Aggregatable) {
	if in == nil {
		return
	}
	sx.Add(in.(*ServerStats))
}

func (sx *ServerStats) Equal(in *ServerStats) bool {
	return sx.AcceptedConns == atomic.LoadInt64(&in.AcceptedConns) &&
		sx.ClosedConns == atomic.LoadInt64(&in.ClosedConns)
}
