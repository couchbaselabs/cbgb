package main

import (
	"fmt"
	"math"

	"github.com/robertkrimen/otto"
)

func newReducer() *otto.Otto {
	o := otto.New()
	must(o.Set("_sum", javascriptReduceSum))
	must(o.Set("_count", javascriptReduceCount))
	must(o.Set("_stats", javascriptReduceStats))
	return o
}

// Convert interface{} to float with NaN and infs to 0 for summing
func zeroate(i interface{}) float64 {
	f, ok := i.(float64)
	if !ok {
		return 0
	}
	if math.IsNaN(f) || math.IsInf(f, 1) || math.IsInf(f, -1) {
		return 0
	}
	return f
}

func ottoMust(v otto.Value, err error) otto.Value {
	must(err)
	return v
}

func javascriptReduceSum(call otto.FunctionCall) otto.Value {
	ob, err := call.Argument(1).Export()
	if err != nil {
		return ottoMust(otto.ToValue(fmt.Sprintf("Error getting stuff: %v", err)))
	}
	l, ok := ob.([]interface{})
	if !ok {
		return ottoMust(otto.ToValue(fmt.Sprintf("unhandled %v/%T", ob, ob)))
	}
	rv := float64(0)
	for _, i := range l {
		rv += zeroate(i)
	}
	return ottoMust(otto.ToValue(rv))
}

func javascriptReduceCount(call otto.FunctionCall) otto.Value {
	rere, err := call.Argument(2).ToBoolean()
	if err != nil {
		return ottoMust(otto.ToValue(fmt.Sprintf("Error getting rere flag: %v", err)))
	}

	ob, err := call.Argument(1).Export()
	if err != nil {
		return ottoMust(otto.ToValue(fmt.Sprintf("Error getting stuff: %v", err)))
	}
	l, ok := ob.([]interface{})
	if !ok {
		return ottoMust(otto.ToValue(fmt.Sprintf("unhandled %v/%T", ob, ob)))
	}

	if !rere {
		return ottoMust(otto.ToValue(len(l)))
	}

	rv := float64(0)
	for _, i := range l {
		rv += zeroate(i)
	}
	return ottoMust(otto.ToValue(rv))
}

type statsResult struct {
	sum, min, max, sumsqr float64
	count                 int
}

func (s statsResult) toMap() map[string]float64 {
	return map[string]float64{
		"sum":    s.sum,
		"count":  float64(s.count),
		"min":    s.min,
		"max":    s.max,
		"sumsqr": s.sumsqr,
	}
}

func (s statsResult) toOtto() otto.Value {
	o := otto.New()
	return ottoMust(o.ToValue(s.toMap()))
}

func (s *statsResult) load(ob interface{}) {
	m, ok := ob.(map[string]interface{})
	if !ok {
		return
	}
	s.sum = zeroate(m["sum"])
	s.count = int(zeroate(m["count"]))
	s.min = zeroate(m["min"])
	s.max = zeroate(m["max"])
	s.sumsqr = zeroate(m["sumsqr"])
}

func (s *statsResult) Add(from statsResult) {
	s.sum += from.sum
	s.count += from.count
	s.min = math.Min(s.min, from.min)
	s.max = math.Max(s.max, from.max)
	s.sumsqr += from.sumsqr
}

func javascriptReduceStats(call otto.FunctionCall) otto.Value {
	rere, err := call.Argument(2).ToBoolean()
	if err != nil {
		return ottoMust(otto.ToValue(fmt.Sprintf("Error getting rere flag: %v", err)))
	}

	ob, err := call.Argument(1).Export()
	if err != nil {
		return ottoMust(otto.ToValue(fmt.Sprintf("Error getting stuff: %v", err)))
	}
	l, ok := ob.([]interface{})
	if !ok {
		return ottoMust(otto.ToValue(fmt.Sprintf("unhandled %v/%T", ob, ob)))
	}

	rv := statsResult{}

	if len(l) == 0 {
		return rv.toOtto()
	}

	if rere {
		// Rereduction goes here.
		rv.load(l[0])
		for i := 1; i < len(l); i++ {
			ob := statsResult{}
			ob.load(l[i])
			rv.Add(ob)
		}
		return rv.toOtto()
	}

	// Initial reduction
	rv.count = 1
	rv.sum = zeroate(l[0])
	rv.min = rv.sum
	rv.max = rv.sum
	rv.sumsqr = rv.sum * rv.sum

	for i := 1; i < len(l); i++ {
		v := zeroate(l[i])
		rv.count++
		rv.sum += v
		rv.min = math.Min(rv.min, v)
		rv.max = math.Max(rv.max, v)
		rv.sumsqr += (v * v)
	}

	return rv.toOtto()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
