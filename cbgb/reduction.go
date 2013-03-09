package main

import (
	"fmt"
	"math"

	"github.com/robertkrimen/otto"
)

func newReducer() *otto.Otto {
	o := otto.New()
	must(o.Set("sum", javascriptSum))
	must(o.Set("_sum", javascriptReduceSum))
	must(o.Set("_count", javascriptReduceCount))
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

func javascriptSum(call otto.FunctionCall) otto.Value {
	rv := float64(0)
	for _, a := range call.ArgumentList {
		v, err := a.Export()
		if err != nil {
			continue
		}

		switch n := v.(type) {
		case float64:
			rv += zeroate(n)
		case []interface{}:
			for _, nv := range n {
				rv += zeroate(nv)
			}
		default:
			return ottoMust(otto.ToValue(fmt.Sprintf("Unhandled: %v/%T", v, v)))
		}
	}
	return ottoMust(otto.ToValue(rv))
}

func ottoMust(v otto.Value, err error) otto.Value {
	if err != nil {
		panic(err)
	}
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

func must(err error) {
	if err != nil {
		panic(err)
	}
}
