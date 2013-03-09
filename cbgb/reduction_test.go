package main

import (
	"io/ioutil"
	"testing"

	"github.com/robertkrimen/otto"
)

func mkAssert(t *testing.T) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		name, err := call.Argument(0).ToString()
		if err != nil {
			t.Fatalf("Error getting name of assertion: %v", err)
		}
		t.Logf("RUN %v", name)

		got, err := call.Argument(1).Export()
		if err != nil {
			t.Errorf("Eval error on %v: %v", name, err)
			return otto.UndefinedValue()
		}
		exp, err := call.Argument(2).Export()
		if err != nil {
			t.Errorf("Comparison error on %v: %v", name, err)
			return otto.UndefinedValue()
		}

		if exp != got {
			t.Errorf("Expected %v for %v, got %v",
				exp, name, got)
		} else {
			t.Logf("PASS %v", name)
		}
		return otto.UndefinedValue()
	}
}

func mkTestOtto(t *testing.T) *otto.Otto {
	o := newReducer()
	must(o.Set("assert", mkAssert(t)))
	return o
}

func TestJSReductions(t *testing.T) {
	o := mkTestOtto(t)

	f, err := ioutil.ReadFile("reduction_tests.js")
	if err != nil {
		t.Fatalf("Error reading tests js: %v", err)
	}

	_, err = o.Run(string(f))
	if err != nil {
		t.Fatalf("Error running tests: %v", err)
	}

}
