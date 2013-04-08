package main

import (
	"reflect"
	"testing"

	"github.com/robertkrimen/otto"
)

func TestArrayPrefix(t *testing.T) {
	sslice := []interface{}{"a", "b", "c"}
	islice := []interface{}{1, 2, 3}

	tests := []struct {
		in   interface{}
		plen int
		exp  interface{}
	}{
		{sslice, 0, nil},
		{sslice, 1, []interface{}{"a"}},
		{sslice, 2, []interface{}{"a", "b"}},
		{sslice, 3, []interface{}{"a", "b", "c"}},
		{sslice, 4, []interface{}{"a", "b", "c"}},

		{islice, 0, nil},
		{islice, 1, []interface{}{1}},
		{islice, 2, []interface{}{1, 2}},
		{islice, 3, []interface{}{1, 2, 3}},
		{islice, 4, []interface{}{1, 2, 3}},

		{"str", 0, nil},
		{"str", 1, "str"},
		{"str", 2, nil},

		{1, 0, nil},
		{1, 1, 1},
		{1, 2, nil},
	}

	for _, test := range tests {
		exp := test.exp
		plen := test.plen
		got := ArrayPrefix(test.in, plen)

		if !reflect.DeepEqual(exp, got) {
			t.Errorf("Expected %#v for ArrayPrefix(%#v, %v), got %#v",
				exp, test.in, plen, got)
		}
	}
}

func TestOttoFromGoArray(t *testing.T) {
	o := otto.New()
	_, err := OttoFromGoArray(o, nil)
	if err == nil {
		t.Errorf("expected err for nil array to OttoFromGoArray")
	}
}

func TestBadOttoNewFunction(t *testing.T) {
	o := otto.New()
	_, err := OttoNewFunction(o, ".")
	if err == nil {
		t.Errorf("expected err for bad input to OttoNewFunction")
	}
	_, err = OttoNewFunction(o, "123")
	if err == nil {
		t.Errorf("expected err for non-func to OttoNewFunction")
	}
}
