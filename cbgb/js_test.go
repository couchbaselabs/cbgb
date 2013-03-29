package main

import (
	"reflect"
	"testing"
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
		got := arrayPrefix(test.in, plen)

		if !reflect.DeepEqual(exp, got) {
			t.Errorf("Expected %#v for arrayPrefix(%#v, %v), got %#v",
				exp, test.in, plen, got)
		}
	}
}
