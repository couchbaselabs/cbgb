package main

import (
	"errors"
	"net/url"
	"testing"

	"github.com/steveyen/gkvlite"
)

func TestDumpColls(t *testing.T) {
	store, _ := gkvlite.NewStore(nil)
	c := store.SetCollection("test", nil)
	n, err := dumpColl(c, "")
	if err != nil || n != 0 {
		t.Errorf("expected dumpColl on empty coll to work, got: %v, %v", n, err)
	}
	n, err = dumpCollAsItems(c, "")
	if err != nil || n != 0 {
		t.Errorf("expected dumpCollAsItems on empty coll to work, got: %v, %v", n, err)
	}
}

func TestGetHTTPInt(t *testing.T) {
	form := url.Values{
		"word":    []string{"up"},
		"number":  []string{"1"},
		"numbers": []string{"1", "2"},
	}

	def := int64(28485)

	tests := map[string]int64{
		"word":        def,
		"number":      1,
		"numbers":     1,
		"nonexistent": def,
	}

	for k, v := range tests {
		got := getIntValue(form, k, def)
		if got != v {
			t.Errorf("Expected %v for %v (%q) got %v",
				v, k, k[v], got)
		}
	}
}

func TestMust(t *testing.T) {
	must(nil) // no problem

	exp := errors.New("the spanish inquisition")

	var err interface{}
	func() {
		defer func() { err = recover() }()
		must(exp)
	}()

	if err != exp {
		t.Errorf("Expected %v, got %v", exp, err)
	}
}
