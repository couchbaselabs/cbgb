package main

import (
	"errors"
	"net/http/httptest"
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

func TestOneResponderImplicit(t *testing.T) {
	rr := httptest.NewRecorder()
	or := oneResponder{w: rr}

	or.Header().Set("a", "1")
	if rr.Header().Get("a") != "1" {
		t.Fatalf("Expected header a to be 1, but it's %q",
			rr.Header().Get("a"))
	}

	or.Write([]byte{'x'})
	if rr.Code != 200 {
		t.Fatalf("Expected write to lead to code 200, it's %v", rr.Code)
	}

	if rr.Body.String() != "x" {
		t.Fatalf("Expected an x, but it's %q", rr.Body.String())
	}

	or.WriteHeader(500)
	if rr.Code != 200 {
		t.Fatalf("Expected code to still be 200, it's %v", rr.Code)
	}
}

func TestOneResponderExplicit(t *testing.T) {
	rr := httptest.NewRecorder()
	or := oneResponder{w: rr}

	or.WriteHeader(200)
	if rr.Code != 200 {
		t.Fatalf("Expected code to be 200, it's %v", rr.Code)
	}

	or.WriteHeader(500)
	if rr.Code != 200 {
		t.Fatalf("Expected code to still be 200, it's %v", rr.Code)
	}
}
