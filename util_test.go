package main

import (
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
