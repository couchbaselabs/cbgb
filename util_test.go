package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/dustin/gomemcached"
	"github.com/steveyen/gkvlite"
)

func TestDumpColls(t *testing.T) {
	x := 0
	printf := func(format string, a ...interface{}) (n int, err error) {
		x++
		return 0, nil
	}

	store, _ := gkvlite.NewStore(nil)
	c := store.SetCollection("test", nil)
	n, err := dumpColl(printf, c, "")
	if err != nil || n != 0 || x != 0 {
		t.Errorf("expected dumpColl on empty coll to work, got: %v, %v", n, err)
	}
	n, err = dumpCollAsItems(printf, c, "")
	if err != nil || n != 0 || x != 0 {
		t.Errorf("expected dumpCollAsItems on empty coll to work, got: %v, %v", n, err)
	}
	c.Set([]byte("test-key"), (&item{}).toValueBytes())
	x = 0
	n, err = dumpColl(printf, c, "")
	if err != nil || n != 1 || x != 1 {
		t.Errorf("expected dumpColl on 1-item coll to work, got: %v, %v", n, err)
	}
	x = 0
	n, err = dumpCollAsItems(printf, c, "")
	if err != nil || n != 1 || x != 1 {
		t.Errorf("expected dumpCollAsItems on 1-item coll to work, got: %v, %v", n, err)
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

// Run through the sessionLoop code with a quit command.
//
// This test doesn't do much other than confirm that the session loop
// actually would terminate the real session goroutine on quit (by
// completing).
func TestSessionLoop(t *testing.T) {
	req := &gomemcached.MCRequest{
		Opcode: gomemcached.QUIT,
	}

	rh := &reqHandler{}

	sessionLoop(rwCloser{bytes.NewBuffer(req.Bytes())}, "test", rh,
		func() {})
}

func TestBytesEncoder(t *testing.T) {
	tests := map[string]string{
		"simple": `"simple"`,
		"O'Hair": `"O%27Hair"`,
	}

	for in, out := range tests {
		b := Bytes(in)
		got, err := json.Marshal(&b)
		if err != nil {
			t.Errorf("Error marshaling %v", in)
		}
		if string(got) != out {
			t.Errorf("Expected %s, got %s", out, got)
		}
	}
}

func TestBytesDecoder(t *testing.T) {
	pos := map[string]string{
		`"simple"`:   "simple",
		`"O%27Hair"`: "O'Hair",
	}

	for in, out := range pos {
		b := Bytes{}
		err := json.Unmarshal([]byte(in), &b)
		if err != nil {
			t.Errorf("Error unmarshaling %v", in)
		}
		if out != b.String() {
			t.Errorf("Expected %v for %v, got %v", out, in, b)
		}
	}

	neg := []string{"xxx no quotes", `"invalid esc %2x"`}

	for _, in := range neg {
		b := Bytes{}
		err := json.Unmarshal([]byte(in), &b)
		if err == nil {
			t.Errorf("Expected error unmarshaling %v, got %v", in, b)
		}
	}

	// This is odd looking, but I use the internal decoder
	// directly since the interior error is just about impossible
	// to encounter otherwise.
	for _, in := range neg {
		b := Bytes{}
		err := b.UnmarshalJSON([]byte(in))
		if err == nil {
			t.Errorf("Expected error unmarshaling %v, got %v", in, b)
		}
	}
}

func TestDirIsDir(t *testing.T) {
	if !isDir(".") {
		t.Errorf("expected . to be a dir")
	}
}

func TestRing(t *testing.T) {
	r := NewRing(1)
	r.Push(0)

	exp := []int{0}
	cur := 0
	r.Visit(func(v interface{}) {
		if v != exp[cur] {
			t.Errorf("ring visit expected %v, got: %v", exp[cur], v)
		}
		cur++
	})
	if cur != len(exp) {
		t.Errorf("expected to see: %#v, but got one more", exp)
	}

	r.Push(1)

	exp = []int{1}
	cur = 0
	r.Visit(func(v interface{}) {
		if v != exp[cur] {
			t.Errorf("ring visit expected %v, got: %v", exp[cur], v)
		}
		cur++
	})
	if cur != len(exp) {
		t.Errorf("expected to see: %#v, but got one more", exp)
	}

	r = NewRing(2)
	r.Push(0)
	r.Push(1)
	r.Push(2)
	r.Push(3)

	exp = []int{2, 3}
	cur = 0
	r.Visit(func(v interface{}) {
		if v != exp[cur] {
			t.Errorf("ring visit expected %v, got: %v", exp[cur], v)
		}
		cur++
	})
	if cur != len(exp) {
		t.Errorf("expected to see: %#v, but got one more", exp)
	}
}

func TestRingConversions(t *testing.T) {
	e0 := fmt.Errorf("e0")
	e1 := fmt.Errorf("e1")

	emptyRing := NewRing(10)

	justNums := NewRing(5)
	for i := 0; i < 6; i ++ {
		justNums.Push(i)
	}

	r := NewRing(10)
	r.Push(0)
	r.Push("hi")
	r.Push("bye")
	r.Push(e0)
	r.Push(1)
	r.Push(e1)

	checkStrings := func(got, exp []string) {
		if len(got) != len(exp) {
			t.Errorf("got vs exp len mismatch, %#v, %#v", got, exp)
		}
		for i, g := range got {
			if g != exp[i] {
				t.Errorf("got vs exp item mismatch, %#v, %#v", g, exp[i])
			}
		}
	}
	checkStrings(RingToStrings(r), []string{"hi", "bye"})
	checkStrings(RingToStrings(emptyRing), []string{})
	checkStrings(RingToStrings(justNums), []string{})

	checkErrors := func(got, exp []error) {
		if len(got) != len(exp) {
			t.Errorf("got vs exp len mismatch, %#v, %#v", got, exp)
		}
		for i, g := range got {
			if g != exp[i] {
				t.Errorf("got vs exp item mismatch, %#v, %#v", g, exp[i])
			}
		}
	}
	checkErrors(RingToErrors(r), []error{e0, e1})
	checkErrors(RingToErrors(emptyRing), []error{})
	checkErrors(RingToErrors(justNums), []error{})
}
