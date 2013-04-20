package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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

func TestTempMkCacheFile(t *testing.T) {
	fn0, f0, err := mkCacheFile("", "testing")
	if err != nil {
		t.Errorf("expected no err, err: %v", err)
	}
	f0.Close()
	fn1, f1, err := mkCacheFile("", "testing")
	if err != nil {
		t.Errorf("expected no err, err: %v", err)
	}
	f1.Close()
	if fn0 == fn1 {
		t.Errorf("expected cache files to be different")
	}
	os.Remove(fn0)
	os.Remove(fn1)
}

func TestMkCacheFile(t *testing.T) {
	testDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(testDir)
	fn0, f0, err := mkCacheFile(filepath.Join(testDir, "tmcf"), "")
	if err != nil {
		t.Errorf("expected no err, err: %v", err)
	}
	f0.Close()
	fn1, f1, err := mkCacheFile(filepath.Join(testDir, "tmcf"), "")
	if err != nil {
		t.Errorf("expected no err, err: %v", err)
	}
	f1.Close()
	if fn0 != fn1 {
		t.Errorf("expected cache files to be same")
	}
	os.Remove(fn0)
	os.Remove(fn1)

	_, _, err = mkCacheFile(filepath.Join(testDir, "not-a-dir", "tmcf"), "")
	if err == nil {
		t.Errorf("expected failed mkCacheFile on bad dir")
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
