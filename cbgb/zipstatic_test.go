package main

import (
	"net/http"
	"testing"
	"time"
)

var parseTimeTests = []struct {
	h   http.Header
	err bool
}{
	{http.Header{"Date": {""}}, true},
	{http.Header{"Date": {"invalid"}}, true},
	{http.Header{"Date": {"1994-11-06T08:49:37Z00:00"}}, true},
	{http.Header{"Date": {"Sun, 06 Nov 1994 08:49:37 GMT"}}, false},
	{http.Header{"Date": {"Sunday, 06-Nov-94 08:49:37 GMT"}}, false},
	{http.Header{"Date": {"Sun Nov  6 08:49:37 1994"}}, false},
}

func TestParseTime(t *testing.T) {
	expect := time.Date(1994, 11, 6, 8, 49, 37, 0, time.UTC)
	for i, test := range parseTimeTests {
		d, err := parseHTTPTime(test.h.Get("Date"))
		if err != nil {
			if !test.err {
				t.Errorf("#%d:\n got err: %v", i, err)
			}
			continue
		}
		if test.err {
			t.Errorf("#%d:\n  should err", i)
			continue
		}
		if !expect.Equal(d) {
			t.Errorf("#%d:\n got: %v\nwant: %v", i, d, expect)
		}
	}
}
