package main

import (
	"archive/zip"
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func map2zip(m map[string]string) []byte {
	buf := &bytes.Buffer{}
	z := zip.NewWriter(buf)
	for k, v := range m {
		f, err := z.Create(k)
		must(err)
		_, err = f.Write([]byte(v))
		must(err)
	}
	must(z.Close())
	return buf.Bytes()
}

type fixedTransport struct {
	path string
	data []byte
}

func (c *fixedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	status := 200
	body := c.data
	if req.URL.Path != c.path {
		status = 404
		body = []byte{}
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
		Request:    req,
	}, nil
}

var brokenTransportError = errors.New("broken")

type brokenTransport struct{}

func (c *brokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, brokenTransportError
}

func TestZipStatic(t *testing.T) {
	// Restore http state
	defer func(c *http.Client) { http.DefaultClient = c }(http.DefaultClient)

	// First, I get an expected error with a broken transport.
	http.DefaultClient = &http.Client{Transport: &brokenTransport{}}

	_, err := zipStatic("/whatever", "")
	if err.Error() != "Get /whatever: broken" {
		t.Fatalf("This is not the error I was looking for: %v", err)
	}

	data := map2zip(map[string]string{
		"/a": "aye",
		"/b": "bye",
	})

	http.DefaultClient = &http.Client{
		Transport: &fixedTransport{"/sample.zip", data},
	}

	_, err = zipStatic("/missing.zip", "")
	if err.Error() != "HTTP error getting /missing.zip: Not Found" {
		t.Fatalf("This is not the error I was looking for: %v", err)
	}

	zh, err := zipStatic("/sample.zip", "")
	if err != nil {
		t.Fatalf("Error getting zip thing: %v", err)
	}

	tests := []struct {
		path    string
		content string
		status  int
	}{
		{"/a", "aye", 200},
		{"/b", "bye", 200},
		{"/c", "Not found\n", 404},
	}

	for _, test := range tests {
		rr := httptest.NewRecorder()
		req := &http.Request{
			URL: &url.URL{Path: test.path},
		}
		zh.ServeHTTP(rr, req)

		if rr.Code != test.status {
			t.Errorf("Expected status %v for %v, got %v",
				test.status, test.path, rr.Code)
			continue
		}

		if !bytes.Equal(rr.Body.Bytes(), []byte(test.content)) {
			t.Errorf("Expected body %q for %v, got %q",
				test.content, test.path, rr.Body.String())
		}
	}
}
