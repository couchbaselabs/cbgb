package main

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

type zipHandler struct {
	ts   time.Time
	data map[string][]byte
}

func (z *zipHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	data, ok := z.data[req.URL.Path]
	if !ok {
		http.Error(w, "Not found", 404)
		return
	}

	http.ServeContent(w, req, req.URL.Path, z.ts, bytes.NewReader([]byte(data)))
}

var httpTimeFormats = []string{
	http.TimeFormat,
	time.RFC850,
	time.ANSIC,
}

// Stolen from go ~1.1
func parseHTTPTime(text string) (t time.Time, err error) {
	for _, layout := range httpTimeFormats {
		t, err = time.Parse(layout, text)
		if err == nil {
			return
		}
	}
	return
}

func zipStatic(path string) (*zipHandler, error) {
	log.Printf("loading static content from %v", path)
	res, err := http.Get(path)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP error getting %v: %v", path, res.Status)
	}

	f, err := ioutil.TempFile("", "staticzip")
	if err != nil {
		return nil, err
	}
	fn := f.Name()
	defer os.Remove(fn)

	_, err = io.Copy(f, res.Body)
	if err != nil {
		f.Close()
		return nil, err
	}
	f.Close()

	zr, err := zip.OpenReader(fn)
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	lastTs, err := parseHTTPTime(res.Header.Get("Last-Modified"))
	if err != nil {
		lastTs = time.Now()
	}

	zf := &zipHandler{
		ts:   lastTs,
		data: map[string][]byte{},
	}

	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}

		d, err := ioutil.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		rc.Close()

		zf.data[f.Name] = d
	}

	return zf, nil
}
