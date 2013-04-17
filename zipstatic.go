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

func zipStatic(path, cachePath string) (*zipHandler, error) {
	log.Printf("loading static content from: %v", path)
	lastTs := time.Now()

	res, err := http.Get(path)
	if err == nil {
		defer res.Body.Close()
		if res.StatusCode == 200 {
			var f *os.File
			cachePath, f, err = mkCacheFile(cachePath, "zipstatic")
			if err != nil {
				return nil, err
			}
			_, err = io.Copy(f, res.Body)
			f.Close()
			if err != nil {
				os.Remove(cachePath)
				return nil, err
			}
			t, parseErr := parseHTTPTime(res.Header.Get("Last-Modified"))
			if parseErr == nil {
				lastTs = t
			}
		} else {
			err = fmt.Errorf("HTTP error getting %v: %v", path, res.Status)
		}
	}

	zf, zerr := zipStaticServe(cachePath, lastTs)
	if zerr != nil {
		// If we couldn't serve from cache, errors from http take precendence.
		if err != nil {
			return nil, err
		}
		return nil, zerr
	}
	return zf, nil
}

func zipStaticServe(fn string, lastTs time.Time) (*zipHandler, error) {
	zr, err := zip.OpenReader(fn)
	if err != nil {
		return nil, err
	}
	defer zr.Close()

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
