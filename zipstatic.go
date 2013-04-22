package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
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

func updateStatic(path, cachePath string) (time.Time, error) {
	lastTs := time.Now()

	res, err := http.Get(path)
	if err != nil {
		return lastTs, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return lastTs, fmt.Errorf("HTTP error getting %v: %v", path, res.Status)
	}

	ftmp := cachePath + ".tmp"
	os.Remove(ftmp)
	defer os.Remove(ftmp)
	f, err := os.OpenFile(ftmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return lastTs, err
	}
	defer f.Close()

	hash := sha1.New()
	_, err = io.Copy(f, io.TeeReader(res.Body, hash))
	if err != nil {
		return lastTs, err
	}
	h := `"` + hex.EncodeToString(hash.Sum(nil)) + `"`
	et := res.Header.Get("Etag")
	if et != "" && h != et {
		err := fmt.Errorf("invalid download: etag=%v, computed=%v",
			res.Header.Get("Etag"), h)
		return lastTs, err
	}

	t, parseErr := parseHTTPTime(res.Header.Get("Last-Modified"))
	if parseErr == nil {
		lastTs = t
	}

	os.Remove(cachePath)
	os.Rename(f.Name(), cachePath)

	return lastTs, nil
}

func zipStatic(path, cachePath string) (*zipHandler, error) {
	log.Printf("loading static content from: %v", path)
	lastTs, err := updateStatic(path, cachePath)
	if err != nil {
		st, e2 := os.Stat(cachePath)
		if e2 == nil {
			log.Printf("Error loading static data (%v). Proceeding with local copy",
				err)
			err = nil
			lastTs = st.ModTime()
		} else {
			err = fmt.Errorf("Both remote (%v) and local (%v) static content "+
				"is unavailable. Cannot REST", err, e2)
		}
	}

	if err != nil {
		return nil, err
	}

	zf, zerr := zipStaticServe(cachePath, lastTs)
	if zerr != nil {
		// If we couldn't serve from cache, errors from http take precendence.
		if err != nil {
			return nil, err
		}
		return nil, zerr
	}

	log.Printf("Static init complete")
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
