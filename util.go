package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/steveyen/gkvlite"
)

func parseBucketName(w http.ResponseWriter, vars map[string]string) (string, Bucket) {
	bucketName, ok := vars["bucketname"]
	if !ok {
		http.Error(w, "missing bucketName parameter", 400)
		return "", nil
	}
	bucket := buckets.Get(bucketName)
	if bucket == nil {
		http.Error(w, "no bucket with that bucketName", 404)
		return bucketName, nil
	}
	return bucketName, bucket
}

func getIntValue(f url.Values, name string, def int64) int64 {
	valstr := f.Get(name)
	if valstr == "" {
		return def
	}
	val, err := strconv.ParseInt(valstr, 10, 64)
	if err != nil {
		return def
	}
	return val
}

func jsonEncode(w io.Writer, i interface{}) error {
	if headered, ok := w.(http.ResponseWriter); ok {
		headered.Header().Set("Cache-Control", "no-cache")
		headered.Header().Set("Content-type", "application/json")
	}
	err := json.NewEncoder(w).Encode(i)
	if err != nil {
		http.Error(w.(http.ResponseWriter), err.Error(), 500)
	}
	return err
}

// Responder used for streaming results that prevents duplicate
// WriteHeader calls from working (while logging what it tried to do)
type oneResponder struct {
	w      http.ResponseWriter
	status int
}

func (w oneResponder) Header() http.Header {
	return w.w.Header()
}

func (w *oneResponder) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	return w.w.Write(b)
}

func (w *oneResponder) WriteHeader(i int) {
	if w.status == 0 {
		w.status = i
		w.w.WriteHeader(i)
	} else {
		log.Printf("Ignoring duplicate header write %v -> %v", w.status, i)
	}
}

func dumpColl(c *gkvlite.Collection, prefix string) (int, error) {
	n := 0
	err := c.VisitItemsAscend(nil, true, func(cItem *gkvlite.Item) bool {
		n++
		fmt.Printf("%v%s %#v\n", prefix, string(cItem.Key), cItem)
		return true
	})
	return n, err
}

func dumpCollAsItems(c *gkvlite.Collection, prefix string) (int, error) {
	n := 0
	var vErr error
	err := c.VisitItemsAscend(nil, true, func(cItem *gkvlite.Item) bool {
		i := &item{}
		if vErr = i.fromValueBytes(cItem.Val); vErr != nil {
			return false
		}
		fmt.Printf("%v%#v, data: %v\n", prefix, i, string(i.data))
		return true
	})
	if vErr != nil {
		return 0, vErr
	}
	return n, err
}
