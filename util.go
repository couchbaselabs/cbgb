package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dustin/yellow"
	"github.com/steveyen/gkvlite"
)

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

func mustEncode(w io.Writer, i interface{}) {
	if headered, ok := w.(http.ResponseWriter); ok {
		headered.Header().Set("Cache-Control", "no-cache")
		headered.Header().Set("Content-type", "application/json")
	}
	err := json.NewEncoder(w).Encode(i)
	if err != nil {
		log.Printf("Failed to marshal %v: %v", i, err)
		http.Error(w.(http.ResponseWriter), err.Error(), 500)
	}
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
		log.Printf("ignoring duplicate header write %v -> %v", w.status, i)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// You can use fmt.Printf() for the printf param.
func dumpColl(printf func(format string, a ...interface{}) (n int, err error),
	c *gkvlite.Collection, prefix string) (int, error) {
	n := 0
	err := c.VisitItemsAscend(nil, true, func(cItem *gkvlite.Item) bool {
		n++
		printf("%v%s %#v\n", prefix, string(cItem.Key), cItem)
		return true
	})
	return n, err
}

// You can use fmt.Printf() for the printf param.
func dumpCollAsItems(printf func(format string, a ...interface{}) (n int, err error),
	c *gkvlite.Collection, prefix string) (int, error) {
	n := 0
	var vErr error
	err := c.VisitItemsAscend(nil, true, func(cItem *gkvlite.Item) bool {
		i := &item{}
		if vErr = i.fromValueBytes(cItem.Val); vErr != nil {
			return false
		}
		n++
		printf("%v%#v, data: %v\n", prefix, i, string(i.data))
		return true
	})
	if vErr != nil {
		return 0, vErr
	}
	return n, err
}

type Form interface {
	FormValue(key string) string
}

func addInt64(x, y int64) int64 {
	return x + y
}

func subInt64(x, y int64) int64 {
	return x - y
}

type funreq struct {
	fun func()
	res chan bool
}

type transmissible interface {
	Transmit(io.Writer) (int, error)
}

// Given an io.Writer, return a channel that can be fed things that
// can write themselves to an io.Writer and a channel that will return
// any encountered error.
//
// The user of this transmitter *MUST* close the input channel to
// indicate no more messages will be sent.
//
// There will be exactly one error written to the error channel either
// after successfully transmitting the entire stream (in which case it
// will be nil) or on any transmission error.
//
// Unless your transmissible stream is finite, it's recommended to
// perform a non-blocking receive of the error stream to check for
// brokenness so you know to stop transmitting.
func transmitPackets(w io.Writer) (chan<- transmissible, <-chan error) {
	ch := make(chan transmissible)
	errs := make(chan error, 1)
	go func() {
		for pkt := range ch {
			_, err := pkt.Transmit(w)
			if err != nil {
				errs <- err
				for _ = range ch {
					// Eat the input
				}
				return
			}
		}
		errs <- nil
	}()
	return ch, errs
}

type Bytes []byte

func (a *Bytes) MarshalJSON() ([]byte, error) {
	s := url.QueryEscape(string(*a))
	return json.Marshal(s)
}

func (a *Bytes) UnmarshalJSON(d []byte) error {
	var s string
	err := jsonUnmarshal(d, &s)
	if err != nil {
		return err
	}
	x, err := url.QueryUnescape(s)
	if err == nil {
		*a = Bytes(x)
	}
	return err
}

func (a *Bytes) String() string {
	return string(*a)
}

func isDir(path string) bool {
	if finfo, err := os.Stat(path); err != nil || !finfo.IsDir() {
		return false
	}
	return true // TODO: check for writability.
}

// TODO: replace with proper UUID implementation
func CreateNewUUID() string {
	val1 := rand.Int63()
	val2 := rand.Int63()
	uuid := fmt.Sprintf("%x%x", val1, val2)
	return uuid
}

func deadlinedHandler(deadline time.Duration, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := ""
		if r.URL.RawQuery != "" {
			q = "?" + r.URL.RawQuery
		}

		defer yellow.DeadlineLog(deadline, "%v:%v%v with a deadline of %v",
			r.Method, r.URL.Path, q, deadline).Done()
		h(w, r)
	}
}

// Simple array-based ring.
type Ring struct {
	Items []interface{}
	Last  int
}

func NewRing(size int) *Ring {
	return &Ring{
		Items: make([]interface{}, size),
		Last:  size - 1,
	}
}

func (r *Ring) Push(v interface{}) {
	r.Last++
	if r.Last >= len(r.Items) {
		r.Last = 0
	}
	r.Items[r.Last] = v
}

func (r *Ring) Visit(f func(interface{})) {
	i := r.Last
	for {
		i++
		if i >= len(r.Items) {
			i = 0
		}
		f(r.Items[i])
		if i == r.Last {
			break
		}
	}
}

// Converts non-nil error items to a []error.  On empty, returns nil
// instead of zero-length array for less garbage.
func RingToErrors(r *Ring) []error {
	var res []error
	r.Visit(func(v interface{}) {
		if v != nil {
			err, ok := v.(error)
			if ok && err != nil {
				if res == nil {
					res = []error{}
				}
				res = append(res, err)
			}
		}
	})
	return res
}

// Converts non-nil/non-"" string items to a []string.  On empty,
// returns nil instead of zero-length array for less garbage.
func RingToStrings(r *Ring) []string {
	var res []string
	r.Visit(func(v interface{}) {
		if v != nil {
			s, ok := v.(string)
			if ok && s != "" {
				if res == nil {
					res = []string{}
				}
				res = append(res, s)
			}
		}
	})
	return res
}

func parseFileNameSuffix(fname string) (string, error) {
	ext := filepath.Ext(fname)
	if len(ext) <= 1 {
		return "", fmt.Errorf("no suffix for fname: %v", fname)
	}
	return ext[1:], nil
}
