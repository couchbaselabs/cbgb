package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

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
		log.Printf("ignoring duplicate header write %v -> %v", w.status, i)
	}
}

func mkCacheFile(fname string, tempPrefix string) (
	fnameActual string, f *os.File, err error) {
	if fname == "" {
		f, err = ioutil.TempFile("", tempPrefix)
		if err != nil {
			return "", nil, err
		}
		fname = f.Name()
	} else {
		os.Remove(fname) // Ignore error as fname might not exist.
		f, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			return "", nil, err
		}
	}
	return fname, f, err
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
	Transmit(io.Writer) error
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
			err := pkt.Transmit(w)
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
	err := json.Unmarshal(d, &s)
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
		start := time.Now()
		q := ""
		if r.URL.RawQuery != "" {
			q = "?" + r.URL.RawQuery
		}

		wd := time.AfterFunc(deadline, func() {
			log.Printf("%v:%v%v is taking longer than %v",
				r.Method, r.URL.Path, q, deadline)
		})

		h(w, r)

		if !wd.Stop() {
			log.Printf("%v:%v%v eventually finished in %v",
				r.Method, r.URL.Path, q, time.Since(start))
		}
	}
}

// Simple array-based ring.
type Ring struct {
	Items []interface{}
	Last int
}

func NewRing(size int) *Ring {
	return &Ring{
		Items: make([]interface{}, size),
		Last:  size-1,
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