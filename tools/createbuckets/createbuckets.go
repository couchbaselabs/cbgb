package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/daaku/go.flagbytes"
)

var todo = flag.Int("buckets", 200, "How many buckets to create.")
var base = flag.String("baseurl", "http://127.0.0.1:8091/",
	"Base URL of your cbgb")
var concurrency = flag.Int("workers", 8,
	"How many concurrent workers creating buckets.")
var verbose = flag.Bool("v", false, "log bucket creation")
var quotaBytes = flagbytes.Bytes("quota", "100MB", "quota for each bucket")

var wg sync.WaitGroup

var cancelRedirect = fmt.Errorf("redirected")

func maybefatal(msg string, err error) {
	if err != nil {
		log.Fatalf("FATAL: %v: %v", msg, err)
	}
}

func isRedirected(e error) bool {
	if x, ok := e.(*url.Error); ok {
		return x.Err == cancelRedirect
	}
	return false
}

func worker(ustr string, ch <-chan int) {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return cancelRedirect
		},
	}

	defer wg.Done()
	for i := range ch {
		vals := url.Values{}
		vals.Set("name", fmt.Sprintf("b%06d", i))
		vals.Set("quotaBytes", fmt.Sprintf("%d", *quotaBytes))
		vals.Set("memoryOnly", "2")
		req, err := http.NewRequest("POST", ustr,
			strings.NewReader(vals.Encode()))
		maybefatal("creating request", err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := client.Do(req)
		if !isRedirected(err) {
			maybefatal("issuing request", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 303 {
			bodyText, _ := ioutil.ReadAll(resp.Body)
			log.Fatalf("HTTP error creating bucket: %v\n%s",
				resp.Status, bodyText)
		}
		if *verbose {
			log.Printf("Created bucket b%06d", i)
		}
	}
}

func main() {
	flag.Parse()

	u, err := url.Parse(*base)
	maybefatal("parsing URL", err)
	u.Path = "/_api/buckets"

	ch := make(chan int)

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(u.String(), ch)
	}

	for i := 0; i < *todo; i++ {
		ch <- i
	}
	close(ch)

	wg.Wait()
}
