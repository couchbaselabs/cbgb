package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

var todo = flag.Int("buckets", 200, "How many buckets to create.")
var base = flag.String("baseurl", "http://127.0.0.1:8077/",
	"Base URL of your cbgb")
var concurrency = flag.Int("workers", 8,
	"How many concurrent workers creating buckets.")
var verbose = flag.Bool("v", false, "log bucket creation")

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
		req, err := http.NewRequest("POST", ustr,
			strings.NewReader(fmt.Sprintf("bucketName=b%06d", i)))
		maybefatal("creating request", err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := client.Do(req)
		if !isRedirected(err) {
			maybefatal("issuing request", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 303 {
			log.Fatalf("HTTP error creating bucket: %v", resp.Status)
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
