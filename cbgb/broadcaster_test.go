package main

import (
	"sync"
	"testing"
)

func TestBroadcast(t *testing.T) {
	wg := sync.WaitGroup{}

	b := newBroadcaster(100)
	defer b.Close()

	for i := 0; i < 5; i++ {
		wg.Add(1)

		cch := make(chan interface{})

		b.Register(cch)

		go func() {
			defer wg.Done()
			defer b.Unregister(cch)
			<-cch
		}()

	}

	b.Submit(mutation{})

	wg.Wait()
}

func TestBroadcastCleanup(t *testing.T) {
	b := newBroadcaster(100)
	b.Register(make(chan interface{}))
	b.Close()
}
