package main

import (
	"sync"
	"testing"
)

func TestBroadcast(t *testing.T) {
	wg := sync.WaitGroup{}

	b := newBroadcaster()

	for i := 0; i < 5; i++ {
		wg.Add(1)

		cch := make(chan mutation)

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
