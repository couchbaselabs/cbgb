package main

import (
	"bytes"
	"os"
	"sync"
	"testing"
)

func TestDumpOnSignal(t *testing.T) {
	wg := sync.WaitGroup{}

	h := NewSigHandler(os.Interrupt)
	buf := &bytes.Buffer{}
	h.hook = func() {
		h.Close()
		wg.Done()
	}
	h.w = buf
	me, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Errorf("Couldn't find current process: %v", err)
	}
	wg.Add(1)
	go h.Run()

	err = me.Signal(os.Interrupt)
	if err != nil {
		t.Errorf("Error signaling me: %v", err)
	}

	wg.Wait()
	if buf.Len() < 10 {
		t.Errorf("Expected some stuff, got %v", buf.Bytes())
	}
}
