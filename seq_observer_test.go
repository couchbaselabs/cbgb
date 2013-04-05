package main

import (
	"testing"
	"time"
)

var (
	seqKey1 = sequenceId("a")
	seqKey2 = sequenceId("b")
)

func TestSeqObsNoReg(t *testing.T) {
	s := newSequencePubSub()
	s.Pub(seqKey1, 845)
	s.Stop()
}

func TestSeqObsStop(t *testing.T) {
	s := newSequencePubSub()
	ch := s.Sub(seqKey1, 4)
	go s.Stop()
	val, ok := <-ch
	if ok {
		t.Errorf("Expected close on stop, got %v", val)
	}
}

func assertNoSeqMessage(t *testing.T, chs ...<-chan int64) {
	for _, ch := range chs {
		select {
		case got := <-ch:
			t.Fatalf("Expected no message, got %v", got)
		case <-time.After(time.Millisecond * 3):
			// OK
		}
	}
}

func tseqmesg(t *testing.T, ch <-chan int64) int64 {
	select {
	case got := <-ch:
		return got
	case <-time.After(time.Millisecond * 3):
		t.Fatalf("Found no message where one was expected")
	}
	panic("unreachable")
}

func TestSeqObsPub(t *testing.T) {
	s := newSequencePubSub()
	defer s.Stop()
	ch1a := s.Sub(seqKey1, 4)
	ch1b := s.Sub(seqKey1, 2)
	ch1c := s.Sub(seqKey1, 10)

	ch2a := s.Sub(seqKey2, 3)

	s.Pub(seqKey1, 3)
	if got := tseqmesg(t, ch1b); got != 3 {
		t.Fatalf("Expected 3, got %v", got)
	}
	assertNoSeqMessage(t, ch1a, ch1c, ch2a)

	s.Pub(seqKey1, 15)
	for _, ch := range []<-chan int64{ch1a, ch1c} {
		if got := tseqmesg(t, ch); got != 15 {
			t.Fatalf("Expected 15, got %v", got)
		}
	}
	assertNoSeqMessage(t, ch2a)
}
