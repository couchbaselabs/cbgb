package main

import (
	"sync/atomic"
	"testing"
	"time"
)

// Configure up some default periodicallys for tests
func init() {
	initPeriodically()
}

func periodicNoop(time.Time) {}

func TestPeriodicallyNoPeriod(t *testing.T) {
	stopch := make(chan bool)
	defer close(stopch)
	qt := newPeriodically(0, 10)
	if qt != nil {
		t.Fatalf("Expected nil for nil, got %v", qt)
	}
}

func TestPeriodicallyNoWorkers(t *testing.T) {
	stopch := make(chan bool)
	defer close(stopch)
	qt := newPeriodically(time.Millisecond, 0)
	if qt != nil {
		t.Fatalf("Expected nil for nil, got %v", qt)
	}
}

func TestPeriodicallyNormal(t *testing.T) {
	stopch := make(chan bool)
	defer close(stopch)

	qt := newPeriodically(time.Millisecond, 1)
	defer qt.Stop()

	ran := int32(0)
	qt.Register(stopch, func(time.Time) bool {
		atomic.AddInt32(&ran, 1)
		return true
	})

	time.Sleep(5 * time.Millisecond)

	if atomic.LoadInt32(&ran) < 1 {
		t.Fatalf("Ticker seems to not be updating with real time: %v", ran)
	}
}

type simTime struct {
	ch      chan time.Time
	stopped chan bool
}

func (s simTime) C() <-chan time.Time {
	return s.ch
}

func (s simTime) Stop() {
	close(s.stopped)
}

func mkSimTime() *simTime {
	return &simTime{
		make(chan time.Time),
		make(chan bool),
	}
}

func TestPeriodicallySimulated(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	timesrc := mkSimTime()
	qt := newPeriodicallyInt(timesrc, 0, 1)
	defer qt.Stop()

	ran := int32(0)
	qt.Register(stopch, func(time.Time) bool {
		atomic.AddInt32(&ran, 1)
		return true
	})

	for i := 0; i < 5; i++ {
		timesrc.ch <- time.Now()
	}

	qt.Stop()
	<-timesrc.stopped

	if atomic.LoadInt32(&ran) != 5 {
		t.Fatalf("Ticker did not update expected number of times: %v", ran)
	}
}

func TestPeriodicallyUnregister(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	timesrc := mkSimTime()
	qt := newPeriodicallyInt(timesrc, 0, 1)
	defer qt.Stop()

	ran := int32(0)
	qt.Register(stopch, func(time.Time) bool {
		atomic.AddInt32(&ran, 1)
		return true
	})

	for i := 0; i < 5; i++ {
		timesrc.ch <- time.Now()
		qt.Unregister(stopch)
	}

	qt.Stop()
	<-timesrc.stopped

	if atomic.LoadInt32(&ran) != 1 {
		t.Fatalf("Ticker did not update expected number of times: %v", ran)
	}
}

func TestPeriodicallyPassiveUnregister(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)

	timesrc := mkSimTime()
	qt := newPeriodicallyInt(timesrc, 0, 1)
	defer qt.Stop()

	ran := int32(0)
	qt.Register(stopch, func(time.Time) bool {
		if atomic.LoadInt32(&ran) == 0 {
			close(stopch)
		}
		atomic.AddInt32(&ran, 1)
		return true
	})

	for i := 0; i < 5; i++ {
		timesrc.ch <- time.Now()
	}

	qt.Stop()
	<-timesrc.stopped

	// It's possible to get a run or two in before closing, but we
	// shouldn't see more than two because there's only one
	// worker.  That means one check with no worker, and one check
	// while the worker is busy.  Beyond that, either the workers
	// are all busy or we've detected it missing.
	if atomic.LoadInt32(&ran) > 2 {
		t.Fatalf("Ticker did not update expected number of times: %v", ran)
	}
}

func TestPeriodicallyRequestNoIteration(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	timesrc := mkSimTime()
	qt := newPeriodicallyInt(timesrc, 0, 1)
	defer qt.Stop()

	ran := int32(0)
	qt.Register(stopch, func(time.Time) bool {
		atomic.AddInt32(&ran, 1)
		return false
	})

	for i := 0; i < 5; i++ {
		timesrc.ch <- time.Now()
	}

	qt.Stop()
	<-timesrc.stopped

	// The closing rules are similar to those above.
	if atomic.LoadInt32(&ran) > 2 {
		t.Fatalf("Ticker did not update expected number of times: %v", ran)
	}
}
