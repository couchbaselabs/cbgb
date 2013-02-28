package cbgb

import (
	"sync/atomic"
	"testing"
	"time"
)

func periodicNoop(time.Time) {}

func TestPeriodicallyNil(t *testing.T) {
	stopch := make(chan bool)
	defer close(stopch)
	qt := newPeriodic(5*time.Millisecond, nil, stopch)
	if qt != nil {
		t.Fatalf("Expected nil for nil, got %v", qt)
	}
}

func TestPeriodicallyDefault(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	qt := newPeriodic(5*time.Millisecond, periodicNoop, stopch)
	time.Sleep(5 * time.Millisecond)

	age := qt.age()
	if age < 4*time.Millisecond {
		t.Fatalf("Ticker seems to be updating: %v", age)
	}
}

func TestPeriodicallyActive(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	qt := newPeriodic(5*time.Millisecond, periodicNoop, stopch)

	qt.resumeTicker(time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	age := qt.age()
	if age > 3*time.Millisecond {
		t.Fatalf("Ticker seems to not be updating: %v", age)
	}
}

func TestPeriodicallyApplyActive(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	var ran int32

	qt := newPeriodic(5*time.Millisecond, func(time.Time) {
		atomic.AddInt32(&ran, 1)
	}, stopch)

	qt.resumeTicker(time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt32(&ran) < 2 {
		t.Fatalf("Expected a few runs, got %v", ran)
	}
}

func TestPeriodicallyDeactivated(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	qt := newPeriodic(5*time.Millisecond, periodicNoop, stopch)

	qt.resumeTicker(time.Millisecond)
	time.Sleep(2 * time.Millisecond)
	qt.pauseTicker()
	time.Sleep(5 * time.Millisecond)

	age := qt.age()
	if age < 3*time.Millisecond {
		t.Fatalf("Ticker seems to still be updating: %v", age)
	}
}

func TestPeriodicallyQuiescing(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	qt := newPeriodic(5*time.Millisecond, periodicNoop, stopch)

	qt.resumeTicker(time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	age := qt.age()
	if age < 4*time.Millisecond {
		t.Fatalf("Ticker seems to have not quiesced: %v", age)
	}
}

func TestPeriodicClosedAge(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)

	qt := newPeriodic(5*time.Millisecond, periodicNoop, stopch)
	qt.resumeTicker(time.Millisecond)

	close(stopch)

	age := qt.age()
	if age != 0 {
		t.Fatalf("Got the wrong answer: %v", age)
	}
}
