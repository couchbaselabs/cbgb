package cbgb

import (
	"testing"
	"time"
)

func TestQuiescingTickerDefault(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	qt := newQTicker(5*time.Millisecond, stopch)
	time.Sleep(5 * time.Millisecond)

	age := qt.age()
	if age < 4*time.Millisecond {
		t.Fatalf("Ticker seems to be updating: %v", age)
	}
}

func TestQuiescingTickerActive(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	qt := newQTicker(5*time.Millisecond, stopch)

	qt.resumeTicker(time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	age := qt.age()
	if age > 3*time.Millisecond {
		t.Fatalf("Ticker seems to not be updating: %v", age)
	}
}

func TestQuiescingTickerApplyActive(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	var ran int

	qt := newQApply(5*time.Millisecond, func(time.Time) {
		ran++
	}, stopch)

	qt.resumeTicker(time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	age := qt.age()
	if age > 3*time.Millisecond {
		t.Fatalf("Ticker seems to not be updating: %v", age)
	}

	if ran < 3 {
		t.Fatalf("Expected a few runs, got %v", ran)
	}
}

func TestQuiescingTickerDeactivated(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	qt := newQTicker(5*time.Millisecond, stopch)

	qt.resumeTicker(time.Millisecond)
	time.Sleep(2 * time.Millisecond)
	qt.pauseTicker()
	time.Sleep(5 * time.Millisecond)

	age := qt.age()
	if age < 3*time.Millisecond {
		t.Fatalf("Ticker seems to still be updating: %v", age)
	}
}

func TestQuiescingTickerQuiescing(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)
	defer close(stopch)

	qt := newQTicker(5*time.Millisecond, stopch)

	qt.resumeTicker(time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	age := qt.age()
	if age < 4*time.Millisecond {
		t.Fatalf("Ticker seems to have not quiesced: %v", age)
	}
}

func TestQuiescingClosedAge(t *testing.T) {
	t.Parallel()

	stopch := make(chan bool)

	qt := newQTicker(5*time.Millisecond, stopch)
	qt.resumeTicker(time.Millisecond)

	close(stopch)

	age := qt.age()
	if age != 0 {
		t.Fatalf("Got the wrong answer: %v", age)
	}
}
