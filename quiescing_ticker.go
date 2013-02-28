package cbgb

import (
	"time"
)

// An auto-quiescing ticker.
//
// This is similar to a time.Ticker, but can automatically stop
// producing ticks when idle.
type qticker struct {
	ctl      chan *time.Ticker
	lastch   chan chan time.Time
	quiescer *time.Ticker

	f func(time.Time)

	// This channel produces ticks.
	C chan time.Time
	// Anything sent on this channel shuts us down (including close)
	StopChan <-chan bool
}

// Build and start a new ticker.
//
// quiescePeriod is how frequently the ticker looks for activity.
// stopChan is used to signal the ticker to stop.  By specifying a
// channel here, the same channel can be used by many related
// components to allow a single signal to cascade multiple component
// shutdowns.
func newQTicker(quiescePeriod time.Duration,
	stopChan <-chan bool) *qticker {

	rv := &qticker{
		ctl:      make(chan *time.Ticker),
		quiescer: time.NewTicker(quiescePeriod),
		lastch:   make(chan chan time.Time),
		C:        make(chan time.Time, 1),
		StopChan: stopChan,
	}

	go rv.run()

	return rv
}

func newQApply(quiescePeriod time.Duration, f func(time.Time),
	stopChan <-chan bool) *qticker {

	rv := &qticker{
		f:        f,
		ctl:      make(chan *time.Ticker),
		quiescer: time.NewTicker(quiescePeriod),
		lastch:   make(chan chan time.Time),
		C:        make(chan time.Time, 1),
		StopChan: stopChan,
	}

	go rv.run()

	return rv
}

func (q *qticker) run() {
	defer q.quiescer.Stop()
	defer close(q.C)

	var ticker *time.Ticker
	var timech <-chan time.Time
	latestTick := time.Now()
	reqs := 0
	for {
		select {
		case t := <-timech: // Ticker sent us a time
			latestTick = t
			select {
			case q.C <- t:
			default:
			}
			if q.f != nil {
				q.f(t)
			}
		case <-q.StopChan: // Shutdown requested
			if ticker != nil {
				ticker.Stop()
			}
			return
		case <-q.quiescer.C: // Periodic check for activity
			if reqs == 0 && ticker != nil {
				ticker.Stop()
				ticker = nil
				timech = nil
			}
			reqs = 0
		case ch := <-q.lastch: // Activity marker
			reqs++
			ch <- latestTick
		case c := <-q.ctl:
			// We are receiving a new ticker.  If we
			// already had one, shut it down.  If we
			// receive one, set it to our channel.  This
			// allows both starting and stopping stat
			// collection in a fairly straightforward way.
			if ticker != nil {
				ticker.Stop()
			}
			ticker = c
			timech = nil
			if ticker != nil {
				timech = ticker.C
			}
			reqs = 0
		}
	}
}

// Resume/start a ticker ticking at the given interval.
func (q *qticker) resumeTicker(d time.Duration) {
	q.ctl <- time.NewTicker(d)
}

// Pause a (potentially) running ticker.
func (q *qticker) pauseTicker() {
	select {
	case q.ctl <- nil:
	case <-q.StopChan:
	}
}

// Get the age of the latest tick delivered from this ticker.
//
// This has a side-effect of marking the timer as active.  The ticker
// will go inactive after an inactivity period where it's not
// receiving sufficient age requests.
func (q *qticker) age() time.Duration {
	ch := make(chan time.Time)
	select {
	case q.lastch <- ch:
	case <-q.StopChan:
		return 0
	}
	return time.Since(<-ch)
}
