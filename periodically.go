package cbgb

import (
	"time"
)

type periodically struct {
	ctl      chan *time.Ticker
	lastch   chan chan time.Time
	quiescer *time.Ticker
	f        func(time.Time)
	stopChan <-chan bool
}

// Build a new, quiesceable periodic function invoker.
func newPeriodic(quiescePeriod time.Duration, f func(time.Time),
	stopChan <-chan bool) *periodically {

	if f == nil {
		return nil
	}

	rv := &periodically{
		f:        f,
		ctl:      make(chan *time.Ticker),
		quiescer: time.NewTicker(quiescePeriod),
		lastch:   make(chan chan time.Time),
		stopChan: stopChan,
	}

	go rv.run()

	return rv
}

func (p *periodically) run() {
	defer p.quiescer.Stop()

	var ticker *time.Ticker
	var timech <-chan time.Time
	latestTick := time.Now()
	reqs := 0
	for {
		select {
		case t := <-timech: // Ticker sent us a time
			latestTick = t
			p.f(t)
		case <-p.stopChan: // Shutdown requested
			if ticker != nil {
				ticker.Stop()
			}
			return
		case <-p.quiescer.C: // Periodic check for activity
			if reqs == 0 && ticker != nil {
				ticker.Stop()
				ticker = nil
				timech = nil
			}
			reqs = 0
		case ch := <-p.lastch: // Activity marker
			reqs++
			ch <- latestTick
		case c := <-p.ctl:
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
func (p *periodically) resumeTicker(d time.Duration) {
	p.ctl <- time.NewTicker(d)
}

// Pause a (potentially) running ticker.
func (p *periodically) pauseTicker() {
	select {
	case p.ctl <- nil:
	case <-p.stopChan:
	}
}

// Get the age of the latest tick delivered from this ticker.
//
// This has a side-effect of marking the timer as active.  The ticker
// will go inactive after an inactivity period where it's not
// receiving sufficient age requests.
func (p *periodically) age() time.Duration {
	ch := make(chan time.Time)
	select {
	case p.lastch <- ch:
	case <-p.stopChan:
		return 0
	}
	return time.Since(<-ch)
}
