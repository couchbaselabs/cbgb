package main

import (
	"time"
)

const defaultCtlBuf = 16

type periodicRequest struct {
	k <-chan bool
	f func(time.Time) bool
}

type pFuncs map[<-chan bool]func(time.Time) bool

type periodically struct {
	funcs   pFuncs
	ctl     chan periodicRequest
	ticker  tickSrc
	sem     chan bool
	running chan bool
}

type tickSrc interface {
	C() <-chan time.Time
	Stop() // hammertime
}

type realTicker struct {
	t *time.Ticker
}

func (t realTicker) C() <-chan time.Time {
	return t.t.C
}

func (t realTicker) Stop() {
	t.t.Stop()
}

func newPeriodically(period time.Duration, workers int) *periodically {
	if period == 0 {
		return nil
	}

	return newPeriodicallyInt(realTicker{time.NewTicker(period)},
		defaultCtlBuf, workers)
}

// When you want to supply your own time source.
func newPeriodicallyInt(ticker tickSrc, ctlbuf, workers int) *periodically {
	if workers < 1 {
		return nil
	}
	rv := &periodically{
		funcs:   make(pFuncs),
		ctl:     make(chan periodicRequest, ctlbuf),
		ticker:  ticker,
		sem:     make(chan bool, workers),
		running: make(chan bool),
	}
	go rv.service()
	return rv
}

func (p *periodically) service() {
	defer close(p.running)
	defer p.ticker.Stop()
	var workFinished <-chan time.Time
	tick := p.ticker.C()
	additions := pFuncs{}
	removals := []<-chan bool{}
	for {
		select {
		case t := <-tick:
			workFinished = p.doWork(t, additions, removals)
			additions = pFuncs{}
			removals = removals[:0]
			tick = nil
		case <-workFinished:
			tick = p.ticker.C()
			workFinished = nil
		case req := <-p.ctl:
			switch {
			case req.k == nil && req.f == nil:
				return
			case req.f == nil:
				removals = append(removals, req.k)
			default:
				additions[req.k] = req.f
			}
		}
	}
}

func (p *periodically) runTask(t time.Time,
	ch <-chan bool, f func(time.Time) bool) chan bool {
	rv := make(chan bool, 1)
	p.sem <- true
	go func() {
		defer func() { <-p.sem }()
		rv <- f(t)
	}()
	return rv
}

func (p *periodically) doWork(t time.Time,
	additions pFuncs, removals []<-chan bool) <-chan time.Time {

	rv := make(chan time.Time)
	// first, fix up the pfuncs
	for _, ch := range removals {
		delete(additions, ch)
		delete(p.funcs, ch)
	}
	for ch, f := range additions {
		p.funcs[ch] = f
	}

	go func() {
		results := map[<-chan bool]chan bool{}
		for ch, f := range p.funcs {
			select {
			case <-ch:
				// This one signaled it's gone now
				delete(p.funcs, ch)
			default:
				results[ch] = p.runTask(t, ch, f)
			}
		}
		// Harvest the results and verify they want to keep tickin'
		for kch, rvch := range results {
			if !<-rvch {
				delete(p.funcs, kch)
			}
		}
		rv <- t
	}()
	return rv
}

func (p *periodically) Stop() {
	select {
	case p.ctl <- periodicRequest{}:
	case <-p.running:
		// already stopped
	}
}

func (p *periodically) Register(k <-chan bool, f func(time.Time) bool) {
	p.ctl <- periodicRequest{k, f}
}

func (p *periodically) Unregister(k <-chan bool) {
	p.ctl <- periodicRequest{k, nil}
}
