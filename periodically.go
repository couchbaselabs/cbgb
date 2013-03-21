package cbgb

import (
	"time"
)

const defaultCtlBuf = 16

type periodicRequest struct {
	k <-chan bool
	f func(time.Time) bool
}

type periodicWorkItem struct {
	t  time.Time
	f  func(time.Time) bool
	rv chan bool
}

type periodically struct {
	funcs   map[<-chan bool]func(time.Time) bool
	ctl     chan periodicRequest
	ticker  tickSrc
	work    chan periodicWorkItem
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
		funcs:   map[<-chan bool]func(time.Time) bool{},
		ctl:     make(chan periodicRequest, ctlbuf),
		ticker:  ticker,
		work:    make(chan periodicWorkItem),
		running: make(chan bool),
	}
	for i := 0; i < workers; i++ {
		go periodicWorker(rv.work)
	}
	go rv.service()
	return rv
}

func periodicWorker(ch <-chan periodicWorkItem) {
	for i := range ch {
		i.rv <- i.f(i.t)
	}
}

func (p *periodically) service() {
	defer close(p.work)
	defer close(p.running)
	defer p.ticker.Stop()
	for {
		select {
		case t := <-p.ticker.C():
			p.doWork(t)
		case req := <-p.ctl:
			switch {
			case req.k == nil && req.f == nil:
				return
			case req.f == nil:
				delete(p.funcs, req.k)
			default:
				p.funcs[req.k] = req.f
			}
		}
	}
}

func (p *periodically) doWork(t time.Time) {
	var remove []<-chan bool
	results := map[<-chan bool]chan bool{}
	for ch, f := range p.funcs {
		select {
		case <-ch:
			remove = append(remove, ch)
		default:
			rv := make(chan bool, 1)
			p.work <- periodicWorkItem{t, f, rv}
			results[ch] = rv
		}
	}
	// Harvest the results and verify they want to keep tickin'
	for kch, rvch := range results {
		if !<-rvch {
			delete(p.funcs, kch)
		}
	}
	// Delete all the ones that were implicitly closed.
	for _, ch := range remove {
		delete(p.funcs, ch)
	}
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
