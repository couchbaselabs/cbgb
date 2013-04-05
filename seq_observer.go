package main

type sequenceId string

type sequenceObserver struct {
	atleast int64
	sub     chan int64
}

type sequenceReg struct {
	seq sequenceId
	obs sequenceObserver
}

type sequenceEvent struct {
	seq sequenceId
	num int64
}

type sequencePubSub struct {
	observers map[sequenceId][]sequenceObserver
	quit      chan bool
	reg       chan sequenceReg
	events    chan sequenceEvent
}

// Build a sequence pubsub.
func newSequencePubSub() *sequencePubSub {
	rv := &sequencePubSub{
		observers: map[sequenceId][]sequenceObserver{},
		quit:      make(chan bool),
		reg:       make(chan sequenceReg),
		events:    make(chan sequenceEvent),
	}
	go rv.run()
	return rv
}

func (s *sequencePubSub) run() {
	for {
		select {
		case <-s.quit:
			return
		case reg := <-s.reg:
			s.observers[reg.seq] = append(s.observers[reg.seq], reg.obs)
		case ev := <-s.events:
			s.dist(ev.seq, ev.num)
		}
	}
}

// Stop (shut down) a sequence pubsub runner.
func (s *sequencePubSub) Stop() {
	close(s.quit)
	for _, obses := range s.observers {
		for _, obs := range obses {
			close(obs.sub)
		}
	}
}

func (s *sequencePubSub) dist(seq sequenceId, num int64) {
	var unprocessed []sequenceObserver
	prev := s.observers[seq]
	delete(s.observers, seq)
	for _, obs := range prev {
		if obs.atleast <= num {
			obs.sub <- num
		} else {
			unprocessed = append(unprocessed, obs)
		}
	}
	if unprocessed != nil {
		s.observers[seq] = unprocessed
	}
}

// Subscribe to a sequence observer and receive a channel over which
// the sequence identifer will be delivered.
func (s *sequencePubSub) Sub(seq sequenceId, atleast int64) <-chan int64 {
	rv := make(chan int64, 1)
	s.reg <- sequenceReg{seq, sequenceObserver{atleast, rv}}
	return rv
}

// Publish a sequence event.
func (s *sequencePubSub) Pub(seq sequenceId, at int64) {
	s.events <- sequenceEvent{seq, at}
}
