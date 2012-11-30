package main

type mutation struct {
	key     []byte
	cas     uint64
	deleted bool
}

const broadcastBuffer = 100

type broadcaster struct {
	input chan mutation
	reg   chan chan<- mutation
	unreg chan chan<- mutation

	outputs map[chan<- mutation]bool
}

func (b *broadcaster) cleanup() {
	for ch := range b.outputs {
		close(ch)
	}
}

func (b *broadcaster) broadcast(m mutation) {
	for ch := range b.outputs {
		ch <- m
	}
}

func (b *broadcaster) run() {
	defer b.cleanup()

	for {
		select {
		case m := (<-b.input):
			b.broadcast(m)
		case ch, ok := (<-b.reg):
			if ok {
				b.outputs[ch] = true
			} else {
				return
			}
		case ch := (<-b.unreg):
			delete(b.outputs, ch)
		}
	}
}

func newBroadcaster() *broadcaster {
	input := make(chan mutation, broadcastBuffer)
	b := &broadcaster{
		input:   input,
		reg:     make(chan chan<- mutation),
		unreg:   make(chan chan<- mutation),
		outputs: make(map[chan<- mutation]bool),
	}

	go b.run()

	return b
}

func (b *broadcaster) Register(newch chan<- mutation) {
	b.reg <- newch
}

func (b *broadcaster) Unregister(newch chan<- mutation) {
	b.unreg <- newch
}

func (b *broadcaster) Close() error {
	close(b.reg)
	return nil
}

func (b *broadcaster) Submit(m mutation) {
	b.input <- m
}
