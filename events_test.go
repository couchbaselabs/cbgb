package main

import (
	"testing"

	"github.com/dustin/go-coap"
)

type dummyTransmitter struct {
	events []coap.Message
}

func (d *dummyTransmitter) Send(m coap.Message) (*coap.Message, error) {
	d.events = append(d.events, m)
	return nil, nil
}

func TestEventTransmit(t *testing.T) {
	if err := transmitEvent(nil, "", statusEvent{}); err != nil {
		t.Errorf("Got an error transmitting on nil: %v", err)
	}

	dt := &dummyTransmitter{}

	err := transmitEvent(dt, "/x", statusEvent{"dbx", "dbt", "thing"})
	if err != nil {
		t.Errorf("Error in test transmit: %v", err)
	}

	if len(dt.events) != 1 {
		t.Fatalf("Expected one event, got %v", dt.events)
	}
}

func TestEventMarshalFail(t *testing.T) {
	dt := &dummyTransmitter{}

	var zero float64
	err := transmitEvent(dt, "/x", statusEvent{"dbx", "dbt", 1.0 / zero})
	if err == nil {
		t.Errorf("Expected marshaling error, got: %v - %v", err, dt.events)
	}
}

func TestDeliverEvents(t *testing.T) {
	em := eventManager{make(chan statusEvent, 10)}

	go em.deliverEvents("coap://localhost:3636/a/b")
	defer em.Close()

	var zero float64
	vals := []interface{}{nil, 1 / zero}
	for _, v := range vals {
		if !em.sendEvent("x", "y", v) {
			t.Errorf("Failed to send event with %v", v)
		}
	}
}

func TestSendEvents(t *testing.T) {
	em := eventManager{make(chan statusEvent, 10)}

	go em.deliverEvents("coap://localhost:3636/a/b")
	defer em.Close()

	sent := 0
	for em.sendEvent("a", "b", 3) {
		sent++
	}
	if sent == 0 {
		t.Errorf("Sent no events, expected to send a few")
	}
}

func TestDialCoap(t *testing.T) {
	tests := []struct {
		in    string
		works bool
		path  string
	}{
		{"", false, ""},
		{"coap://%", false, ""},
		{"coap://127.0.0.0.1", false, ""},
		{"coap://127.0.0.1:3737/x/ab", true, "/x/ab"},
	}

	for _, test := range tests {
		ccon, path := dialCoap(test.in)
		if (ccon != nil) != test.works {
			t.Errorf("Error on %q: expected works=%v, got %v - %v",
				test.in, test.works, ccon, path)
		}
		if path != test.path {
			t.Errorf("Error on %q: expected path=%q, got %q",
				test.in, test.path, path)
		}
	}
}
