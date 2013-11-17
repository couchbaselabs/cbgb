package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/url"

	"github.com/dustin/go-coap"
)

var eventUrl = flag.String("event-url", "", "Event URL")

type statusEvent struct {
	DBName string      `json:"db"`
	Type   string      `json:"type"`
	Data   interface{} `json:"data,omitempty"`
}

type eventManager struct {
	eventCh chan statusEvent
}

var defaultEventManager eventManager

type coapSender interface {
	Send(coap.Message) (*coap.Message, error)
}

func transmitEvent(c coapSender, path string, s statusEvent) error {
	if c == nil {
		return nil
	}

	payload, err := json.Marshal(s)
	if err != nil {
		return err
	}

	req := coap.Message{
		Type:    coap.NonConfirmable,
		Code:    coap.POST,
		Payload: payload,
	}

	req.SetOption(coap.ContentFormat, coap.AppJSON)
	req.SetPathString(path)

	_, err = c.Send(req)
	return err
}

func dialCoap(ustr string) (coapSender, string) {
	if ustr == "" {
		return nil, ""
	}
	u, err := url.Parse(ustr)
	if err != nil {
		log.Printf("Error dialing coap from %q: %v",
			ustr, err)
		return nil, ""
	}
	conn, err := coap.Dial("udp", u.Host)
	if err != nil {
		log.Printf("Error dialing coap from %q: %v", ustr, err)
		return nil, ""
	}
	return conn, u.Path
}

func (e *eventManager) deliverEvents(u string) {
	c, path := dialCoap(u)

	for ev := range e.eventCh {
		err := transmitEvent(c, path, ev)
		if err != nil {
			log.Printf("Error transmitting %v: %v", ev, err)
		}
	}
}

func (e *eventManager) sendEvent(name, t string, data interface{}) bool {
	ev := statusEvent{name, t, data}
	select {
	case e.eventCh <- ev:
		return true
	default:
		log.Printf("Dropped event: %v", ev)
		return false
	}
}

func (e *eventManager) Close() error {
	close(e.eventCh)
	return nil
}
