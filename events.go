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

var eventCh = make(chan statusEvent, 20)

func transmitEvent(c *coap.Conn, path string, s statusEvent) {
	if c == nil {
		return
	}

	payload, err := json.Marshal(s)
	if err != nil {
		log.Printf("Error marshaling %v: %v", s, err)
		return
	}

	req := coap.Message{
		Type:    coap.NonConfirmable,
		Code:    coap.POST,
		Payload: payload,
	}

	req.SetOption(coap.ContentFormat, coap.AppJSON)
	req.SetPathString(path)

	_, err = c.Send(req)
	if err != nil {
		log.Printf("Error transmitting %v: %v", req, err)
	}
}

func dialCoap() (*coap.Conn, string) {
	if *eventUrl == "" {
		return nil, ""
	}
	u, err := url.Parse(*eventUrl)
	if err != nil {
		log.Printf("Error dialing coap from %q: %v",
			*eventUrl, err)
		return nil, ""
	}
	conn, err := coap.Dial("udp", u.Host)
	if err != nil {
		log.Printf("Error dialing coap from %q: %v",
			*eventUrl, err)
		return nil, ""
	}
	return conn, u.Path
}

func deliverEvents() {
	c, path := dialCoap()

	for ev := range eventCh {
		transmitEvent(c, path, ev)
	}
}

func sendEvent(name, t string, data interface{}) {
	e := statusEvent{name, t, data}
	select {
	case eventCh <- e:
	default:
		log.Printf("Dropped event: %v", e)
	}
}
