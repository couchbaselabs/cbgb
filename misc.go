//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
)

var VERSION = "0.0.0"

type Form interface {
	FormValue(key string) string
}

func addInt64(x, y int64) int64 {
	return x + y
}

func subInt64(x, y int64) int64 {
	return x - y
}

type funreq struct {
	fun func()
	res chan bool
}

type transmissible interface {
	Transmit(io.Writer) error
}

// Given an io.Writer, return a channel that can be fed things that
// can write themselves to an io.Writer and a channel that will return
// any encountered error.
//
// The user of this transmitter *MUST* close the input channel to
// indicate no more messages will be sent.
//
// There will be exactly one error written to the error channel either
// after successfully transmitting the entire stream (in which case it
// will be nil) or on any transmission error.
//
// Unless your transmissible stream is finite, it's recommended to
// perform a non-blocking receive of the error stream to check for
// brokenness so you know to stop transmitting.
func transmitPackets(w io.Writer) (chan<- transmissible, <-chan error) {
	ch := make(chan transmissible)
	errs := make(chan error, 1)
	go func() {
		for pkt := range ch {
			err := pkt.Transmit(w)
			if err != nil {
				errs <- err
				for _ = range ch {
					// Eat the input
				}
				return
			}
		}
		errs <- nil
	}()
	return ch, errs
}

type Bytes []byte

func (a *Bytes) MarshalJSON() ([]byte, error) {
	s := url.QueryEscape(string(*a))
	return json.Marshal(s)
}

func (a *Bytes) UnmarshalJSON(d []byte) error {
	var s string
	err := json.Unmarshal(d, &s)
	if err != nil {
		return err
	}
	x, err := url.QueryUnescape(s)
	if err == nil {
		*a = Bytes(x)
	}
	return err
}

func (a *Bytes) String() string {
	return string(*a)
}

func isDir(path string) bool {
	if finfo, err := os.Stat(path); err != nil || !finfo.IsDir() {
		return false
	}
	return true // TODO: check for writability.
}

// TODO: replace with proper UUID implementation
func CreateNewUUID() string {
	val1 := rand.Int63()
	val2 := rand.Int63()
	uuid := fmt.Sprintf("%x%x", val1, val2)
	return uuid
}
