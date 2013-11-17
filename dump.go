// Copyright (c) 2013 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License. You
// may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"io"
	"os"
	"os/signal"
	"runtime/pprof"
)

var infoSigs []os.Signal

type sigHandler struct {
	ch   chan os.Signal
	w    io.Writer
	hook func()
}

func NewSigHandler(sigs []os.Signal) *sigHandler {
	c := make(chan os.Signal, 1)
	signal.Notify(c, sigs...)
	return &sigHandler{c, os.Stderr, func() {}}
}

func (c *sigHandler) Run() {
	for _ = range c.ch {
		pprof.Lookup("goroutine").WriteTo(c.w, 1)
		c.hook()
	}
}

func (c *sigHandler) Close() error {
	signal.Stop(c.ch)
	close(c.ch)
	return nil
}

func startInfoHandler() {
	if len(infoSigs) > 0 {
		go NewSigHandler(infoSigs).Run()
	}
}
