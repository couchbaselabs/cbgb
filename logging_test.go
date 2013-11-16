package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"testing"
)

// Don't do any normal logging while running tests.
func init() {
	log.SetOutput(ioutil.Discard)
}

func TestInitLogSyslog(t *testing.T) {
	defer log.SetOutput(ioutil.Discard)

	buf := &bytes.Buffer{}
	*logPlain = false
	log.SetOutput(buf)
	initLogger(false)

	log.Printf("hello")
	wrote := buf.Bytes()
	if len(wrote) < 10 {
		t.Errorf("Expected a timestamp and stuff, got %q", wrote)
	}

	buf.Reset()
	*logPlain = true
	initLogger(false)

	log.Printf("hello")
	wrote = buf.Bytes()
	if !bytes.Equal([]byte("hello\n"), buf.Bytes()) {
		t.Errorf("Expected plain log to not look like %q", buf.Bytes())
	}

	buf.Reset()
	initLogger(true)
	log.Printf("hello")

	if len(buf.Bytes()) > 0 {
		log.Printf("Expected nothing written, got %q", buf.Bytes())
	}
}
