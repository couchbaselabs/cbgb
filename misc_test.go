package main

import (
	"io/ioutil"
	"log"
)

// Don't do any normal logging while running tests.
func init() {
	log.SetOutput(ioutil.Discard)
}
