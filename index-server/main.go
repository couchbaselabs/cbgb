package main

import (
	"flag"
	"log"
)

func main() {
	addr := flag.String("bind", ":11411", "indexer listen port")

	flag.Parse()

	log.Printf("got addr: %v", *addr)
}
