package main

import (
	"fmt"
	"os"

	"github.com/couchbaselabs/cbgb"
)

// For each bucket name on the cmd-line, this tool prints the bucket
// subdirectory path.

func main() {
	for _, arg := range os.Args[1:] {
		fmt.Printf("%s\n", cbgb.BucketPath(arg))
	}
}
