package main

import (
	"bytes"
	"encoding/json"
)

func jsonUnmarshal(b []byte, ob interface{}) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	return d.Decode(ob)
}
