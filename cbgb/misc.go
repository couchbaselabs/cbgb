package main

import (
	"encoding/json"
	"net/url"
)

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
