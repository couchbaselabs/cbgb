package main

import (
	"bytes"
	"testing"
)

func TestObserveParse(t *testing.T) {
	// From the spec
	sample := []byte{
		0x00, 0x04, 0x00, 0x05,
		'h', 'e', 'l', 'l', 'o',
		0x00, 0x05, 0x00, 0x05,
		'w', 'o', 'r', 'l', 'd',
	}

	exp := []obsKey{
		{4, []byte("hello")},
		{5, []byte("world")},
	}

	parsed, err := parseObserveKeys(sample)
	if err != nil {
		t.Fatalf("Error parsing: %v", err)
	}

	if len(parsed) != len(exp) {
		t.Fatalf("Expected to find %v items, found %v",
			len(exp), len(parsed))
	}

	for i := range exp {
		if parsed[i].vbid != exp[i].vbid {
			t.Errorf("Incorrect vbucket at %v: %v",
				i, parsed[i].vbid)
		}

		if !bytes.Equal(parsed[i].key, exp[i].key) {
			t.Errorf("Incorrect key at %v: %s", i, parsed[i].key)
		}
	}
}

func TestObserveParseErrors(t *testing.T) {
	failures := [][]byte{
		{0, 0, 3},            // short
		{0, 0, 0, 1},         // no key
		{0, 0, 0, 1, 'x', 0}, // good, then short
	}

	for _, test := range failures {
		_, err := parseObserveKeys(test)
		if err == nil {
			t.Errorf("Expected failure on %v", test)
		}
	}
}

func TestObserveEncode(t *testing.T) {
	got := encodeObserveBody([]obsStatus{
		{obsKey{4, []byte("hello")}, obsPersisted, 0xa},
		{obsKey{5, []byte("world")}, obsNotPersisted, 0xdeadbeefdeadcafe},
	})

	exp := []byte{
		// Hello
		0, 4, 0, 5, 'h', 'e', 'l', 'l', 'o',
		1,
		0, 0, 0, 0, 0, 0, 0, 0xa,
		// World
		0, 5, 0, 5, 'w', 'o', 'r', 'l', 'd',
		0,
		0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xca, 0xfe,
	}

	if !bytes.Equal(got, exp) {
		t.Fatalf("Encoding failed:\n%x\n%x", got, exp)
	}
}
