package cbgb

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
)

const testfileservicename = ",file_server_test"

func TestFileService(t *testing.T) {
	defer os.Remove(testfileservicename)

	in := map[string]interface{}{"b": "bee"}

	fs := NewFileService(2)
	defer fs.Close()

	err := fs.Do(testfileservicename, os.O_CREATE|os.O_WRONLY,
		func(f *os.File) error {
			e := json.NewEncoder(f)
			return e.Encode(in)
		})
	if err != nil {
		t.Fatalf("Failed to encode and persist thing: %v", err)
	}

	out := map[string]interface{}{}

	err = fs.Do(testfileservicename, os.O_RDONLY,
		func(f *os.File) error {
			d := json.NewDecoder(f)
			return d.Decode(&out)
		})
	if err != nil {
		t.Fatalf("Failed to read and decode thing: %v", err)
	}

	if len(in) != len(out) {
		t.Fatalf("in != out: %v != %v", in, out)
	}
	for k := range in {
		if in[k] != out[k] {
			t.Errorf("Error at %v:  %v (%T) != %v (%T)",
				k, in[k], in[k], out[k], out[k])
		}
	}
}

func TestFileServiceOpenError(t *testing.T) {
	fs := NewFileService(2)
	defer fs.Close()

	err := fs.Do(",idonotexist", os.O_RDONLY, func(f *os.File) error {
		t.Fatalf("Expected to avoid running function due to error")
		return nil
	})
	if !os.IsNotExist(err) {
		t.Fatalf("Unexpected error opening missing file: %v", err)
	}
}

func TestFileServiceFuncError(t *testing.T) {
	fs := NewFileService(2)
	defer fs.Close()

	e := errors.New("Expected error")

	err := fs.Do("/dev/zero", os.O_RDONLY, func(f *os.File) error {
		return e
	})
	if err != e {
		t.Fatalf("Unexpected error with broken function: %v", err)
	}
}
