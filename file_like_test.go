package main

import (
	"os"
	"testing"
)

// This test is kind of long, but it effectively writes some data,
// checks the size and then reads it back while verifying you can't do
// operations you didn't request in states you didn't allow them.
func TestFileLike(t *testing.T) {
	fn := ",file-like-thing"
	defer os.Remove(fn)

	fs := NewFileService(1)
	defer fs.Close()
	f, err := fs.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_EXCL)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}

	buf := make([]byte, 4096)
	copy(buf, []byte("first write"))

	n, err := f.WriteAt(buf, 8192)
	if err != nil {
		t.Fatalf("Error writing: %v", err)
	}
	if n != 4096 {
		t.Fatalf("Short write: %v", n)
	}

	copy(buf, []byte("second write"))

	n, err = f.WriteAt(buf, 32768)
	if err != nil {
		t.Fatalf("Error writing: %v", err)
	}
	if n != 4096 {
		t.Fatalf("Short write: %v", n)
	}

	//
	// Now try to read them back
	//

	// An immediate read should fail because we're write only.
	n, err = f.ReadAt(buf, 4096)
	if err != unReadable {
		t.Fatalf("Should've failed read with unReadable, "+
			"got %v and %v bytes instead", err, n)
	}

	// Reopen for reading.
	f.Close()
	f, err = fs.OpenFile(fn, os.O_RDONLY)
	if err != nil {
		t.Fatalf("Error reopening for read: %v", err)
	}

	// Let's throw in a stat
	fi, err := f.Stat()
	if err != nil {
		t.Errorf("Stat failed: %v", err)
	}
	if fi.Size() != 32768+4096 {
		t.Errorf("File size didn't meet our expectations: %v", fi.Size())
	}

	// And a write should fail
	n, err = f.WriteAt(buf, 4096)
	if err == nil {
		t.Fatalf("Should've failed write, wrote %v bytes instead", n)
	}

	n, err = f.ReadAt(buf, 32768)
	if err != nil {
		t.Fatalf("Error reading data: %v", err)
	}
	if n != 4096 {
		t.Fatalf("Short read: %v", n)
	}

	s := string(buf[:len("second write")])
	if s != "second write" {
		t.Fatalf("Misread:  %q", s)
	}

	n, err = f.ReadAt(buf, 8192)
	if err != nil {
		t.Fatalf("Error reading data: %v", err)
	}
	if n != 4096 {
		t.Fatalf("Short read: %v", n)
	}

	s = string(buf[:len("first write")])
	if s != "first write" {
		t.Fatalf("Misread:  %q", s)
	}
}

func TestFileLikeRW(t *testing.T) {
	fn := ",file-like-thing"
	defer os.Remove(fn)

	fs := NewFileService(1)
	defer fs.Close()
	f, err := fs.OpenFile(fn, os.O_CREATE|os.O_RDWR|os.O_EXCL)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}

	buf := make([]byte, 4096)
	copy(buf, []byte("a write"))

	n, err := f.WriteAt(buf, 8192)
	if err != nil {
		t.Fatalf("Error writing: %v", err)
	}
	if n != 4096 {
		t.Fatalf("Short write: %v", n)
	}

	buf[0] = 'x'

	n, err = f.ReadAt(buf, 8192)
	if err != nil {
		t.Fatalf("Error reading data: %v", err)
	}
	if n != 4096 {
		t.Fatalf("Short read: %v", n)
	}

	s := string(buf[:len("a write")])
	if s != "a write" {
		t.Fatalf("Misread:  %q", s)
	}
}
