package main

import (
	"testing"
)

func TestNilBucketSettingsAuth(t *testing.T) {
	var bs *BucketSettings
	if bs.Auth([]byte{}) {
		t.Errorf("Fail to fail nil setting auth")
	}
}

func TestBucketSettingsAuth(t *testing.T) {
	tests := []struct {
		hf, salt, h, input string
		exp                bool
	}{
		{"notimplemented", "", "", "", false},
		{"", "badsalt", "", "", false},
		{"", "", "good", "bad", false},
		{"", "", "yay", "yay", true},
		{"", "", "", "", true},
	}

	for _, test := range tests {
		bs := BucketSettings{
			PasswordHashFunc: test.hf,
			PasswordSalt:     test.salt,
			PasswordHash:     test.h,
		}

		got := bs.Auth([]byte(test.input))
		if got != test.exp {
			t.Errorf("auth test fail: hf=%v, salt=%v, "+
				"hash=%v, input=%v, got %v, expected %v",
				test.hf, test.salt, test.h, test.input,
				got, test.exp)
		}
	}
}
