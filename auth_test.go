package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestBasicAuthParsing(t *testing.T) {
	tests := []struct {
		input, user, pass string
		ok                bool
	}{
		{"Notbasic QWxhZGRpbjpvcGVuIHNlc2FtZQ==", "", "", false},
		{"Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==", "Aladdin", "open sesame", true},
		{"Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=x", "", "", false},
	}

	for _, test := range tests {
		gu, gp, err := parseBasicAuth(test.input)
		if test.ok {
			if err != nil {
				t.Errorf("Expected no error on %v, got %v",
					test.input, err)
			}
			if gu != test.user {
				t.Errorf("Expected user=%v for %v, got %v",
					test.user, test.input, gu)
			}
			if gp != test.pass {
				t.Errorf("Expected pass=%v for %v, got %v",
					test.pass, test.input, gp)
			}
		} else {
			if err == nil {
				t.Errorf("Expected error on %v, got %v/%v",
					test.input, gu, gp)
			}
		}
	}
}

func TestAuthenticateUser(t *testing.T) {
	origUser, origPass := adminUser, adminPass
	defer func() {
		adminUser, adminPass = origUser, origPass
	}()
	u, p := "tom", ""
	adminUser, adminPass = &u, &p
	if authenticateUser("tom", "fullery") {
		t.Errorf("expected authhenticateUser to work")
	}
	if !authenticateUser("tom", "") {
		t.Errorf("expected authhenticateUser to not work")
	}
	d, _ := testSetupBuckets(t, 1)
	defer os.RemoveAll(d)
	b, _ := buckets.New("foo", &BucketSettings{
		PasswordHash: "bar",
	})
	defer b.Close()
	if !authenticateUser("foo", "bar") {
		t.Errorf("expected authhenticateUser to work")
	}
	if authenticateUser("foo", "") {
		t.Errorf("expected authhenticateUser to not work")
	}
	if authenticateUser("foo", "baz") {
		t.Errorf("expected authhenticateUser to not work")
	}
	if authenticateUser("not-a-bucket", "bar") {
		t.Errorf("expected authhenticateUser to not work")
	}
	if authenticateUser("not-a-bucket", "") {
		t.Errorf("expected authhenticateUser to not work")
	}
}

func TestAuthError401(t *testing.T) {
	w := httptest.NewRecorder()
	req := &http.Request{}
	authError(w, req)
	if w.Code != 401 {
		t.Errorf("Expected status 401, got %v", w.Code)
	}
	got := w.Header().Get("WWW-Authenticate")
	if got != `Basic realm="cbgb"` {
		t.Errorf("Expected cbgb realm, got %v", got)
	}
}

func TestInitAdmin(t *testing.T) {
	initAdmin() // Should not panic.
}