package main

import (
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/context"
	"github.com/gorilla/mux"
)

var adminUser = flag.String("adminUser", "admin", "Admin username")
var adminPass = flag.String("adminPass", "", "Admin password (default is random)")

// Generate a random default password instead of having a default.
func init() {
	d := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(d)
	if err != nil {
		panic(err)
	}
	*adminPass = base64.StdEncoding.EncodeToString(d)
}

type contextKey int

const (
	authInfoKey = contextKey(iota)
)

type authenticationFilter struct {
	next http.Handler
}

type httpUser string

func (h httpUser) isAdmin() bool {
	return string(h) == *adminUser
}

func parseBasicAuth(ahdr string) (string, string, error) {
	parts := strings.SplitN(ahdr, " ", 2)
	if strings.ToLower(parts[0]) != "basic" {
		return "", "", fmt.Errorf("Can't authenticate with %v", parts[0])
	}
	d, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", err
	}
	parts = strings.SplitN(string(d), ":", 2)
	return parts[0], parts[1], nil

}

func authenticateUser(u, p string) bool {
	if u == *adminUser {
		return p == *adminPass
	}
	return p == "correctpassword"
}

func (a authenticationFilter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer a.next.ServeHTTP(w, r)
	ahdr := r.Header.Get("Authorization")
	if ahdr != "" {
		u, p, err := parseBasicAuth(ahdr)
		if err != nil {
			log.Printf("Error reading auth data: %v", err)
		}
		if authenticateUser(u, p) {
			context.Set(r, authInfoKey, httpUser(u))
		} else {
			log.Printf("Incorrect password for %v", u)
		}
	}
}

func adminRequired(req *http.Request, rm *mux.RouteMatch) bool {
	u, _ := context.Get(req, authInfoKey).(httpUser)
	log.Printf("Verifying admin at %v -> %v (user is %v)", req.URL, rm,
		u)
	return u.isAdmin()
}

func authError(w http.ResponseWriter, r *http.Request) {
	u := context.Get(r, authInfoKey)
	if u == nil {
		w.Header().Set("WWW-Authenticate", `Basic realm="cbgb"`)
		http.Error(w, "Please authenticate", 401)
	} else {
		http.Error(w, "You're not allowed here", 403)
	}
}
