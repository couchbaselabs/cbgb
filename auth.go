package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/context"
	"github.com/gorilla/mux"
)

var adminUser = flag.String("adminUser", "", "Admin username")
var adminPass = flag.String("adminPass", "", "Admin password")

func initAdmin() {
	if *adminUser == "" {
		*adminUser = os.Getenv("CBGB_ADMIN_USER")
	}
	if *adminPass == "" {
		*adminPass = os.Getenv("CBGB_ADMIN_PASS")
	}
	if *adminUser != "" && *adminPass == "" {
		log.Fatalf("error: adminUser was supplied, but missing adminPass param")
	}
	if *adminUser == "" && *adminPass != "" {
		log.Fatalf("error: adminPass was supplied, but missing adminUser param")
	}
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

func (h httpUser) canAccess(bucket string) bool {
	return h.isAdmin() || string(h) == bucket
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
	b := buckets.Get(u)
	if b != nil {
		return b.Auth([]byte(p))
	}
	return false
}

func (a authenticationFilter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer a.next.ServeHTTP(w, r)
	ahdr := r.Header.Get("Authorization")
	if ahdr != "" {
		u, p, err := parseBasicAuth(ahdr)
		if err != nil {
			log.Printf("error: parseBasicAuth, err: %v", err)
		}
		if authenticateUser(u, p) {
			context.Set(r, authInfoKey, httpUser(u))
		} else {
			log.Printf("error: incorrect password, user: %v", u)
		}
	}
}

func currentUser(req *http.Request) httpUser {
	u, _ := context.Get(req, authInfoKey).(httpUser)
	return u
}

func adminRequired(req *http.Request, rm *mux.RouteMatch) bool {
	u := currentUser(req)
	log.Printf("verifying admin for url: %v, user: %v", req.URL, u)
	return u.isAdmin()
}

func withBucketAccess(orig func(http.ResponseWriter,
	*http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		u := currentUser(r)
		b := mux.Vars(r)["bucketname"]
		if u.canAccess(b) {
			orig(w, r)
		} else {
			log.Printf("%q (isAdmin=%v) can't access bucket: %v", u, u.isAdmin(), b)
			if string(u) == "" {
				authError(w, r)
			} else {
				http.Error(w, "Access denied", 403)
			}
		}
	}
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
