package main

import (
	"net/http"

	"github.com/gorilla/mux"
)

func restCouch(r *mux.Router) {
	r.HandleFunc("/_all_dbs",
		couchAllDbs).Methods("GET")
}

func couchAllDbs(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, buckets.GetNames())
}
