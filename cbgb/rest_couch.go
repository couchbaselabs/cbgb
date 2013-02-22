package main

import (
	"net/http"

	"github.com/gorilla/mux"
)

func restCouch(r *mux.Router) {
	r.HandleFunc("/_all_dbs",
		couchAllDbs).Methods("GET")

	dbr := r.PathPrefix("/{db}/").Subrouter()

	dbr.Handle("/_all_docs",
		http.HandlerFunc(couchDbAllDocs)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_bulk_docs",
		http.HandlerFunc(couchDbBulkDocs)).Methods("POST")
	dbr.Handle("/_bulk_get",
		http.HandlerFunc(couchDbBulkGet)).Methods("GET", "HEAD")
	dbr.Handle("/_changes",
		http.HandlerFunc(couchDbChanges)).Methods("GET", "HEAD")
	dbr.Handle("/_revs_diff",
		http.HandlerFunc(couchDbRevsDiff)).Methods("POST")

	dbr.Handle("/_local/{docid}",
		http.HandlerFunc(couchDbGetLocalDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_local/{docid}",
		http.HandlerFunc(couchDbPutLocalDoc)).Methods("PUT")
	dbr.Handle("/_local/{docid}",
		http.HandlerFunc(couchDbDelLocalDoc)).Methods("DELETE")

	dbr.Handle("/_design/{docid}",
		http.HandlerFunc(couchDbGetDesignDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_design/{docid}",
		http.HandlerFunc(couchDbPutDesignDoc)).Methods("PUT")
	dbr.Handle("/_design/{docid}",
		http.HandlerFunc(couchDbDelDesignDoc)).Methods("DELETE")

	dbr.Handle("/{docid}",
		http.HandlerFunc(couchDbGetDoc)).Methods("GET", "HEAD")
	dbr.Handle("/{docid}",
		http.HandlerFunc(couchDbPutDoc)).Methods("PUT")
	dbr.Handle("/{docid}",
		http.HandlerFunc(couchDbDelDoc)).Methods("DELETE")
}

func couchAllDbs(w http.ResponseWriter, r *http.Request) {
	jsonEncode(w, buckets.GetNames())
}

func couchDbAllDocs(w http.ResponseWriter, r *http.Request) {
}

func couchDbBulkDocs(w http.ResponseWriter, r *http.Request) {
}

func couchDbBulkGet(w http.ResponseWriter, r *http.Request) {
}

func couchDbChanges(w http.ResponseWriter, r *http.Request) {
}

func couchDbRevsDiff(w http.ResponseWriter, r *http.Request) {
}

func couchDbGetLocalDoc(w http.ResponseWriter, r *http.Request) {
}

func couchDbPutLocalDoc(w http.ResponseWriter, r *http.Request) {
}

func couchDbDelLocalDoc(w http.ResponseWriter, r *http.Request) {
}

func couchDbGetDesignDoc(w http.ResponseWriter, r *http.Request) {
}

func couchDbPutDesignDoc(w http.ResponseWriter, r *http.Request) {
}

func couchDbDelDesignDoc(w http.ResponseWriter, r *http.Request) {
}

func couchDbGetDoc(w http.ResponseWriter, r *http.Request) {
}

func couchDbPutDoc(w http.ResponseWriter, r *http.Request) {
}

func couchDbDelDoc(w http.ResponseWriter, r *http.Request) {
}
