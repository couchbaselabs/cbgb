package main

import (
	"fmt"
	"net/http"
	"path/filepath"
	"text/template"

	"github.com/gorilla/mux"
)

func restNSAPI(r *mux.Router) {
	r.HandleFunc("/pools/default",
		nsGetPoolsDefault).Methods("GET")
}

func nsGetPoolsDefault(w http.ResponseWriter, r *http.Request) {
	dir := filepath.Join(*staticPath, "ns", "pools_default*.json")
	t, err := template.ParseGlob(dir)
	if err != nil {
		http.Error(w, fmt.Sprintf("template.ParseFiles err: %v, dir: %v", err, dir), 500)
		return
	}
	if err = t.ExecuteTemplate(w, "pools_default.json", nil); err != nil {
		http.Error(w, fmt.Sprintf("ExecuteTemplate err: %v", err), 500)
		return
	}
}
