package main

import (
	"fmt"
	"net/http"
	"path/filepath"
	"text/template"

	"github.com/gorilla/mux"
)

var nsTemplates *template.Template

func restNSAPI(r *mux.Router) {
	dir := filepath.Join(*staticPath, "ns", "pools_default*.json")
	nsTemplates = template.Must(template.ParseGlob(dir))

	r.HandleFunc("/pools/default",
		nsGetPoolsDefault).Methods("GET")
}

func nsGetPoolsDefault(w http.ResponseWriter, r *http.Request) {
	if err := nsTemplates.ExecuteTemplate(w, "pools_default.json", nil); err != nil {
		http.Error(w, fmt.Sprintf("ExecuteTemplate err: %v", err), 500)
		return
	}
}
