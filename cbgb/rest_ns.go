package main

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/gorilla/mux"
)

var nsTemplates *template.Template

func restNSAPI(r *mux.Router) {
	dir := filepath.Join(*staticPath, "ns", "pools_default*.json")
	files, err := filepath.Glob(dir)
	if err != nil {
		panic(fmt.Sprintf("glob ns dir: %v, err: %v", dir, err))
	}
	for _, file := range files {
		if strings.HasPrefix(file, *staticPath) {
			p := file[len(*staticPath)+len("/ns"):]
			p = strings.Replace(p, "_", "/", -1)
			p = strings.Replace(p, ".json", "", -1)
			r.HandleFunc(p, restNSHandler).Methods("GET")
		}
	}

	nsTemplates = template.Must(template.ParseGlob(dir))
}

func restNSHandler(w http.ResponseWriter, r *http.Request) {
	templateName := strings.Replace(r.URL.Path[1:], "/", "_", -1) + ".json"
	if err := nsTemplates.ExecuteTemplate(w, templateName, nil); err != nil {
		http.Error(w, fmt.Sprintf("ExecuteTemplate err: %v", err), 500)
		return
	}
}
