package main

import (
	"encoding/json"
	"io"
	"net/http"
)

func (s runtimeServer) restoreDirectory(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Root  string `json:"root"`
		Stage string `json:"stage"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		w.Header().Set("X-Loom-Sandbox-Error", "restore_request_invalid")
		http.Error(w, "invalid directory restore request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		w.Header().Set("X-Loom-Sandbox-Error", "restore_request_invalid")
		http.Error(w, "invalid directory restore request", http.StatusBadRequest)
		return
	}
	if err := replaceDirectory(request.Root, request.Stage); err != nil {
		w.Header().Set("X-Loom-Sandbox-Error", "directory_restore_failed")
		http.Error(w, "unable to restore sandbox directory", http.StatusUnprocessableEntity)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
