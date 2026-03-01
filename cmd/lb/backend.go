package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
)

func runBackend(args []string) {
	fs := flag.NewFlagSet("backend", flag.ExitOnError)
	id := fs.String("id", "b1", "backend ID")
	listen := fs.String("listen", ":9101", "listen address")
	fs.Parse(args) //nolint:errcheck

	mux := http.NewServeMux()

	// Health endpoint — required by LB health checker.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "id": *id}) //nolint:errcheck
	})

	// Echo endpoint — responds to all other requests.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
			"id":     *id,
			"method": r.Method,
			"path":   r.URL.Path,
		})
	})

	fmt.Fprintf(os.Stdout, "{\"level\":\"info\",\"msg\":\"backend starting\",\"id\":%q,\"addr\":%q}\n", *id, *listen)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		fmt.Fprintf(os.Stderr, "backend error: %v\n", err)
		os.Exit(1)
	}
}
