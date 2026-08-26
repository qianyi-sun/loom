package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "fixture mode is required")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "sidecar":
		http.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusNoContent)
		})
		http.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusNoContent)
		})
		if err := http.ListenAndServe(":8080", nil); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "phase":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "fixture phase name is required")
			os.Exit(2)
		}
		if os.Args[2] == "agent" {
			if err := os.WriteFile("/workspace/agent-output.txt", []byte("agent-output\n"), 0o600); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		if os.Args[2] == "verifier" {
			payload, err := os.ReadFile("/workspace/agent-output.txt")
			if err != nil || string(payload) != "agent-output\n" {
				fmt.Fprintln(os.Stderr, "verifier did not receive exact agent output")
				os.Exit(1)
			}
		}
		fmt.Printf("fixture-phase=%s\n", os.Args[2])
	default:
		fmt.Fprintln(os.Stderr, "unsupported fixture mode")
		os.Exit(2)
	}
}
