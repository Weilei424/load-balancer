package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// runAdmin discovers the cluster leader and executes admin commands.
func runAdmin(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: lb admin <add-backend|remove-backend|set-weight|set-algorithm> [flags]")
		os.Exit(1)
	}

	subcmd := args[0]
	rest := args[1:]

	switch subcmd {
	case "add-backend":
		adminAddBackend(rest)
	case "remove-backend":
		adminRemoveBackend(rest)
	case "set-weight":
		adminSetWeight(rest)
	case "set-algorithm":
		adminSetAlgorithm(rest)
	default:
		fmt.Fprintf(os.Stderr, "unknown admin command: %s\n", subcmd)
		os.Exit(1)
	}
}

func adminAddBackend(args []string) {
	fs := flag.NewFlagSet("add-backend", flag.ExitOnError)
	url := fs.String("url", "", "backend URL (required)")
	weight := fs.Int("weight", 1, "backend weight")
	nodes := fs.String("nodes", "http://localhost:9001,http://localhost:9002,http://localhost:9003", "LB node HTTP addresses")
	fs.Parse(args) //nolint:errcheck

	if *url == "" {
		fmt.Fprintln(os.Stderr, "--url is required")
		os.Exit(1)
	}
	body := map[string]interface{}{"url": *url, "weight": *weight}
	doAdminRequest(*nodes, "POST", "/admin/backends", body)
}

func adminRemoveBackend(args []string) {
	fs := flag.NewFlagSet("remove-backend", flag.ExitOnError)
	url := fs.String("url", "", "backend URL (required)")
	nodes := fs.String("nodes", "http://localhost:9001,http://localhost:9002,http://localhost:9003", "LB node HTTP addresses")
	fs.Parse(args) //nolint:errcheck

	if *url == "" {
		fmt.Fprintln(os.Stderr, "--url is required")
		os.Exit(1)
	}
	doAdminRequest(*nodes, "DELETE", "/admin/backends", map[string]string{"url": *url})
}

func adminSetWeight(args []string) {
	fs := flag.NewFlagSet("set-weight", flag.ExitOnError)
	url := fs.String("url", "", "backend URL (required)")
	weight := fs.Int("weight", 1, "new weight")
	nodes := fs.String("nodes", "http://localhost:9001,http://localhost:9002,http://localhost:9003", "LB node HTTP addresses")
	fs.Parse(args) //nolint:errcheck

	doAdminRequest(*nodes, "PATCH", "/admin/backends", map[string]interface{}{"url": *url, "weight": *weight})
}

func adminSetAlgorithm(args []string) {
	fs := flag.NewFlagSet("set-algorithm", flag.ExitOnError)
	algo := fs.String("algorithm", "round_robin", "algorithm: round_robin|least_conn")
	nodes := fs.String("nodes", "http://localhost:9001,http://localhost:9002,http://localhost:9003", "LB node HTTP addresses")
	fs.Parse(args) //nolint:errcheck

	doAdminRequest(*nodes, "PUT", "/admin/algorithm", map[string]string{"algorithm": *algo})
}

// doAdminRequest tries each node in turn, following 307 redirects to the leader.
func doAdminRequest(nodesStr, method, path string, body interface{}) {
	addrs := strings.Split(nodesStr, ",")
	client := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return nil // follow redirects
		},
	}

	data, _ := json.Marshal(body)

	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		req, err := http.NewRequest(method, addr+path, bytes.NewReader(data))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: %s unreachable: %v\n", addr, err)
			continue
		}
		defer resp.Body.Close()

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result) //nolint:errcheck

		if resp.StatusCode == http.StatusOK {
			out, _ := json.MarshalIndent(result, "", "  ")
			fmt.Println(string(out))
			return
		}
		// 503 means this node has no known leader yet (election in progress); try next.
		if resp.StatusCode == http.StatusServiceUnavailable {
			fmt.Fprintf(os.Stderr, "warn: %s has no leader yet, trying next node\n", addr)
			continue
		}
		fmt.Fprintf(os.Stderr, "error from %s: status=%d body=%v\n", addr, resp.StatusCode, result)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "error: could not reach any node")
	os.Exit(1)
}
