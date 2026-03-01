// Command lb is the single binary for the Raft load balancer.
// Subcommands: node, backend, admin.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: lb <node|backend|admin> [flags]")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "node":
		runNode(os.Args[2:])
	case "backend":
		runBackend(os.Args[2:])
	case "admin":
		runAdmin(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", os.Args[1])
		os.Exit(1)
	}
}
