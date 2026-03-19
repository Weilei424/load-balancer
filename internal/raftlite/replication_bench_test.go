package raftlite

import (
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// fixedCmd is a small representative command used in all raft benchmarks.
var fixedCmd = json.RawMessage(`{"op":"add_backend","url":"http://bench:9999"}`)

// BenchmarkRaftPropose measures single-node commit latency. The single node
// elects itself leader immediately (no peers) and each Propose blocks until
// the entry is committed locally.
func BenchmarkRaftPropose(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}

	applyCh := make(chan LogEntry, 64)
	proposeCh := make(chan ProposeReq, 8)
	nd, err := NewNode(Config{
		ID:        "bench",
		Peers:     map[string]string{},
		HTTPPeers: map[string]string{},
		DataDir:   b.TempDir(),
	}, applyCh, proposeCh, zerolog.Nop())
	if err != nil {
		ln.Close()
		b.Fatalf("NewNode: %v", err)
	}

	srv := &http.Server{Handler: nd.Handler()}
	go srv.Serve(ln) //nolint:errcheck
	go nd.Run()

	// Drain applyCh so the applier goroutine never blocks.
	go func() {
		for range applyCh {
		}
	}()

	b.Cleanup(func() {
		nd.Stop()
		srv.Close()
		ln.Close()
	})

	// Wait for single-node self-election.
	deadline := time.Now().Add(2 * time.Second)
	for !nd.IsLeader() {
		if time.Now().After(deadline) {
			b.Fatal("node did not become leader within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := nd.Propose(fixedCmd); err != nil {
			b.Fatalf("Propose: %v", err)
		}
	}
}

// BenchmarkRaftReplication3Node measures end-to-end commit latency through a
// 3-node in-process cluster over loopback TCP. Propose blocks until a majority
// (2 of 3) acknowledges the entry.
func BenchmarkRaftReplication3Node(b *testing.B) {
	// Phase 1: allocate listeners to get real addresses before creating nodes.
	lns := make([]net.Listener, 3)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			b.Fatalf("listen: %v", err)
		}
		lns[i] = ln
	}

	ids := []string{"b1", "b2", "b3"}
	addrs := make(map[string]string, 3)
	for i, id := range ids {
		addrs[id] = "http://" + lns[i].Addr().String()
	}

	type benchNode struct {
		nd  *Node
		srv *http.Server
		ln  net.Listener
	}
	nodes := make([]benchNode, 3)

	applyChans := make([]chan LogEntry, 3)
	for i, id := range ids {
		peers := make(map[string]string, 2)
		for pid, pa := range addrs {
			if pid != id {
				peers[pid] = pa
			}
		}
		applyCh := make(chan LogEntry, 64)
		applyChans[i] = applyCh
		proposeCh := make(chan ProposeReq, 8)

		nd, err := NewNode(Config{
			ID:        id,
			Peers:     peers,
			HTTPPeers: map[string]string{},
			DataDir:   b.TempDir(),
		}, applyCh, proposeCh, zerolog.Nop())
		if err != nil {
			b.Fatalf("NewNode %s: %v", id, err)
		}

		srv := &http.Server{Handler: nd.Handler()}
		go srv.Serve(lns[i]) //nolint:errcheck
		go nd.Run()
		nodes[i] = benchNode{nd: nd, srv: srv, ln: lns[i]}
	}

	// Drain all applyChans so appliers never block.
	for _, ch := range applyChans {
		ch := ch
		go func() {
			for range ch {
			}
		}()
	}

	b.Cleanup(func() {
		for _, n := range nodes {
			n.nd.Stop()
			n.srv.Close()
			n.ln.Close()
		}
	})

	// Wait for a leader to be elected.
	var leader *Node
	deadline := time.Now().Add(3 * time.Second)
	for leader == nil {
		if time.Now().After(deadline) {
			b.Fatal("no leader elected within 3s")
		}
		for _, n := range nodes {
			if n.nd.IsLeader() {
				leader = n.nd
				break
			}
		}
		if leader == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := leader.Propose(fixedCmd); err != nil {
			b.Fatalf("Propose: %v", err)
		}
	}
}
