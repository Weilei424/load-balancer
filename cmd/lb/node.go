package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Weilei424/load-balancer/internal/dashboard"
	"github.com/Weilei424/load-balancer/internal/lb"
	"github.com/Weilei424/load-balancer/internal/logging"
	"github.com/Weilei424/load-balancer/internal/metrics"
	"github.com/Weilei424/load-balancer/internal/raftlite"
)

func runNode(args []string) {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	id := fs.String("id", "n1", "node ID")
	httpAddr := fs.String("http", ":9001", "HTTP listen address (proxy + dashboard + admin)")
	raftAddr := fs.String("raft", ":10001", "Raft RPC listen address")
	peersStr := fs.String("peers", "", "Raft peers: id=raftAddr,... (e.g. n1=:10001,n2=:10002)")
	httpPeersStr := fs.String("http-peers", "", "HTTP peers: id=httpAddr,... (e.g. n1=:9001,n2=:9002)")
	dataDir := fs.String("data", "./data/n1", "data directory for persistence")
	healthInterval := fs.Duration("health-interval", 5*time.Second, "backend health-check interval")
	fs.Parse(args) //nolint:errcheck

	logger := logging.New(*id)

	raftPeers := parsePeers(*peersStr, *id)
	httpPeers := parsePeers(*httpPeersStr, *id)
	// Normalise addresses to http:// URLs.
	for k, v := range raftPeers {
		if !strings.HasPrefix(v, "http") {
			raftPeers[k] = "http://" + v
		}
	}
	for k, v := range httpPeers {
		if !strings.HasPrefix(v, "http") {
			httpPeers[k] = "http://" + v
		}
	}

	// Create config state machine and metrics.
	config := lb.NewConfigState()
	met := metrics.New()

	// Channels bridging raft ↔ state machine.
	applyCh := make(chan raftlite.LogEntry, 256)
	proposeCh := make(chan raftlite.ProposeReq, 8)

	// Create raft node.
	raftNode, err := raftlite.NewNode(raftlite.Config{
		ID:        *id,
		Peers:     raftPeers,
		HTTPPeers: httpPeers,
		DataDir:   *dataDir,
	}, applyCh, proposeCh, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create raft node")
	}

	// Dashboard.
	broadcaster := dashboard.NewBroadcaster()
	dashHandler := dashboard.NewHandler(raftNode, config, met, broadcaster)

	// Health checker.
	healthChecker := lb.NewHealthChecker(config, *healthInterval, logger)
	healthChecker.Start()

	// Applier goroutine: consume committed log entries → apply to config → broadcast.
	go func() {
		for entry := range applyCh {
			var cmd lb.Command
			if err := json.Unmarshal(entry.Command, &cmd); err != nil {
				logger.Error().Err(err).Msg("unmarshal command")
				continue
			}
			config.Apply(cmd)
			logger.Info().
				Str("op", string(cmd.Op)).
				Str("url", cmd.URL).
				Int("index", entry.Index).
				Msg("applied command")
			broadcaster.Broadcast(dashHandler.BuildSnapshot())
		}
	}()

	// Start Raft node.
	go raftNode.Run()

	// Periodic dashboard broadcast (for live raft status even without config changes).
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for range t.C {
			// Update raft metrics.
			s := raftNode.Status()
			roleInt := int32(0)
			switch s.Role {
			case "candidate":
				roleInt = 1
			case "leader":
				roleInt = 2
			}
			met.SetRaft(s.Term, roleInt)
			broadcaster.Broadcast(dashHandler.BuildSnapshot())
		}
	}()

	// Raft RPC server (dedicated port).
	raftMux := raftNode.Handler()
	go func() {
		logger.Info().Str("addr", *raftAddr).Msg("raft server starting")
		if err := http.ListenAndServe(*raftAddr, raftMux); err != nil {
			logger.Fatal().Err(err).Msg("raft server error")
		}
	}()

	// Main HTTP server: dashboard + metrics + admin + proxy.
	mainMux := http.NewServeMux()

	// SSE events.
	mainMux.HandleFunc("/events", dashHandler.ServeHTTP)

	// Metrics.
	mainMux.Handle("/metrics", met.Handler())

	// Admin routes (leader-only; followers 307 redirect).
	adminHandler := lb.NewAdminHandler(raftNode, logger)
	mainMux.Handle("/admin/", adminHandler)

	// Proxy + dashboard catch-all.
	proxy := lb.NewProxy(config, met, logger)
	mainMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/dashboard" {
			dashHandler.ServeHTTP(w, r)
			return
		}
		proxy.ServeHTTP(w, r)
	})

	logger.Info().Str("addr", *httpAddr).Msg("http server starting")
	fmt.Printf("dashboard: http://localhost%s/\n", *httpAddr)
	if err := http.ListenAndServe(*httpAddr, mainMux); err != nil {
		logger.Fatal().Err(err).Msg("http server error")
	}
}

// parsePeers parses "id1=addr1,id2=addr2" into a map, excluding the local node.
func parsePeers(s, localID string) map[string]string {
	out := make(map[string]string)
	if s == "" {
		return out
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		peerID := strings.TrimSpace(kv[0])
		addr := strings.TrimSpace(kv[1])
		if peerID == localID {
			continue // skip self
		}
		out[peerID] = addr
	}
	return out
}
