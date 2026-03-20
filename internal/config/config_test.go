package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadMissingFile(t *testing.T) {
	_, _, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !strings.Contains(err.Error(), "/nonexistent/path/config.yaml") {
		t.Errorf("error should contain path, got: %v", err)
	}
}

func TestLoadMinimal(t *testing.T) {
	f, err := os.CreateTemp("", "cfg*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString("id: n1\n")
	f.Close()

	cfg, dur, err := Load(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ID != "n1" {
		t.Errorf("ID = %q; want n1", cfg.ID)
	}
	if cfg.HTTP != "" || cfg.Raft != "" {
		t.Error("unset fields should be zero")
	}
	if dur != 0 {
		t.Errorf("dur = %v; want 0", dur)
	}
}

func TestLoadFullConfig(t *testing.T) {
	f, err := os.CreateTemp("", "cfg*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString(`id: n1
http: ":9001"
raft: ":10001"
peers:
  n2: ":10002"
http_peers:
  n2: ":9002"
data: "./data/n1"
health_interval: 10s
`)
	f.Close()

	cfg, dur, err := Load(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ID != "n1" {
		t.Errorf("ID = %q; want n1", cfg.ID)
	}
	if cfg.HTTP != ":9001" {
		t.Errorf("HTTP = %q; want :9001", cfg.HTTP)
	}
	if cfg.Raft != ":10001" {
		t.Errorf("Raft = %q; want :10001", cfg.Raft)
	}
	if cfg.Peers["n2"] != ":10002" {
		t.Errorf("Peers[n2] = %q; want :10002", cfg.Peers["n2"])
	}
	if cfg.HTTPPeers["n2"] != ":9002" {
		t.Errorf("HTTPPeers[n2] = %q; want :9002", cfg.HTTPPeers["n2"])
	}
	if cfg.Data != "./data/n1" {
		t.Errorf("Data = %q; want ./data/n1", cfg.Data)
	}
	if dur != 10*time.Second {
		t.Errorf("dur = %v; want 10s", dur)
	}
}

func TestLoadBadDuration(t *testing.T) {
	f, err := os.CreateTemp("", "cfg*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString("health_interval: not-a-duration\n")
	f.Close()

	_, _, err = Load(f.Name())
	if err == nil {
		t.Fatal("expected error for bad duration")
	}
	if !strings.Contains(err.Error(), "health_interval") {
		t.Errorf("error should mention health_interval, got: %v", err)
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	f, err := os.CreateTemp("", "cfg*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString("id: [\n")
	f.Close()

	_, _, err = Load(f.Name())
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestPeersStringEmpty(t *testing.T) {
	if s := PeersString(nil); s != "" {
		t.Errorf("nil map: got %q; want %q", s, "")
	}
	if s := PeersString(map[string]string{}); s != "" {
		t.Errorf("empty map: got %q; want %q", s, "")
	}
}

func TestPeersStringSingleEntry(t *testing.T) {
	got := PeersString(map[string]string{"n2": ":10002"})
	if got != "n2=:10002" {
		t.Errorf("got %q; want %q", got, "n2=:10002")
	}
}

func TestPeersStringDeterministic(t *testing.T) {
	m := map[string]string{
		"n3": ":10003",
		"n1": ":10001",
		"n2": ":10002",
	}
	got := PeersString(m)
	want := "n1=:10001,n2=:10002,n3=:10003"
	if got != want {
		t.Errorf("got %q; want %q", got, want)
	}

	// Verify it round-trips through split
	parts := strings.Split(got, ",")
	if len(parts) != 3 {
		t.Errorf("expected 3 parts, got %d", len(parts))
	}
}
