package config

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// NodeConfig holds all node settings loadable from a YAML file.
// health_interval is a string (e.g. "5s") parsed by Load.
type NodeConfig struct {
	ID             string            `yaml:"id"`
	HTTP           string            `yaml:"http"`
	Raft           string            `yaml:"raft"`
	Peers          map[string]string `yaml:"peers"`
	HTTPPeers      map[string]string `yaml:"http_peers"`
	Data           string            `yaml:"data"`
	HealthInterval string            `yaml:"health_interval"`
}

// Load reads and decodes the YAML file at path.
// Returns the config, parsed health_interval duration (0 if unset), and any error.
func Load(path string) (NodeConfig, time.Duration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return NodeConfig{}, 0, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg NodeConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return NodeConfig{}, 0, fmt.Errorf("parse config %s: %w", path, err)
	}

	var dur time.Duration
	if cfg.HealthInterval != "" {
		dur, err = time.ParseDuration(cfg.HealthInterval)
		if err != nil {
			return NodeConfig{}, 0, fmt.Errorf("health_interval %q: %w", cfg.HealthInterval, err)
		}
	}

	return cfg, dur, nil
}

// PeersString converts map[id]addr to "id1=addr1,id2=addr2" (sorted, stable).
// Returns "" for nil/empty maps. Compatible with parsePeers in cmd/lb/node.go.
func PeersString(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ",")
}

// StrOr returns s if non-empty, else fallback.
func StrOr(s, fallback string) string {
	if s != "" {
		return s
	}
	return fallback
}
