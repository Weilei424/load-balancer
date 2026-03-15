package raftlite

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// saveSnapshot atomically writes snapshot.json via tmp→rename.
func saveSnapshot(dataDir string, snap Snapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dataDir, "snapshot.json.tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dataDir, "snapshot.json"))
}

// loadSnapshot reads snapshot.json. Returns zero Snapshot + false if missing.
func loadSnapshot(dataDir string) (Snapshot, bool, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "snapshot.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Snapshot{}, false, nil
		}
		return Snapshot{}, false, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, false, err
	}
	return snap, true, nil
}
