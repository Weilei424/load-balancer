package raftlite

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type persistedMeta struct {
	CurrentTerm int    `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

// saveMeta atomically persists currentTerm and votedFor to state.json via rename.
func saveMeta(dataDir string, term int, votedFor string) error {
	data, err := json.Marshal(persistedMeta{CurrentTerm: term, VotedFor: votedFor})
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	tmp := filepath.Join(dataDir, "state.json.tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write meta tmp: %w", err)
	}
	return os.Rename(tmp, filepath.Join(dataDir, "state.json"))
}

// loadMeta reads currentTerm and votedFor from state.json.
// Returns zero values if the file does not exist (first boot).
func loadMeta(dataDir string) (int, string, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "state.json"))
	if os.IsNotExist(err) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("read meta: %w", err)
	}
	var m persistedMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return 0, "", fmt.Errorf("unmarshal meta: %w", err)
	}
	return m.CurrentTerm, m.VotedFor, nil
}

type persistedCommit struct {
	CommitIndex int `json:"commit_index"`
}

// saveCommit atomically persists commitIndex to commit.json via rename.
func saveCommit(dataDir string, idx int) error {
	data, err := json.Marshal(persistedCommit{CommitIndex: idx})
	if err != nil {
		return fmt.Errorf("marshal commit: %w", err)
	}
	tmp := filepath.Join(dataDir, "commit.json.tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write commit tmp: %w", err)
	}
	return os.Rename(tmp, filepath.Join(dataDir, "commit.json"))
}

// loadCommit reads commitIndex from commit.json.
// Returns 0 if the file does not exist (first boot or pre-existing node).
func loadCommit(dataDir string) (int, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "commit.json"))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read commit: %w", err)
	}
	var c persistedCommit
	if err := json.Unmarshal(data, &c); err != nil {
		return 0, fmt.Errorf("unmarshal commit: %w", err)
	}
	return c.CommitIndex, nil
}

// appendLogEntryDisk appends a single log entry to log.jsonl (append-only).
func appendLogEntryDisk(dataDir string, entry LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dataDir, "log.jsonl"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open log.jsonl: %w", err)
	}
	defer f.Close()
	_, err = f.Write(append(data, '\n'))
	return err
}

// rewriteLogDisk rewrites the full log to log.jsonl atomically (used after truncation).
func rewriteLogDisk(dataDir string, entries []LogEntry) error {
	path := filepath.Join(dataDir, "log.jsonl")
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open log tmp: %w", err)
	}
	enc := json.NewEncoder(f)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			f.Close()
			return fmt.Errorf("encode entry: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadLogDisk reads all log entries from log.jsonl.
func loadLogDisk(dataDir string) ([]LogEntry, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "log.jsonl"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read log.jsonl: %w", err)
	}
	var entries []LogEntry
	start := 0
	for i := 0; i <= len(data); i++ {
		if i == len(data) || data[i] == '\n' {
			line := data[start:i]
			start = i + 1
			if len(line) == 0 {
				continue
			}
			var e LogEntry
			if err := json.Unmarshal(line, &e); err != nil {
				return nil, fmt.Errorf("unmarshal log line: %w", err)
			}
			entries = append(entries, e)
		}
	}
	return entries, nil
}
