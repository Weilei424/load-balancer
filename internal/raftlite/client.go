package raftlite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// raftHTTPClient is shared for all Raft RPC calls.
// The timeout must comfortably exceed local HTTP handling plus fsync/write latency,
// otherwise vote/append RPCs can fail during elections on slower filesystems.
var raftHTTPClient = &http.Client{Timeout: 500 * time.Millisecond}

// snapshotHTTPClient uses a longer timeout because snapshot payloads can be large.
var snapshotHTTPClient = &http.Client{Timeout: 5 * time.Second}

func sendVote(addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	if err := doPostJSON(raftHTTPClient, addr+"/raft/vote", args, &reply); err != nil {
		return reply, err
	}
	return reply, nil
}

func sendAppend(addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if err := doPostJSON(raftHTTPClient, addr+"/raft/append", args, &reply); err != nil {
		return reply, err
	}
	return reply, nil
}

func sendInstallSnapshot(addr string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	if err := doPostJSON(snapshotHTTPClient, addr+"/raft/install-snapshot", args, &reply); err != nil {
		return reply, err
	}
	return reply, nil
}

func doPostJSON(client *http.Client, url string, body, reply interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	resp, err := client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(reply)
}
