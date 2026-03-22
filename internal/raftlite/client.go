package raftlite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

func (n *Node) sendVote(addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	if err := doPostJSON(n.raftClient, addr+"/raft/vote", args, &reply); err != nil {
		return reply, err
	}
	return reply, nil
}

func (n *Node) sendAppend(addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if err := doPostJSON(n.raftClient, addr+"/raft/append", args, &reply); err != nil {
		return reply, err
	}
	return reply, nil
}

func (n *Node) sendInstallSnapshot(addr string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	if err := doPostJSON(n.snapClient, addr+"/raft/install-snapshot", args, &reply); err != nil {
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
