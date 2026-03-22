package integration

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Weilei424/load-balancer/internal/lb"
	"github.com/Weilei424/load-balancer/internal/raftlite"
)

// partitionedTransport wraps a real RoundTripper and selectively drops requests
// to specific destination addresses, simulating a network partition. Each node
// in the test cluster gets its own instance; blocking a peer's host:port makes
// all outbound Raft RPCs to that peer fail with net.ErrClosed, just as a
// refused TCP connection would.
type partitionedTransport struct {
	base    http.RoundTripper
	mu      sync.RWMutex
	blocked map[string]bool // destination host:port → drop
}

func newPartitionedTransport() *partitionedTransport {
	return &partitionedTransport{
		base:    http.DefaultTransport,
		blocked: make(map[string]bool),
	}
}

func (pt *partitionedTransport) block(hostPort string) {
	pt.mu.Lock()
	pt.blocked[hostPort] = true
	pt.mu.Unlock()
}

func (pt *partitionedTransport) unblock(hostPort string) {
	pt.mu.Lock()
	delete(pt.blocked, hostPort)
	pt.mu.Unlock()
}

func (pt *partitionedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	pt.mu.RLock()
	drop := pt.blocked[req.URL.Host]
	pt.mu.RUnlock()
	if drop {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: net.ErrClosed}
	}
	return pt.base.RoundTrip(req)
}

// bootPartitionCluster creates n raftlite nodes each equipped with a
// partitionedTransport for controllable partition injection. It returns the
// cluster nodes, per-node transports, and per-node raft base addresses (as
// "host:port" strings, matching the key format used by partitionedTransport).
func bootPartitionCluster(t *testing.T, n int) ([]*clusterNode, []*partitionedTransport, []string) {
	t.Helper()

	lns := make([]net.Listener, n)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		lns[i] = ln
	}

	ids := make([]string, n)
	addrMap := make(map[string]string, n) // id → "http://host:port"
	hostPorts := make([]string, n)        // index → "host:port"
	for i := range ids {
		ids[i] = string(rune('a' + i))
		addrMap[ids[i]] = "http://" + lns[i].Addr().String()
		hostPorts[i] = lns[i].Addr().String()
	}

	transports := make([]*partitionedTransport, n)
	for i := range transports {
		transports[i] = newPartitionedTransport()
	}

	nodes := make([]*clusterNode, n)
	for i, id := range ids {
		peers := make(map[string]string, n-1)
		for pid, pa := range addrMap {
			if pid != id {
				peers[pid] = pa
			}
		}
		applyCh := make(chan raftlite.LogEntry, 64)
		// Drain apply channel so it never blocks the node.
		go func(ch chan raftlite.LogEntry) {
			for range ch {
			}
		}(applyCh)
		proposeCh := make(chan raftlite.ProposeReq, 8)
		nd, err := raftlite.NewNode(raftlite.Config{
			ID:        id,
			Peers:     peers,
			HTTPPeers: make(map[string]string),
			DataDir:   t.TempDir(),
			Transport: transports[i],
		}, applyCh, proposeCh, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}
		srv := &http.Server{Handler: nd.Handler()}
		go srv.Serve(lns[i]) //nolint:errcheck
		go nd.Run()
		nodes[i] = &clusterNode{Node: nd, ln: lns[i], srv: srv}
	}

	t.Cleanup(func() {
		for _, cn := range nodes {
			cn.stop()
		}
	})

	return nodes, transports, hostPorts
}

// waitForLeaderAmong polls a subset of nodes for a leader, up to timeout.
func waitForLeaderAmong(t *testing.T, nodes []*clusterNode, timeout time.Duration) *clusterNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected among %d nodes within %v", len(nodes), timeout)
	return nil
}

// proposeWithTimeout calls Propose in a goroutine and returns the result or a
// timeout error after d. The goroutine is fire-and-forget: if it times out it
// will unblock eventually via drainPending (when the node steps down or stops).
func proposeWithTimeout(node *raftlite.Node, cmd json.RawMessage, d time.Duration) error {
	ch := make(chan error, 1)
	go func() { ch <- node.Propose(cmd) }()
	select {
	case err := <-ch:
		return err
	case <-time.After(d):
		return fmt.Errorf("propose timed out after %v", d)
	}
}

// waitForCommitConvergence polls until every node has CommitIndex >= minCommit,
// or fatally fails after timeout.
func waitForCommitConvergence(t *testing.T, nodes []*clusterNode, minCommit int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range nodes {
			if n.Status().CommitIndex < minCommit {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i, n := range nodes {
		t.Logf("node %d commitIndex=%d (want >=%d)", i, n.Status().CommitIndex, minCommit)
	}
	t.Fatalf("nodes did not converge to commitIndex >= %d within %v", minCommit, timeout)
}

// TestNetworkPartition boots a 5-node cluster, cuts it into a 2-node minority
// and a 3-node majority, asserts the minority cannot commit, the majority can,
// and that the minority nodes catch up after the partition is healed.
//
// Timing budget (worst case < 15 s):
//
//	0–3 s   initial election
//	3–4.2 s minority propose attempt (1.2 s timeout)
//	4.2–8.2 s majority leader + commit (4 s budget)
//	8.2–8.3 s heal
//	8.3–13.3 s convergence (5 s budget)
func TestNetworkPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network-partition integration test in short mode")
	}

	nodes, transports, hostPorts := bootPartitionCluster(t, 5)

	_ = waitForLeader(t, nodes, 3*time.Second)

	// Partition: minority = nodes[0,1], majority = nodes[2,3,4].
	// Apply symmetric block in both directions.
	minority := []int{0, 1}
	majority := []int{2, 3, 4}
	for _, mi := range minority {
		for _, mj := range majority {
			transports[mi].block(hostPorts[mj])
			transports[mj].block(hostPorts[mi])
		}
	}

	// Assert minority cannot commit a new entry.
	// nodes[0] may be a follower ("not leader" error) or the isolated leader
	// (blocks until heal). Either outcome means minority cannot commit.
	minCmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: "http://minority:8001", Weight: 1})
	minorityErr := proposeWithTimeout(nodes[0].Node, minCmd, 1200*time.Millisecond)
	if minorityErr == nil {
		t.Fatal("minority partition committed an entry — split-brain detected")
	}

	// Assert majority elects a leader (or keeps the existing one) and can commit.
	majorityNodes := nodes[2:]
	majLeader := waitForLeaderAmong(t, majorityNodes, 4*time.Second)
	majCmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: "http://majority:8002", Weight: 1})
	if err := majLeader.Propose(majCmd); err != nil {
		t.Fatalf("majority leader could not commit: %v", err)
	}
	majCommit := majLeader.Status().CommitIndex

	// Heal the partition.
	for _, mi := range minority {
		for _, mj := range majority {
			transports[mi].unblock(hostPorts[mj])
			transports[mj].unblock(hostPorts[mi])
		}
	}

	// After healing, nodes[0] is drained via stepDown → drainPending.
	// If it was still blocked, the error will arrive within a heartbeat interval.
	// We already stored minorityErr above; nothing more to check here.

	// Assert all 5 nodes converge to at least majCommit.
	waitForCommitConvergence(t, nodes, majCommit, 5*time.Second)

	// Assert minority nodes stepped down (no stale leader in the minority).
	for _, mi := range minority {
		if nodes[mi].IsLeader() {
			t.Errorf("minority node %d is still leader after partition heal", mi)
		}
	}
}

// TestSplitBrainPrevention boots a 3-node cluster, isolates the leader, lets
// the followers elect a new leader, and verifies that:
//  1. The isolated leader cannot commit (no majority).
//  2. The new majority leader can commit.
//  3. After healing the isolated leader steps down and converges to the
//     majority log — its uncommitted entry is overwritten and never applied.
func TestSplitBrainPrevention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping split-brain integration test in short mode")
	}

	nodes, transports, hostPorts := bootPartitionCluster(t, 3)

	// Identify the original leader (leaderA).
	leaderA := waitForLeader(t, nodes, 3*time.Second)
	leaderIdx := -1
	for i, n := range nodes {
		if n.Node == leaderA.Node {
			leaderIdx = i
			break
		}
	}

	// Isolate leaderA from both followers (symmetric).
	for i := range nodes {
		if i == leaderIdx {
			continue
		}
		transports[leaderIdx].block(hostPorts[i])
		transports[i].block(hostPorts[leaderIdx])
	}

	// Wait for the two followers to elect a new leader (leaderB, higher term).
	followerNodes := make([]*clusterNode, 0, 2)
	for i, n := range nodes {
		if i != leaderIdx {
			followerNodes = append(followerNodes, n)
		}
	}
	leaderB := waitForLeaderAmong(t, followerNodes, 3*time.Second)

	// Attempt to commit on the isolated leaderA.
	// It cannot replicate to a majority → should time out or return "not leader".
	staleCmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: "http://stale:8001", Weight: 1})
	staleErr := proposeWithTimeout(leaderA.Node, staleCmd, 1200*time.Millisecond)
	// staleErr is only checked after heal below — record for deferred assertion.

	// Commit on leaderB — must succeed (it has 2/3 majority).
	goodCmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: "http://good:8002", Weight: 1})
	if err := leaderB.Propose(goodCmd); err != nil {
		t.Fatalf("new majority leader could not commit: %v", err)
	}
	goodCommit := leaderB.Status().CommitIndex

	// Heal the partition.
	for i := range nodes {
		if i == leaderIdx {
			continue
		}
		transports[leaderIdx].unblock(hostPorts[i])
		transports[i].unblock(hostPorts[leaderIdx])
	}

	// Deferred assertion: stale propose must not have committed.
	if staleErr == nil {
		t.Error("isolated leader committed an entry during partition — split-brain detected")
	}

	// leaderA must step down after receiving a higher-term AppendEntries from leaderB.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !leaderA.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leaderA.IsLeader() {
		t.Error("original leader should have stepped down after partition heal but is still leader")
	}

	// All 3 nodes must converge to at least goodCommit (the majority's log).
	waitForCommitConvergence(t, nodes, goodCommit, 5*time.Second)
}
