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
// cluster nodes, per-node transports, per-node raft host:port strings (for use
// in block/unblock calls), and the per-node apply channels so callers can wire
// up state machines.
func bootPartitionCluster(t *testing.T, n int) ([]*clusterNode, []*partitionedTransport, []string, []chan raftlite.LogEntry) {
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
	applyChans := make([]chan raftlite.LogEntry, n)
	for i, id := range ids {
		peers := make(map[string]string, n-1)
		for pid, pa := range addrMap {
			if pid != id {
				peers[pid] = pa
			}
		}
		applyCh := make(chan raftlite.LogEntry, 64)
		applyChans[i] = applyCh
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

	return nodes, transports, hostPorts, applyChans
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

// hasBackend reports whether cs contains a backend with the given URL.
func hasBackend(cs *lb.ConfigState, url string) bool {
	for _, b := range cs.AllBackends() {
		if b.URL == url {
			return true
		}
	}
	return false
}

// TestNetworkPartition boots a 5-node cluster and cuts it such that the
// pre-partition leader is always in the 2-node minority. It verifies:
//
//  1. The isolated leader cannot commit (no majority).
//  2. The 3-node majority elects a new leader and commits successfully.
//  3. After healing, the minority nodes step down, their stale log entries are
//     truncated, and all 5 nodes converge to the same log and config state.
//
// Timing budget (worst case < 15 s):
//
//	0–3 s   initial election
//	3–4.2 s minority propose attempt (1.2 s timeout)
//	4.2–8.2 s majority leader + commit (4 s budget)
//	8.2–8.3 s heal
//	8.3–14.3 s convergence (state machine + log-length, 6 s total budget)
func TestNetworkPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network-partition integration test in short mode")
	}

	nodes, transports, hostPorts, applyChans := bootPartitionCluster(t, 5)
	states := attachConfigStates(applyChans)

	// Identify the pre-partition leader so the partition is built around it.
	origLeader := waitForLeader(t, nodes, 3*time.Second)
	leaderIdx := -1
	for i, n := range nodes {
		if n.Node == origLeader.Node {
			leaderIdx = i
			break
		}
	}

	// Minority = {origLeader, first non-leader node}.  This guarantees we always
	// test the dangerous case: a partitioned leader with 2/5 nodes cannot commit.
	minority := []int{leaderIdx}
	for i := range nodes {
		if i != leaderIdx && len(minority) < 2 {
			minority = append(minority, i)
		}
	}
	minoritySet := map[int]bool{minority[0]: true, minority[1]: true}
	majority := make([]int, 0, 3)
	for i := range nodes {
		if !minoritySet[i] {
			majority = append(majority, i)
		}
	}

	// Apply symmetric partition: minority ↔ majority traffic is dropped.
	for _, mi := range minority {
		for _, mj := range majority {
			transports[mi].block(hostPorts[mj])
			transports[mj].block(hostPorts[mi])
		}
	}

	// Assert the isolated leader cannot commit.  Because nodes[leaderIdx] IS
	// the leader, Propose will actually attempt replication — and block because
	// it cannot reach a majority.  proposeWithTimeout enforces the time bound.
	const staleURL = "http://minority:8001"
	minCmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: staleURL, Weight: 1})
	minorityErr := proposeWithTimeout(nodes[leaderIdx].Node, minCmd, 1200*time.Millisecond)
	if minorityErr == nil {
		t.Fatal("isolated leader committed an entry — split-brain detected")
	}

	// Assert majority elects a leader (or keeps the existing one) and can commit.
	majorityNodes := make([]*clusterNode, len(majority))
	for i, mi := range majority {
		majorityNodes[i] = nodes[mi]
	}
	majLeader := waitForLeaderAmong(t, majorityNodes, 4*time.Second)
	const goodURL = "http://majority:8002"
	majCmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: goodURL, Weight: 1})
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

	// CommitIndex convergence: all 5 nodes must reach majCommit.
	waitForCommitConvergence(t, nodes, majCommit, 5*time.Second)

	// State-machine convergence: all 5 ConfigStates must apply the committed
	// command.  This directly proves the majority's entry was applied everywhere.
	waitForConvergence(t, states, 1, 2*time.Second)
	for i, cs := range states {
		if !hasBackend(cs, goodURL) {
			t.Errorf("node %d: config state missing committed backend %s", i, goodURL)
		}
		// The stale command was never committed, so it must not appear in any
		// node's state machine — verifying split-brain safety end-to-end.
		if hasBackend(cs, staleURL) {
			t.Errorf("node %d: config state contains uncommitted stale backend %s", i, staleURL)
		}
	}

	// Log-length convergence: no node should have uncommitted tail entries.
	// After heal, the minority leader's stale log entry is truncated by
	// handleAppendEntries; all nodes should end up with LogLength == CommitIndex.
	for i, n := range nodes {
		s := n.Status()
		if s.LogLength != s.CommitIndex {
			t.Errorf("node %d: LogLength=%d != CommitIndex=%d (divergent log tail not cleaned up)",
				i, s.LogLength, s.CommitIndex)
		}
	}

	// Minority nodes must have stepped down after seeing a higher-term leader.
	for _, mi := range minority {
		if nodes[mi].IsLeader() {
			t.Errorf("minority node %d is still leader after partition heal", mi)
		}
	}
}

// TestSplitBrainPrevention boots a 3-node cluster, isolates the leader, and
// verifies that:
//
//  1. The isolated leader cannot commit (only 1/3 nodes).
//  2. The two followers elect a new higher-term leader and commit.
//  3. After healing the original leader steps down, its uncommitted log entry
//     is overwritten, and every node's config state contains only the majority
//     leader's entry — proving the stale entry was never applied.
func TestSplitBrainPrevention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping split-brain integration test in short mode")
	}

	nodes, transports, hostPorts, applyChans := bootPartitionCluster(t, 3)
	states := attachConfigStates(applyChans)

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

	// Attempt to commit on the isolated leaderA (1/3 — no majority).
	// proposeWithTimeout will time out; the goroutine drains via stepDown after heal.
	const staleURL = "http://stale:8001"
	staleCmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: staleURL, Weight: 1})
	staleErr := proposeWithTimeout(leaderA.Node, staleCmd, 1200*time.Millisecond)

	// Commit on leaderB — must succeed (2/3 majority).
	const goodURL = "http://good:8002"
	goodCmd, _ := json.Marshal(lb.Command{Op: lb.OpAddBackend, URL: goodURL, Weight: 1})
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

	// The isolated leader's propose must not have committed.
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
		t.Error("original leader did not step down after partition heal")
	}

	// CommitIndex convergence: all 3 nodes must reach goodCommit.
	waitForCommitConvergence(t, nodes, goodCommit, 5*time.Second)

	// State-machine convergence: all 3 ConfigStates must have applied goodCmd
	// and must NOT have applied staleCmd.  This is the key safety assertion:
	// the stale entry was never committed, so it must never reach the state machine.
	waitForConvergence(t, states, 1, 2*time.Second)
	for i, cs := range states {
		if !hasBackend(cs, goodURL) {
			t.Errorf("node %d: config state missing committed backend %s", i, goodURL)
		}
		if hasBackend(cs, staleURL) {
			t.Errorf("node %d: config state contains stale backend %s — split-brain entry was applied", i, staleURL)
		}
	}

	// Log-length convergence: after leaderA's stale entry is truncated by
	// handleAppendEntries, all nodes should have LogLength == CommitIndex with no
	// divergent tail.
	for i, n := range nodes {
		s := n.Status()
		if s.LogLength != s.CommitIndex {
			t.Errorf("node %d: LogLength=%d != CommitIndex=%d (stale log entry not truncated)",
				i, s.LogLength, s.CommitIndex)
		}
	}
}
