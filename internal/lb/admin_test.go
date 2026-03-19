package lb

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
)

// mockRaftNode implements RaftNode with configurable behaviour for unit tests.
type mockRaftNode struct {
	isLeader       bool
	leaderHTTPAddr string
	proposeErr     error
	proposeCalled  bool
	lastProposed   json.RawMessage
}

func (m *mockRaftNode) IsLeader() bool         { return m.isLeader }
func (m *mockRaftNode) LeaderHTTPAddr() string { return m.leaderHTTPAddr }
func (m *mockRaftNode) Propose(cmd json.RawMessage) error {
	m.proposeCalled = true
	m.lastProposed = cmd
	return m.proposeErr
}

// newTestHandler builds an AdminHandler backed by the mock, bypassing the
// concrete *raftlite.Node requirement of NewAdminHandler.
func newTestHandler(mock *mockRaftNode) *AdminHandler {
	return &AdminHandler{node: mock, log: zerolog.Nop()}
}

func TestAdminHandlerLeaderPath(t *testing.T) {
	mock := &mockRaftNode{isLeader: true}
	h := newTestHandler(mock)

	body, _ := json.Marshal(map[string]interface{}{"url": "http://b1:9101", "weight": 2})
	req := httptest.NewRequest(http.MethodPost, "/admin/backends", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if !mock.proposeCalled {
		t.Fatal("Propose was not called")
	}
	var cmd Command
	if err := json.Unmarshal(mock.lastProposed, &cmd); err != nil {
		t.Fatalf("unmarshal proposed command: %v", err)
	}
	if cmd.Op != OpAddBackend || cmd.URL != "http://b1:9101" || cmd.Weight != 2 {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestAdminHandlerFollowerRedirect(t *testing.T) {
	mock := &mockRaftNode{isLeader: false, leaderHTTPAddr: "http://leader:9001"}
	h := newTestHandler(mock)

	body, _ := json.Marshal(map[string]string{"url": "http://b1:9101"})
	req := httptest.NewRequest(http.MethodPost, "/admin/backends", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("want 307, got %d", rr.Code)
	}
	loc := rr.Header().Get("Location")
	want := "http://leader:9001/admin/backends"
	if loc != want {
		t.Errorf("Location: want %q, got %q", want, loc)
	}
	if mock.proposeCalled {
		t.Fatal("Propose should not be called on a follower")
	}
}

func TestAdminHandlerNoLeader(t *testing.T) {
	mock := &mockRaftNode{isLeader: false, leaderHTTPAddr: ""}
	h := newTestHandler(mock)

	req := httptest.NewRequest(http.MethodPost, "/admin/backends", bytes.NewReader([]byte(`{"url":"http://b:9101"}`)))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rr.Code)
	}
}

func TestAdminHandlerBadRequest(t *testing.T) {
	mock := &mockRaftNode{isLeader: true}
	h := newTestHandler(mock)

	// Missing "url" field.
	body, _ := json.Marshal(map[string]int{"weight": 1})
	req := httptest.NewRequest(http.MethodPost, "/admin/backends", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rr.Code, rr.Body.String())
	}
	if mock.proposeCalled {
		t.Fatal("Propose should not be called for a bad request")
	}
}

func TestAdminHandlerRemoveBackend(t *testing.T) {
	mock := &mockRaftNode{isLeader: true}
	h := newTestHandler(mock)

	body, _ := json.Marshal(map[string]string{"url": "http://b1:9101"})
	req := httptest.NewRequest(http.MethodDelete, "/admin/backends", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var cmd Command
	json.Unmarshal(mock.lastProposed, &cmd) //nolint:errcheck
	if cmd.Op != OpRemoveBackend || cmd.URL != "http://b1:9101" {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestAdminHandlerSetWeight(t *testing.T) {
	mock := &mockRaftNode{isLeader: true}
	h := newTestHandler(mock)

	body, _ := json.Marshal(map[string]interface{}{"url": "http://b1:9101", "weight": 3})
	req := httptest.NewRequest(http.MethodPatch, "/admin/backends", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var cmd Command
	json.Unmarshal(mock.lastProposed, &cmd) //nolint:errcheck
	if cmd.Op != OpSetWeight || cmd.Weight != 3 {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestAdminHandlerSetAlgorithm(t *testing.T) {
	mock := &mockRaftNode{isLeader: true}
	h := newTestHandler(mock)

	body, _ := json.Marshal(map[string]string{"algorithm": "least_conn"})
	req := httptest.NewRequest(http.MethodPut, "/admin/algorithm", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var cmd Command
	json.Unmarshal(mock.lastProposed, &cmd) //nolint:errcheck
	if cmd.Op != OpSetAlgorithm || cmd.Algorithm != "least_conn" {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestAdminHandlerBadRequestRemove(t *testing.T) {
	mock := &mockRaftNode{isLeader: true}
	h := newTestHandler(mock)

	// Empty url field.
	body, _ := json.Marshal(map[string]string{})
	req := httptest.NewRequest(http.MethodDelete, "/admin/backends", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}

	// Malformed JSON.
	req2 := httptest.NewRequest(http.MethodDelete, "/admin/backends", bytes.NewReader([]byte("not-json")))
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for bad JSON, got %d", rr2.Code)
	}
}

func TestAdminHandlerBadRequestSetWeight(t *testing.T) {
	mock := &mockRaftNode{isLeader: true}
	h := newTestHandler(mock)

	req := httptest.NewRequest(http.MethodPatch, "/admin/backends", bytes.NewReader([]byte("not-json")))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestAdminHandlerBadRequestSetAlgorithm(t *testing.T) {
	mock := &mockRaftNode{isLeader: true}
	h := newTestHandler(mock)

	req := httptest.NewRequest(http.MethodPut, "/admin/algorithm", bytes.NewReader([]byte("not-json")))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestAdminHandlerSetWeightValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing url", `{"weight":2}`},
		{"empty url", `{"url":"","weight":2}`},
		{"zero weight", `{"url":"http://b:9101","weight":0}`},
		{"negative weight", `{"url":"http://b:9101","weight":-1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockRaftNode{isLeader: true}
			h := newTestHandler(mock)
			req := httptest.NewRequest(http.MethodPatch, "/admin/backends", bytes.NewReader([]byte(tc.body)))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %s", rr.Code, rr.Body.String())
			}
			if mock.proposeCalled {
				t.Fatal("Propose must not be called for invalid input")
			}
		})
	}
}

func TestAdminHandlerSetAlgorithmValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty algorithm", `{"algorithm":""}`},
		{"unknown algorithm", `{"algorithm":"random"}`},
		{"typo", `{"algorithm":"round-robin"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockRaftNode{isLeader: true}
			h := newTestHandler(mock)
			req := httptest.NewRequest(http.MethodPut, "/admin/algorithm", bytes.NewReader([]byte(tc.body)))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d: %s", rr.Code, rr.Body.String())
			}
			if mock.proposeCalled {
				t.Fatal("Propose must not be called for invalid algorithm")
			}
		})
	}
}

func TestAdminHandlerSetAlgorithmConsistentHash(t *testing.T) {
	mock := &mockRaftNode{isLeader: true}
	h := newTestHandler(mock)

	body, _ := json.Marshal(map[string]string{"algorithm": "consistent_hash"})
	req := httptest.NewRequest(http.MethodPut, "/admin/algorithm", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200 for consistent_hash, got %d: %s", rr.Code, rr.Body.String())
	}
	var cmd Command
	json.Unmarshal(mock.lastProposed, &cmd) //nolint:errcheck
	if cmd.Op != OpSetAlgorithm || cmd.Algorithm != AlgoConsistentHash {
		t.Errorf("unexpected command: %+v", cmd)
	}
}

func TestAdminHandlerProposeError(t *testing.T) {
	mock := &mockRaftNode{isLeader: true, proposeErr: errors.New("not leader")}
	h := newTestHandler(mock)

	body, _ := json.Marshal(map[string]string{"url": "http://b1:9101"})
	req := httptest.NewRequest(http.MethodPost, "/admin/backends", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d: %s", rr.Code, rr.Body.String())
	}
	var result map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&result); err != nil {
		t.Fatalf("response not valid JSON: %v", err)
	}
	if result["error"] == "" {
		t.Error("expected non-empty error field in JSON response")
	}
}
