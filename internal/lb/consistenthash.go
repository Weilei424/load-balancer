package lb

import (
	"hash/crc32"
	"sort"
	"strconv"
)

const virtualNodesPerWeight = 100

type ringNode struct {
	hash uint32
	url  string
}

// HashRing is a consistent hash ring of virtual nodes mapped to backend URLs.
type HashRing struct {
	nodes []ringNode // sorted by hash, ascending
}

// newHashRing builds the ring from the current healthy backends.
// Backends with weight <= 0 are treated as weight 1.
func newHashRing(backends []*Backend) *HashRing {
	var nodes []ringNode
	for _, b := range backends {
		if !b.Healthy {
			continue
		}
		w := b.Weight
		if w <= 0 {
			w = 1
		}
		for i := 0; i < w*virtualNodesPerWeight; i++ {
			key := b.URL + "#" + strconv.Itoa(i)
			h := crc32.ChecksumIEEE([]byte(key))
			nodes = append(nodes, ringNode{hash: h, url: b.URL})
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].hash < nodes[j].hash
	})
	return &HashRing{nodes: nodes}
}

// Get returns the backend URL for the given key, skipping backends in the
// skip set. Walks the ring clockwise from the hash of key; wraps around.
// Returns "" if the ring is empty or all backends are in skip.
func (r *HashRing) Get(key string, skip map[string]bool) string {
	if len(r.nodes) == 0 {
		return ""
	}
	h := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i].hash >= h
	})
	n := len(r.nodes)
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		node := r.nodes[(idx+i)%n]
		if seen[node.url] {
			continue
		}
		if skip[node.url] {
			seen[node.url] = true
			continue
		}
		return node.url
	}
	return ""
}
