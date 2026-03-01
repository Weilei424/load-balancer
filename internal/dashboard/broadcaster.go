package dashboard

import (
	"encoding/json"
	"sync"
)

// Broadcaster fans out NodeSnapshot events to all connected SSE clients.
type Broadcaster struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

// NewBroadcaster creates a new Broadcaster.
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{clients: make(map[chan []byte]struct{})}
}

// Subscribe registers a new SSE client channel and returns it.
// The channel is buffered; slow clients drop events rather than blocking.
func (b *Broadcaster) Subscribe() chan []byte {
	ch := make(chan []byte, 16)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes and closes the client channel.
func (b *Broadcaster) Unsubscribe(ch chan []byte) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
	close(ch)
}

// Broadcast sends a NodeSnapshot to all connected clients (non-blocking).
func (b *Broadcaster) Broadcast(snap NodeSnapshot) {
	data, err := json.Marshal(snap)
	if err != nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.clients {
		select {
		case ch <- data:
		default:
			// Slow client — drop event.
		}
	}
}
