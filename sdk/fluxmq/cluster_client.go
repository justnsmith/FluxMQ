package fluxmq

import (
	"fmt"
	"sync"
	"time"
)

// ClusterClient manages connections to all brokers in a FluxMQ cluster and
// routes produce/fetch requests to the correct partition leader.
type ClusterClient struct {
	seedAddr string

	mu      sync.RWMutex
	clients map[int32]*Client // brokerID → Client
	meta    *ClusterMetadata

	// leaderOf maps "topic:partition" → brokerID for quick routing.
	leaderOf map[string]int32
}

// NewClusterClient dials the seed broker, fetches cluster metadata, and
// returns a ClusterClient ready to route requests.
func NewClusterClient(seedAddr string) (*ClusterClient, error) {
	cc := &ClusterClient{
		seedAddr: seedAddr,
		clients:  make(map[int32]*Client),
		leaderOf: make(map[string]int32),
	}
	if err := cc.RefreshMetadata(); err != nil {
		return nil, err
	}
	return cc, nil
}

// Close shuts down all broker connections.
func (cc *ClusterClient) Close() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, c := range cc.clients {
		c.Close()
	}
	cc.clients = make(map[int32]*Client)
}

// RefreshMetadata fetches v1 cluster metadata from a live broker and
// updates the routing table and broker connections.
// It tries the seed address first; if unreachable, it falls back to any
// broker address known from the previous metadata fetch.
func (cc *ClusterClient) RefreshMetadata() error {
	// Build candidate list: seed first, then all known broker addresses.
	addrs := []string{cc.seedAddr}
	cc.mu.RLock()
	if cc.meta != nil {
		for _, b := range cc.meta.Brokers {
			candidate := fmt.Sprintf("%s:%d", b.Host, b.Port)
			if candidate != cc.seedAddr {
				addrs = append(addrs, candidate)
			}
		}
	}
	cc.mu.RUnlock()

	var meta *ClusterMetadata
	var lastErr error
	for _, addr := range addrs {
		c, err := NewClient(addr)
		if err != nil {
			lastErr = fmt.Errorf("clusterClient: dial %s: %w", addr, err)
			continue
		}
		meta, err = c.MetadataV1()
		c.Close()
		if err != nil {
			lastErr = fmt.Errorf("clusterClient: MetadataV1 from %s: %w", addr, err)
			continue
		}
		break
	}
	if meta == nil {
		return lastErr
	}

	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.meta = meta

	// Ensure we have a live connection to every broker listed in metadata.
	liveIDs := make(map[int32]struct{})
	for _, b := range meta.Brokers {
		liveIDs[b.ID] = struct{}{}
		existing, ok := cc.clients[b.ID]
		if ok && existing.IsAlive() {
			continue // reuse
		}
		if ok {
			existing.Close()
		}
		addr := fmt.Sprintf("%s:%d", b.Host, b.Port)
		c, err := NewClient(addr)
		if err != nil {
			// Non-fatal: the broker may be momentarily unavailable.
			continue
		}
		cc.clients[b.ID] = c
	}
	// Remove connections to brokers that are no longer in the metadata.
	for id, c := range cc.clients {
		if _, ok := liveIDs[id]; !ok {
			c.Close()
			delete(cc.clients, id)
		}
	}

	// Rebuild routing table.
	cc.leaderOf = make(map[string]int32)
	for _, t := range meta.Topics {
		for _, p := range t.Partitions {
			key := fmt.Sprintf("%s:%d", t.Name, p.ID)
			cc.leaderOf[key] = p.LeaderID
		}
	}
	return nil
}

// ClientFor returns the Client connected to the current leader of the given
// topic/partition. It refreshes metadata once on a cache miss.
func (cc *ClusterClient) ClientFor(topic string, partID int32) (*Client, error) {
	key := fmt.Sprintf("%s:%d", topic, partID)

	cc.mu.RLock()
	leaderID, ok := cc.leaderOf[key]
	client := cc.clients[leaderID]
	cc.mu.RUnlock()

	if ok && client != nil && client.IsAlive() {
		return client, nil
	}

	// Cache miss — refresh and try again.
	if err := cc.RefreshMetadata(); err != nil {
		return nil, err
	}

	cc.mu.RLock()
	leaderID, ok = cc.leaderOf[key]
	client = cc.clients[leaderID]
	cc.mu.RUnlock()

	if !ok || client == nil || !client.IsAlive() {
		return nil, fmt.Errorf("clusterClient: no leader found for %s/%d", topic, partID)
	}
	return client, nil
}

// Produce routes a produce request to the partition leader, retrying once on
// ErrNotLeader (which triggers a metadata refresh).
func (cc *ClusterClient) Produce(topic string, partID int32, key, value []byte) (int32, uint64, error) {
	for attempt := 0; attempt < 2; attempt++ {
		client, err := cc.ClientFor(topic, partID)
		if err != nil {
			return 0, 0, err
		}
		part, off, err := client.Produce(topic, partID, key, value)
		if isRetryableErr(err) {
			cc.RefreshMetadata() //nolint:errcheck
			continue
		}
		return part, off, err
	}
	return 0, 0, ErrNotLeader
}

// Fetch routes a fetch request to the partition leader, retrying once on
// ErrNotLeader or a connection error.
func (cc *ClusterClient) Fetch(topic string, partID int32, offset uint64, maxBytes, maxWaitMs uint32) ([]Record, error) {
	for attempt := 0; attempt < 2; attempt++ {
		client, err := cc.ClientFor(topic, partID)
		if err != nil {
			return nil, err
		}
		records, err := client.Fetch(topic, partID, offset, maxBytes, maxWaitMs)
		if isRetryableErr(err) {
			cc.RefreshMetadata() //nolint:errcheck
			continue
		}
		return records, err
	}
	return nil, ErrNotLeader
}

// isRetryableErr returns true for errors that warrant a metadata refresh and retry.
func isRetryableErr(err error) bool {
	if err == nil {
		return false
	}
	if err == ErrNotLeader {
		return true
	}
	msg := err.Error()
	return msg == "fluxmq: connection closed" ||
		msg == "fluxmq: connection closed while waiting for response"
}

// WaitForLeader polls until every partition of the topic has a reachable leader
// or the timeout elapses.
func (cc *ClusterClient) WaitForLeader(topic string, numParts int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := cc.RefreshMetadata(); err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		cc.mu.RLock()
		ready := 0
		for p := 0; p < numParts; p++ {
			key := fmt.Sprintf("%s:%d", topic, p)
			if lid, ok := cc.leaderOf[key]; ok {
				if c, cok := cc.clients[lid]; cok && c.IsAlive() {
					ready++
				}
			}
		}
		cc.mu.RUnlock()
		if ready == numParts {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("clusterClient: timed out waiting for leader of %s", topic)
}
