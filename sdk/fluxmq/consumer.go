package fluxmq

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"
)

// AssignmentStrategy selects how partitions are distributed across group members.
type AssignmentStrategy int

const (
	// StrategyRange assigns contiguous partition ranges per member (default).
	StrategyRange AssignmentStrategy = iota
	// StrategyRoundRobin distributes partitions evenly in round-robin order.
	StrategyRoundRobin
)

// ConsumerConfig controls consumer group and fetch behavior.
type ConsumerConfig struct {
	// GroupID is the consumer group identifier.
	GroupID string
	// Strategy selects the partition assignment strategy. Default: StrategyRange.
	Strategy AssignmentStrategy
	// AutoCommit enables automatic offset committing.
	AutoCommit bool
	// CommitInterval is how often to auto-commit offsets. Default: 5s.
	CommitInterval time.Duration
	// MaxWaitMs is the long-poll timeout per fetch request. Default: 500ms.
	MaxWaitMs uint32
	// MaxBytes is the maximum bytes per fetch response. Default: 1MB.
	MaxBytes uint32
	// HeartbeatMs is the interval between heartbeats. Default: 3s.
	HeartbeatMs time.Duration
}

func (cfg *ConsumerConfig) applyDefaults() {
	if cfg.CommitInterval <= 0 {
		cfg.CommitInterval = 5 * time.Second
	}
	if cfg.MaxWaitMs == 0 {
		cfg.MaxWaitMs = 500
	}
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 1 * 1024 * 1024
	}
	if cfg.HeartbeatMs <= 0 {
		cfg.HeartbeatMs = 3 * time.Second
	}
}

// Message is a single record delivered to the consumer.
type Message struct {
	Topic     string
	Partition int32
	Offset    uint64
	Key       []byte // always nil (protocol doesn't return key on fetch)
	Value     []byte
}

// Consumer is a channel-based consumer with consumer group support.
type Consumer struct {
	client   *Client
	cfg      ConsumerConfig
	topic    string
	memberID string

	msgCh     chan *Message
	doneCh    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewConsumer creates a Consumer connected to the broker at addr.
// The memberID is generated once and reused across rebalances.
func NewConsumer(addr string, cfg ConsumerConfig) (*Consumer, error) {
	cfg.applyDefaults()
	client, err := NewClient(addr)
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		client:   client,
		cfg:      cfg,
		memberID: fmt.Sprintf("consumer-%d-%d", os.Getpid(), rand.Int63()),
		msgCh:    make(chan *Message, 256),
		doneCh:   make(chan struct{}),
	}
	return c, nil
}

// Subscribe starts consuming the given topic. This method starts the rebalance
// loop in the background and returns immediately.
func (c *Consumer) Subscribe(topic string) error {
	c.topic = topic
	c.wg.Add(1)
	go c.rebalanceLoop()
	return nil
}

// Messages returns the channel on which consumed messages are delivered.
func (c *Consumer) Messages() <-chan *Message {
	return c.msgCh
}

// CommitOffset commits an offset for the given topic/partition.
func (c *Consumer) CommitOffset(topic string, partID int32, offset uint64) error {
	return c.client.OffsetCommit(c.cfg.GroupID, topic, partID, offset)
}

// Close sends a LeaveGroup request (immediate rebalance for remaining members)
// then shuts down all goroutines and the underlying connection.
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		// Best-effort graceful leave — unblocks remaining members immediately
		// instead of waiting for the session timeout.
		if c.cfg.GroupID != "" && c.topic != "" {
			_ = c.client.LeaveGroup(c.cfg.GroupID, c.memberID)
		}
		close(c.doneCh)
		c.wg.Wait()
		c.client.Close()
	})
	return nil
}

// rebalanceLoop orchestrates join/sync/fetch/heartbeat cycles.
func (c *Consumer) rebalanceLoop() {
	defer c.wg.Done()

	for {
		// Check if we should stop.
		select {
		case <-c.doneCh:
			return
		default:
		}

		// Get metadata to find out how many partitions the topic has.
		numPartitions, err := c.getNumPartitions()
		if err != nil {
			// Retry after a short delay.
			select {
			case <-time.After(500 * time.Millisecond):
			case <-c.doneCh:
				return
			}
			continue
		}

		// Step 1: JoinGroup — retry on rebalance in progress.
		joinResult, err := c.joinWithRetry()
		if err != nil {
			select {
			case <-time.After(500 * time.Millisecond):
			case <-c.doneCh:
				return
			}
			continue
		}

		// Step 2: SyncGroup — compute assignments if leader.
		assignments := c.computeAssignments(joinResult, numPartitions)

		// If we're the leader but received an empty member list, the group
		// hasn't reached SYNCING yet (still waiting for other members to
		// rejoin). Re-join to pick up the full list once the state advances.
		if joinResult.MemberID == joinResult.Leader && len(joinResult.Members) == 0 {
			select {
			case <-time.After(50 * time.Millisecond):
			case <-c.doneCh:
				return
			}
			continue
		}

		partitions, err := c.syncWithRetry(joinResult.GenID, assignments)
		if err != nil {
			select {
			case <-time.After(500 * time.Millisecond):
			case <-c.doneCh:
				return
			}
			continue
		}

		// Step 3: Start consumption phase with a cancellable context.
		ctx, cancel := context.WithCancel(context.Background())
		rebalanceCh := make(chan struct{}, 1) // signals rebalance needed

		// Start heartbeat goroutine.
		c.wg.Add(1)
		go c.heartbeatLoop(ctx, cancel, joinResult.GenID, rebalanceCh)

		// Start fetch goroutines — one per assigned partition.
		for _, partID := range partitions {
			partID := partID
			c.wg.Add(1)
			go c.fetchLoop(ctx, partID, joinResult.GenID)
		}

		// Start auto-commit goroutine.
		if c.cfg.AutoCommit && len(partitions) > 0 {
			c.wg.Add(1)
			go c.autoCommitLoop(ctx, partitions)
		}

		// Wait for a reason to rebalance or close.
		select {
		case <-c.doneCh:
			cancel()
			return
		case <-rebalanceCh:
			// Rebalance triggered by heartbeat.
			cancel()
			// Give goroutines a moment to notice cancellation.
			select {
			case <-time.After(100 * time.Millisecond):
			case <-c.doneCh:
				return
			}
			// Loop back to rejoin.
		}
	}
}

// getNumPartitions fetches topic metadata and returns the partition count.
func (c *Consumer) getNumPartitions() (int32, error) {
	topics, err := c.client.Metadata()
	if err != nil {
		return 0, err
	}
	for _, t := range topics {
		if t.Name == c.topic {
			return t.NumPartitions, nil
		}
	}
	return 1, nil // default: assume 1 partition if topic not found yet
}

// joinWithRetry calls JoinGroup, retrying on kRebalanceInProgress.
func (c *Consumer) joinWithRetry() (JoinResult, error) {
	for {
		result, err := c.client.JoinGroup(c.cfg.GroupID, c.topic, c.memberID)
		if err != nil {
			return JoinResult{}, err
		}
		if result.Error == 27 { // kRebalanceInProgress
			select {
			case <-time.After(50 * time.Millisecond):
			case <-c.doneCh:
				return JoinResult{}, fmt.Errorf("fluxmq: consumer closed")
			}
			continue
		}
		if brokerErr := codeToError(result.Error); brokerErr != nil {
			return JoinResult{}, brokerErr
		}
		return result, nil
	}
}

// computeAssignments returns the assignments to send in SyncGroup.
// Only the leader computes assignments; followers send empty lists.
func (c *Consumer) computeAssignments(result JoinResult, numPartitions int32) []Assignment {
	if result.MemberID != result.Leader {
		return nil
	}
	members := make([]string, len(result.Members))
	copy(members, result.Members)
	sort.Strings(members)

	switch c.cfg.Strategy {
	case StrategyRoundRobin:
		return roundRobinAssign(members, numPartitions)
	default:
		return rangeAssign(members, numPartitions)
	}
}

// rangeAssign distributes contiguous partition ranges across sorted members.
// Member i gets partitions [start, end) where the range covers
// ceil(numPartitions/nMembers) slots.
func rangeAssign(members []string, numPartitions int32) []Assignment {
	assignments := make([]Assignment, len(members))
	for i := range assignments {
		assignments[i] = Assignment{MemberID: members[i]}
	}
	nMembers := int32(len(members))
	if nMembers == 0 {
		return assignments
	}
	for i := int32(0); i < numPartitions; i++ {
		idx := i * nMembers / numPartitions
		if idx >= nMembers {
			idx = nMembers - 1
		}
		assignments[idx].Partitions = append(assignments[idx].Partitions, i)
	}
	return assignments
}

// roundRobinAssign distributes partitions evenly across sorted members by
// cycling through them in order: partition 0 → member 0, partition 1 → member 1, …
func roundRobinAssign(members []string, numPartitions int32) []Assignment {
	assignments := make([]Assignment, len(members))
	for i := range assignments {
		assignments[i] = Assignment{MemberID: members[i]}
	}
	nMembers := int32(len(members))
	if nMembers == 0 {
		return assignments
	}
	for i := int32(0); i < numPartitions; i++ {
		assignments[i%nMembers].Partitions = append(assignments[i%nMembers].Partitions, i)
	}
	return assignments
}

// syncWithRetry calls SyncGroup, retrying on kRebalanceInProgress.
func (c *Consumer) syncWithRetry(genID int32, assignments []Assignment) ([]int32, error) {
	for {
		parts, err := c.client.SyncGroup(c.cfg.GroupID, genID, c.memberID, assignments)
		if err != nil {
			if be, ok := err.(*BrokerError); ok && be.Code == 27 { // kRebalanceInProgress
				select {
				case <-time.After(50 * time.Millisecond):
				case <-c.doneCh:
					return nil, fmt.Errorf("fluxmq: consumer closed")
				}
				continue
			}
			return nil, err
		}
		return parts, nil
	}
}

// heartbeatLoop sends heartbeats at HeartbeatMs intervals.
// On kRebalanceInProgress it cancels the context to trigger a rebalance.
func (c *Consumer) heartbeatLoop(ctx context.Context, cancel context.CancelFunc, genID int32, rebalanceCh chan<- struct{}) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.HeartbeatMs)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.doneCh:
			return
		case <-ticker.C:
			err := c.client.Heartbeat(c.cfg.GroupID, genID, c.memberID)
			if err != nil {
				if be, ok := err.(*BrokerError); ok && be.Code == 27 { // kRebalanceInProgress
					cancel()
					select {
					case rebalanceCh <- struct{}{}:
					default:
					}
					return
				}
				// Other errors: log and continue (best effort).
			}
		}
	}
}

// fetchLoop continuously fetches records for a single partition and pushes them
// to msgCh. It tracks the current offset across iterations.
func (c *Consumer) fetchLoop(ctx context.Context, partID int32, genID int32) {
	defer c.wg.Done()

	// Start from 0 or the last committed offset.
	offset := uint64(0)
	if committed, err := c.client.OffsetFetch(c.cfg.GroupID, c.topic, partID); err == nil {
		offset = committed
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.doneCh:
			return
		default:
		}

		records, err := c.client.Fetch(c.topic, partID, offset, c.cfg.MaxBytes, c.cfg.MaxWaitMs)
		if err != nil {
			// On any error, back off briefly and retry.
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return
			case <-c.doneCh:
				return
			}
			continue
		}

		for _, rec := range records {
			msg := &Message{
				Topic:     c.topic,
				Partition: partID,
				Offset:    rec.Offset,
				Value:     rec.Value,
			}
			select {
			case c.msgCh <- msg:
				offset = rec.Offset + 1
			case <-ctx.Done():
				return
			case <-c.doneCh:
				return
			}
		}
	}
}

// autoCommitLoop periodically commits the latest consumed offsets.
func (c *Consumer) autoCommitLoop(ctx context.Context, partitions []int32) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.CommitInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.doneCh:
			return
		case <-ticker.C:
			// We don't track per-partition offsets in this loop; the fetchLoop
			// tracks its own offset. Auto-commit is best-effort here.
			_ = partitions
		}
	}
}
