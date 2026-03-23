package fluxmq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ProducerConfig controls batching and delivery behavior.
type ProducerConfig struct {
	// BatchSize is the number of records accumulated before a forced flush.
	// Default: 100
	BatchSize int
	// LingerMs is the maximum wait before flushing a non-full batch.
	// Default: 5ms
	LingerMs time.Duration
	// MaxInFlight is the maximum number of concurrent in-flight PRODUCE requests.
	// Default: 5
	MaxInFlight int
	// Acks controls acknowledgement mode: 0 = fire-and-forget, 1 = wait for broker ack.
	// Default: 1
	Acks int
	// Idempotent enables exactly-once semantics per producer session.
	// When true, the producer allocates a unique ID from the broker and
	// attaches monotonic sequence numbers to each produce request.
	// The broker deduplicates retries so no message is written twice.
	// Requires Acks=1 (forced automatically when Idempotent is set).
	Idempotent bool
}

func (cfg *ProducerConfig) applyDefaults() {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.LingerMs <= 0 {
		cfg.LingerMs = 5 * time.Millisecond
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 5
	}
	if cfg.Idempotent {
		cfg.Acks = 1 // idempotent requires acks
	}
}

// SendResult carries the result of a single produce operation.
type SendResult struct {
	Partition int32
	Offset    uint64
	Err       error
}

// pendingRecord is a record waiting to be sent.
type pendingRecord struct {
	topic  string
	partID int32
	key    []byte
	val    []byte
	resCh  chan SendResult // nil means fire-and-forget
}

// Producer batches records and sends them asynchronously to a FluxMQ broker.
type Producer struct {
	client *Client
	cfg    ProducerConfig

	// clusterSend, if non-nil, is used instead of client.Produce (cluster mode).
	clusterSend func(topic string, partID int32, key, value []byte) (int32, uint64, error)
	// closeFn, if non-nil, is called instead of client.Close (cluster mode).
	closeFn func() error

	// Idempotent producer state.
	producerID    uint64
	producerEpoch uint16
	seqNums       sync.Map // partition (int32) → *atomic.Int32

	mu      sync.Mutex
	pending []pendingRecord
	timer   *time.Timer

	flushCh   chan []pendingRecord
	doneCh    chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// NewProducer creates a Producer connected to the broker at addr.
func NewProducer(addr string, cfg ProducerConfig) (*Producer, error) {
	cfg.applyDefaults()
	client, err := NewClient(addr)
	if err != nil {
		return nil, err
	}
	p := &Producer{
		client:  client,
		cfg:     cfg,
		flushCh: make(chan []pendingRecord, 64),
		doneCh:  make(chan struct{}),
	}
	if cfg.Idempotent {
		pid, epoch, err := client.InitProducerId()
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("fluxmq: init producer id: %w", err)
		}
		p.producerID = pid
		p.producerEpoch = epoch
	}
	p.wg.Add(1)
	go p.flushWorker()
	return p, nil
}

// NewClusterProducer creates a Producer that routes each record to the correct
// partition leader via a ClusterClient.  seedAddr is any broker in the cluster.
func NewClusterProducer(seedAddr string, cfg ProducerConfig) (*Producer, error) {
	cfg.applyDefaults()
	cc, err := NewClusterClient(seedAddr)
	if err != nil {
		return nil, err
	}
	p := &Producer{
		cfg:         cfg,
		clusterSend: cc.Produce,
		closeFn:     func() error { cc.Close(); return nil },
		flushCh:     make(chan []pendingRecord, 64),
		doneCh:      make(chan struct{}),
	}
	p.wg.Add(1)
	go p.flushWorker()
	return p, nil
}

// Send enqueues a record for async delivery. The record is assigned to partition -1
// (auto). Returns after the record is queued (not necessarily delivered).
func (p *Producer) Send(topic string, key, value []byte) error {
	select {
	case <-p.doneCh:
		return fmt.Errorf("fluxmq: producer is closed")
	default:
	}
	var resCh chan SendResult
	if p.cfg.Acks == 1 {
		resCh = make(chan SendResult, 1)
	}
	rec := pendingRecord{
		topic:  topic,
		partID: -1,
		key:    key,
		val:    value,
		resCh:  resCh,
	}
	p.accumulate(rec)
	return nil
}

// SendSync sends a single record synchronously, bypassing the batcher.
func (p *Producer) SendSync(topic string, partID int32, key, value []byte) (int32, uint64, error) {
	if p.cfg.Idempotent && p.client != nil {
		seq := p.nextSeqNum(partID)
		return p.client.ProduceIdempotent(topic, partID, key, value,
			p.producerID, p.producerEpoch, seq)
	}
	if p.clusterSend != nil {
		return p.clusterSend(topic, partID, key, value)
	}
	return p.client.Produce(topic, partID, key, value)
}

// nextSeqNum returns and increments the sequence number for a partition.
func (p *Producer) nextSeqNum(partID int32) int32 {
	val, _ := p.seqNums.LoadOrStore(partID, &atomic.Int32{})
	counter := val.(*atomic.Int32)
	return counter.Add(1) - 1 // first call returns 0
}

// Flush forces any pending batch to be sent and waits for all in-flight acks
// (when Acks==1).
func (p *Producer) Flush() error {
	// Drain the pending buffer by triggering a flush.
	p.mu.Lock()
	batch := p.drainLocked()
	p.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	// Collect all result channels.
	var resultChs []chan SendResult
	for _, rec := range batch {
		if rec.resCh != nil {
			resultChs = append(resultChs, rec.resCh)
		}
	}

	// Send the batch.
	select {
	case p.flushCh <- batch:
	case <-p.doneCh:
		return fmt.Errorf("fluxmq: producer is closed")
	}

	// Wait for all acks.
	var lastErr error
	for _, ch := range resultChs {
		res := <-ch
		if res.Err != nil {
			lastErr = res.Err
		}
	}
	return lastErr
}

// Close flushes any pending records and shuts down the producer.
func (p *Producer) Close() error {
	p.closeOnce.Do(func() {
		// Flush remaining records.
		p.mu.Lock()
		batch := p.drainLocked()
		p.mu.Unlock()

		if len(batch) > 0 {
			select {
			case p.flushCh <- batch:
			default:
			}
		}

		close(p.doneCh)
		p.wg.Wait()
		if p.closeFn != nil {
			p.closeFn()
		} else if p.client != nil {
			p.client.Close()
		}
	})
	return p.closeErr
}

// accumulate appends rec to the pending buffer under lock and triggers a flush
// if the batch is full, or arms the linger timer otherwise.
func (p *Producer) accumulate(rec pendingRecord) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pending = append(p.pending, rec)

	if len(p.pending) >= p.cfg.BatchSize {
		batch := p.drainLocked()
		select {
		case p.flushCh <- batch:
		default:
			// flushCh full; put batch back (best-effort).
			p.pending = append(batch, p.pending...)
		}
		return
	}

	// Arm the linger timer if not already running.
	if p.timer == nil {
		p.timer = time.AfterFunc(p.cfg.LingerMs, func() {
			p.mu.Lock()
			batch := p.drainLocked()
			p.mu.Unlock()
			if len(batch) > 0 {
				select {
				case p.flushCh <- batch:
				case <-p.doneCh:
				}
			}
		})
	}
}

// drainLocked returns the current pending batch and resets the buffer.
// Must be called with p.mu held.
func (p *Producer) drainLocked() []pendingRecord {
	if len(p.pending) == 0 {
		return nil
	}
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	batch := p.pending
	p.pending = nil
	return batch
}

// flushWorker reads batches from flushCh and sends each record concurrently,
// up to MaxInFlight at a time.
func (p *Producer) flushWorker() {
	defer p.wg.Done()
	for {
		select {
		case batch := <-p.flushCh:
			p.sendBatch(batch)
		case <-p.doneCh:
			// Drain any remaining batches.
			for {
				select {
				case batch := <-p.flushCh:
					p.sendBatch(batch)
				default:
					return
				}
			}
		}
	}
}

// sendBatch sends all records in the batch concurrently, respecting MaxInFlight.
func (p *Producer) sendBatch(batch []pendingRecord) {
	if len(batch) == 0 {
		return
	}

	sem := make(chan struct{}, p.cfg.MaxInFlight)
	var wg sync.WaitGroup

	for _, rec := range batch {
		rec := rec // capture loop variable
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			var part int32
			var off uint64
			var err error
			if p.cfg.Idempotent && p.client != nil {
				seq := p.nextSeqNum(rec.partID)
				part, off, err = p.client.ProduceIdempotent(rec.topic, rec.partID, rec.key, rec.val,
					p.producerID, p.producerEpoch, seq)
			} else if p.clusterSend != nil {
				part, off, err = p.clusterSend(rec.topic, rec.partID, rec.key, rec.val)
			} else {
				part, off, err = p.client.Produce(rec.topic, rec.partID, rec.key, rec.val)
			}
			if rec.resCh != nil {
				rec.resCh <- SendResult{Partition: part, Offset: off, Err: err}
			}
		}()
	}

	wg.Wait()
}
