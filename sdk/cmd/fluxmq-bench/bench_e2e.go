package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	fluxmq "fluxmq/sdk/fluxmq"

	"github.com/spf13/cobra"
)

var (
	e2eTopic    string
	e2eMsgSize  int
	e2eDuration time.Duration
)

var e2eCmd = &cobra.Command{
	Use:   "e2e",
	Short: "Benchmark end-to-end produce-to-consume latency",
	Long: `Produces messages with an embedded timestamp and measures the time until
each message is consumed.  Both producer and consumer run on the same machine.
Requires synchronized clocks only within the local process (time.Now()).`,
	RunE: runE2E,
}

func init() {
	e2eCmd.Flags().StringVar(&e2eTopic, "topic", "bench-e2e", "topic name")
	e2eCmd.Flags().IntVar(&e2eMsgSize, "msg-size", 100, "message payload size in bytes (excluding 8B timestamp header)")
	e2eCmd.Flags().DurationVar(&e2eDuration, "duration", 10*time.Second, "how long to run")
}

// buildE2EPayload returns an 8-byte big-endian unix-nanosecond timestamp
// followed by a fixed-size payload.
func buildE2EPayload(extra int) []byte {
	buf := make([]byte, 8+extra)
	binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UnixNano()))
	copy(buf[8:], bytes.Repeat([]byte("x"), extra))
	return buf
}

func runE2E(cmd *cobra.Command, args []string) error {
	// Unique group so we always start from offset 0 and don't reuse stale offsets.
	group := fmt.Sprintf("bench-e2e-%d", time.Now().UnixNano())

	// Create topic (1 partition).
	setup, err := fluxmq.NewClient(brokerAddr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := setup.CreateTopic(e2eTopic, 1); err != nil {
		if e, ok := err.(*fluxmq.BrokerError); !ok || e.Code != 36 {
			setup.Close()
			return fmt.Errorf("create topic: %w", err)
		}
	}
	setup.Close()

	rec := &latencyRecorder{}
	var totalMsgs, totalBytes, totalErrs int64

	consumerReady := make(chan struct{})
	stopCh := make(chan struct{})

	// Consumer goroutine.
	go func() {
		c, err := fluxmq.NewConsumer(brokerAddr, fluxmq.ConsumerConfig{
			GroupID:   group,
			MaxWaitMs: 100,
			MaxBytes:  1 * 1024 * 1024,
		})
		if err != nil {
			close(consumerReady)
			return
		}
		defer c.Close()

		if err := c.Subscribe(e2eTopic); err != nil {
			close(consumerReady)
			return
		}

		close(consumerReady) // signal producer to start

		for {
			select {
			case <-stopCh:
				return
			case msg, ok := <-c.Messages():
				if !ok {
					return
				}
				if len(msg.Value) < 8 {
					continue
				}
				sentNs := int64(binary.BigEndian.Uint64(msg.Value[:8]))
				latency := time.Duration(time.Now().UnixNano() - sentNs)
				rec.Record(latency)
				atomic.AddInt64(&totalMsgs, 1)
				atomic.AddInt64(&totalBytes, int64(len(msg.Value)))
			}
		}
	}()

	<-consumerReady
	// Brief pause so the consumer completes its JoinGroup/SyncGroup cycle.
	time.Sleep(300 * time.Millisecond)

	// Producer goroutine.
	deadline := time.Now().Add(e2eDuration)
	p, err := fluxmq.NewProducer(brokerAddr, fluxmq.ProducerConfig{Acks: 1})
	if err != nil {
		return fmt.Errorf("producer: %w", err)
	}

	start := time.Now()
	for time.Now().Before(deadline) {
		payload := buildE2EPayload(e2eMsgSize)
		if _, _, err := p.SendSync(e2eTopic, 0, nil, payload); err != nil {
			atomic.AddInt64(&totalErrs, 1)
		}
	}
	p.Close()

	// Give the consumer time to drain in-flight messages.
	time.Sleep(500 * time.Millisecond)
	close(stopCh)

	dur := time.Since(start)
	result := BenchResult{
		Duration:      dur,
		TotalMessages: totalMsgs,
		TotalBytes:    totalBytes,
		Errors:        totalErrs,
		Latencies:     rec.Drain(),
	}
	fmt.Printf("\n=== End-to-End Benchmark ===\n")
	fmt.Printf("Topic: %s  |  msg-size: %d bytes  |  duration: %s\n\n", e2eTopic, e2eMsgSize, e2eDuration)
	result.Print()
	return nil
}
