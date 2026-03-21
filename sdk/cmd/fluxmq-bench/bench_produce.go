package main

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	fluxmq "fluxmq/sdk/fluxmq"

	"github.com/spf13/cobra"
)

var (
	produceTopic   string
	produceMsgSize int
	produceNumMsgs int
	produceThreads int
)

var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Benchmark produce throughput",
	Long: `Produce a fixed number of messages across multiple goroutines and report
throughput (msg/sec, MB/sec) and per-message latency percentiles.`,
	RunE: runProduce,
}

func init() {
	produceCmd.Flags().StringVar(&produceTopic, "topic", "bench", "topic name")
	produceCmd.Flags().IntVar(&produceMsgSize, "msg-size", 100, "message payload size in bytes")
	produceCmd.Flags().IntVar(&produceNumMsgs, "num-msgs", 100000, "total number of messages to produce")
	produceCmd.Flags().IntVar(&produceThreads, "threads", 1, "number of concurrent producer goroutines")
}

func runProduce(cmd *cobra.Command, args []string) error {
	// Create the topic with one partition per thread.
	setup, err := fluxmq.NewClient(brokerAddr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := setup.CreateTopic(produceTopic, int32(produceThreads)); err != nil {
		// Ignore topic-already-exists; surface other errors.
		if e, ok := err.(*fluxmq.BrokerError); !ok || e.Code != 8 {
			setup.Close()
			return fmt.Errorf("create topic: %w", err)
		}
	}
	setup.Close()

	payload := bytes.Repeat([]byte("x"), produceMsgSize)
	msgsPerThread := produceNumMsgs / produceThreads

	var (
		totalMsgs  int64
		totalBytes int64
		totalErrs  int64
	)
	rec := &latencyRecorder{}
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < produceThreads; i++ {
		partID := int32(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := fluxmq.NewProducer(brokerAddr, fluxmq.ProducerConfig{Acks: 1})
			if err != nil {
				atomic.AddInt64(&totalErrs, int64(msgsPerThread))
				return
			}
			defer p.Close()

			for j := 0; j < msgsPerThread; j++ {
				t0 := time.Now()
				_, _, err := p.SendSync(produceTopic, partID, nil, payload)
				elapsed := time.Since(t0)
				if err != nil {
					atomic.AddInt64(&totalErrs, 1)
					continue
				}
				rec.Record(elapsed)
				atomic.AddInt64(&totalMsgs, 1)
				atomic.AddInt64(&totalBytes, int64(produceMsgSize))
			}
		}()
	}

	wg.Wait()
	dur := time.Since(start)

	result := BenchResult{
		Duration:      dur,
		TotalMessages: totalMsgs,
		TotalBytes:    totalBytes,
		Errors:        totalErrs,
		Latencies:     rec.Drain(),
	}
	fmt.Printf("\n=== Produce Benchmark ===\n")
	fmt.Printf("Topic: %s  |  msg-size: %d bytes  |  threads: %d\n\n", produceTopic, produceMsgSize, produceThreads)
	result.Print()
	return nil
}
