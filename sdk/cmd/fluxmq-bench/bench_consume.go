package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	fluxmq "fluxmq/sdk/fluxmq"

	"github.com/spf13/cobra"
)

var (
	consumeTopic    string
	consumeGroup    string
	consumeThreads  int
	consumeDuration time.Duration
	consumeNumMsgs  int64
)

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Benchmark consume throughput",
	Long: `Start multiple consumer group members and report aggregate throughput.
Runs until --duration elapses or --num-msgs total messages are consumed.`,
	RunE: runConsume,
}

func init() {
	consumeCmd.Flags().StringVar(&consumeTopic, "topic", "bench", "topic name")
	consumeCmd.Flags().StringVar(&consumeGroup, "group", "bench-group", "consumer group ID")
	consumeCmd.Flags().IntVar(&consumeThreads, "threads", 1, "number of concurrent consumers")
	consumeCmd.Flags().DurationVar(&consumeDuration, "duration", 30*time.Second, "how long to consume")
	consumeCmd.Flags().Int64Var(&consumeNumMsgs, "num-msgs", 0, "stop after this many messages (0 = use duration)")
}

func runConsume(cmd *cobra.Command, args []string) error {
	var (
		totalMsgs  int64
		totalBytes int64
	)

	done := make(chan struct{})
	var wg sync.WaitGroup

	// Close done channel after duration.
	go func() {
		time.Sleep(consumeDuration)
		close(done)
	}()

	start := time.Now()

	for i := 0; i < consumeThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := fluxmq.NewConsumer(brokerAddr, fluxmq.ConsumerConfig{
				GroupID:   consumeGroup,
				MaxWaitMs: 200,
				MaxBytes:  1 * 1024 * 1024,
			})
			if err != nil {
				return
			}
			defer c.Close()

			if err := c.Subscribe(consumeTopic); err != nil {
				return
			}

			for {
				select {
				case <-done:
					return
				case msg, ok := <-c.Messages():
					if !ok {
						return
					}
					n := atomic.AddInt64(&totalMsgs, 1)
					atomic.AddInt64(&totalBytes, int64(len(msg.Value)))
					if consumeNumMsgs > 0 && n >= consumeNumMsgs {
						close(done)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	dur := time.Since(start)

	result := BenchResult{
		Duration:      dur,
		TotalMessages: totalMsgs,
		TotalBytes:    totalBytes,
	}
	fmt.Printf("\n=== Consume Benchmark ===\n")
	fmt.Printf("Topic: %s  |  group: %s  |  threads: %d\n\n", consumeTopic, consumeGroup, consumeThreads)
	result.Print()
	return nil
}
