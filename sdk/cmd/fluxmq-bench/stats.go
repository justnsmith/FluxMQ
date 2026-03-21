package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// latencyRecorder collects per-observation latency samples in microseconds.
// Samples are merged and sorted only at report time, so the hot path is
// just a mutex-protected append.
type latencyRecorder struct {
	mu      sync.Mutex
	samples []int64
}

func (r *latencyRecorder) Record(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d.Microseconds())
	r.mu.Unlock()
}

func (r *latencyRecorder) Drain() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.samples
	r.samples = nil
	return s
}

// BenchResult holds the aggregated outcome of one benchmark run.
type BenchResult struct {
	Duration      time.Duration
	TotalMessages int64
	TotalBytes    int64
	Errors        int64
	Latencies     []int64 // microseconds, sorted ascending
}

func (r *BenchResult) Print() {
	secs := r.Duration.Seconds()
	msgPerSec := float64(r.TotalMessages) / secs
	mbPerSec := float64(r.TotalBytes) / secs / (1024 * 1024)

	fmt.Printf("Duration:   %s\n", r.Duration.Round(time.Millisecond))
	fmt.Printf("Messages:   %d  (errors: %d)\n", r.TotalMessages, r.Errors)
	fmt.Printf("Throughput: %.0f msg/sec  |  %.2f MB/sec\n", msgPerSec, mbPerSec)

	if len(r.Latencies) > 0 {
		sort.Slice(r.Latencies, func(i, j int) bool { return r.Latencies[i] < r.Latencies[j] })
		fmt.Printf("Latency:    p50=%.2fms  p95=%.2fms  p99=%.2fms  max=%.2fms\n",
			pctMs(r.Latencies, 50),
			pctMs(r.Latencies, 95),
			pctMs(r.Latencies, 99),
			float64(r.Latencies[len(r.Latencies)-1])/1000.0,
		)
	}
}

// pctMs returns the p-th percentile of sorted microsecond samples, in milliseconds.
func pctMs(sorted []int64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted)-1)*p/100
	return float64(sorted[idx]) / 1000.0
}
