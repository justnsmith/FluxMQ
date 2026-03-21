#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

// Per-partition gauge state.  Heap-allocated so the containing unordered_map
// can rehash without moving the atomic members.
struct PartitionGauges
{
    std::atomic<uint64_t> log_end_offset{0};
    std::atomic<uint64_t> high_watermark{0};
};

// MetricsRegistry holds all broker-level counters, gauges, and histograms.
//
// Thread-safety model:
//   - Global counters (bytes_in_total, …) are plain atomics; update lock-free.
//   - Per-partition / per-group maps are protected by shared_mutexes.
//     Map entries are created once (at topic-creation time) and never deleted,
//     so a raw pointer obtained under a shared lock stays valid indefinitely.
//     Hot-path updates only need a shared lock (read) on the map, then an
//     atomic store on the entry value.
class MetricsRegistry
{
  public:
    // ── Global counters ──────────────────────────────────────────────────────
    std::atomic<uint64_t> bytes_in_total{0};
    std::atomic<uint64_t> bytes_out_total{0};
    std::atomic<uint64_t> messages_in_total{0};
    std::atomic<uint64_t> messages_out_total{0};

    // ── Per-partition gauges ─────────────────────────────────────────────────
    // Key: "topic:partition"
    mutable std::shared_mutex part_mu_;
    std::unordered_map<std::string, std::unique_ptr<PartitionGauges>> partitions_;

    // ── Consumer group lag ───────────────────────────────────────────────────
    // Key: "group\0topic:partition"  (null byte separates group from the rest)
    mutable std::shared_mutex lag_mu_;
    std::unordered_map<std::string, std::unique_ptr<std::atomic<uint64_t>>> consumer_lags_;

    // ── Replication lag per partition (milliseconds) ─────────────────────────
    // Key: "topic:partition"
    mutable std::shared_mutex repl_mu_;
    std::unordered_map<std::string, std::unique_ptr<std::atomic<uint64_t>>> replication_lags_;

    // ── Fetch latency histogram (milliseconds) ───────────────────────────────
    // Buckets (cumulative): 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000 ms + +Inf
    static constexpr std::array<uint64_t, 10> kBuckets = {1, 2, 5, 10, 25, 50, 100, 250, 500, 1000};
    std::array<std::atomic<uint64_t>, 11> fetch_latency_buckets_{};
    std::atomic<uint64_t> fetch_latency_sum_ms_{0};
    std::atomic<uint64_t> fetch_latency_count_{0};

    // ── Public API ───────────────────────────────────────────────────────────

    // Render all metrics in Prometheus text exposition format (0.0.4).
    std::string Render() const;

    // Record one fetch latency sample (updates all cumulative buckets >= ms).
    void ObserveFetchLatency(uint64_t ms);

    // Ensure map entries exist — idempotent, called at topic-creation time.
    void EnsurePartition(const std::string &topic, int32_t partition);
    void EnsureConsumerLag(const std::string &group, const std::string &topic, int32_t partition);
    void EnsureReplicationLag(const std::string &topic, int32_t partition);

    // Update per-partition gauges (cheap: shared lock + atomic store).
    void SetPartitionLEO(const std::string &topic, int32_t partition, uint64_t leo);
    void SetPartitionHWM(const std::string &topic, int32_t partition, uint64_t hwm);
    void SetConsumerLag(const std::string &group, const std::string &topic, int32_t partition, uint64_t lag);
    void SetReplicationLag(const std::string &topic, int32_t partition, uint64_t lag_ms);
};
