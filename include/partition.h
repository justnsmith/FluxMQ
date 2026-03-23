#pragma once

#include "log.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// A single record returned by Partition::ReadBatch.
struct Record
{
    uint64_t offset{0};
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

// Partition wraps a Log and adds:
//   - Consumer group committed offset storage (one file per group under consumer_offsets/).
//   - A condition_variable so FETCH can long-poll for new data without busy-waiting.
//
// All public methods are thread-safe.  The Log itself is protected by mu_.
class Partition
{
  public:
    Partition(const std::filesystem::path &dir, int id, uint64_t max_seg_bytes = kDefaultMaxSegmentBytes);

    // Append value bytes (no key); returns the assigned logical offset.
    uint64_t Append(const uint8_t *data, size_t len);

    // Append a key-value record; returns the assigned logical offset.
    // The key is stored alongside the value for key-based log compaction.
    uint64_t AppendKV(const uint8_t *key, size_t key_len, const uint8_t *value, size_t value_len);

    // Run key-based log compaction on closed (non-active) segments.
    // For each key, only the record with the highest offset is retained.
    // Records with empty keys are always preserved (cannot be compacted).
    // Returns the number of records removed.
    size_t Compact();

    // Consumer-facing read: capped at HighWatermark() so consumers never see
    // data that hasn't been committed to all ISR members yet.
    // In standalone mode HighWatermark() == NextOffset() so behaviour is unchanged.
    std::vector<Record> ReadBatch(uint64_t fetch_offset, uint32_t max_bytes) const;

    // Replication-facing read: reads up to NextOffset() ignoring the HWM.
    // Used by HandleReplicaFetch to send un-committed data to followers.
    std::vector<Record> ReadBatchForReplication(uint64_t fetch_offset, uint32_t max_bytes) const;

    // Block until HighWatermark() > fetch_offset OR the deadline passes.
    bool WaitForData(uint64_t fetch_offset, std::chrono::steady_clock::time_point deadline);

    uint64_t NextOffset() const;

    // High-watermark — the highest offset that has been replicated to all
    // current ISR members (and therefore safe to serve to consumers).
    // Sentinel kUnreplicatedHWM means standalone mode: HighWatermark()==NextOffset().
    static constexpr uint64_t kUnreplicatedHWM = std::numeric_limits<uint64_t>::max();

    uint64_t HighWatermark() const;
    void SetHighWatermark(uint64_t hwm); // called by ReplicationManager

    // Consumer group offset management.
    void CommitOffset(std::string_view group, uint64_t offset);
    uint64_t FetchCommittedOffset(std::string_view group) const;

    int Id() const
    {
        return id_;
    }

  private:
    void LoadOffsets();
    std::vector<Record> ReadBatchUpTo(uint64_t fetch_offset, uint32_t max_bytes, uint64_t limit) const;

    int id_;
    Log log_;
    std::filesystem::path offsets_dir_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::unordered_map<std::string, uint64_t> offsets_;

    std::atomic<uint64_t> hwm_{kUnreplicatedHWM};
};
