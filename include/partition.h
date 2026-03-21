#pragma once

#include "log.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// A single record returned by Partition::ReadBatch.
struct Record
{
    uint64_t offset{0};
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

    // Append value bytes; returns the assigned logical offset.
    uint64_t Append(const uint8_t *data, size_t len);

    // Read records starting at fetch_offset, accumulating up to max_bytes of payload.
    // Always returns at least one record if fetch_offset < NextOffset().
    std::vector<Record> ReadBatch(uint64_t fetch_offset, uint32_t max_bytes) const;

    // Block until NextOffset() > fetch_offset OR the deadline passes.
    // Returns true if data became available before the deadline.
    bool WaitForData(uint64_t fetch_offset, std::chrono::steady_clock::time_point deadline);

    uint64_t NextOffset() const;

    // Consumer group offset management.
    void CommitOffset(std::string_view group, uint64_t offset);
    uint64_t FetchCommittedOffset(std::string_view group) const;

    int Id() const
    {
        return id_;
    }

  private:
    void LoadOffsets();

    int id_;
    Log log_;
    std::filesystem::path offsets_dir_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::unordered_map<std::string, uint64_t> offsets_;
};
