#pragma once

#include "partition.h"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

// Topic owns N Partition objects and routes produce requests.
//
// Key-based routing: murmur3(key) % num_partitions.
// Keyless routing: round-robin (atomic counter).
class Topic
{
  public:
    Topic(std::string name, int num_partitions, const std::filesystem::path &base_dir, uint64_t max_seg_bytes = kDefaultMaxSegmentBytes);

    // Append payload to the partition chosen by key (or round-robin when key is empty).
    // Returns {partition_id, assigned_offset}.
    std::pair<int, uint64_t> Publish(const uint8_t *value, size_t value_len, const uint8_t *key = nullptr, size_t key_len = 0);

    Partition &GetPartition(int id);
    const Partition &GetPartition(int id) const;

    int NumPartitions() const
    {
        return static_cast<int>(partitions_.size());
    }
    const std::string &Name() const
    {
        return name_;
    }

  private:
    std::string name_;
    std::vector<std::unique_ptr<Partition>> partitions_;
    std::atomic<int> rr_counter_{0};
};
