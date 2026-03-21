#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <vector>

// Sparse index granularity: one index entry per this many bytes of log data.
inline constexpr uint32_t kIndexIntervalBytes = 4096;

// On-disk .log record layout:
//   [4B payload_length][4B crc32][payload_length bytes]
//
// On-disk .index entry layout (sparse, one entry per ~kIndexIntervalBytes):
//   [8B logical_offset][4B byte_position_in_log]
//
// Segment file names are the base logical offset zero-padded to 20 digits:
//   00000000000000000000.log
//   00000000000000000000.index

struct IndexEntry
{
    uint64_t offset;   // logical offset of the record at this position
    uint32_t position; // byte position in the .log file where that record starts
} __attribute__((packed));

static_assert(sizeof(IndexEntry) == 12, "IndexEntry must be 12 bytes");

class Segment
{
  public:
    // Create a new, empty segment in dir with the given base offset.
    Segment(const std::filesystem::path &dir, uint64_t base_offset, uint64_t max_bytes);

    // Open and recover an existing segment from disk.
    // Returns nullptr if the segment files do not exist.
    static std::unique_ptr<Segment> Open(const std::filesystem::path &dir, uint64_t base_offset, uint64_t max_bytes);

    ~Segment();

    Segment(const Segment &) = delete;
    Segment &operator=(const Segment &) = delete;

    // Append data. Returns the assigned logical offset, or UINT64_MAX on error.
    uint64_t Append(std::span<const std::byte> data);

    // Read the record at the given logical offset.
    // Returns an empty vector if the offset is out of range or CRC check fails.
    std::vector<std::byte> Read(uint64_t offset) const;

    uint64_t BaseOffset() const
    {
        return base_offset_;
    }
    uint64_t NextOffset() const
    {
        return next_offset_;
    }
    uint64_t LogSize() const
    {
        return log_size_;
    }
    bool IsFull() const
    {
        return log_size_ >= max_bytes_;
    }

  private:
    Segment() = default; // used by Open()

    bool OpenFiles(bool create);
    void LoadFromDisk();
    void WriteIndexEntry(uint64_t offset, uint32_t position);

    // Binary search: returns {indexed_offset, byte_position} of the last index entry
    // whose offset <= target_offset, or {UINT64_MAX, UINT32_MAX} if none exists.
    std::pair<uint64_t, uint32_t> FindNearestIndex(uint64_t target_offset) const;

    std::filesystem::path log_path_;
    std::filesystem::path index_path_;

    uint64_t base_offset_{0};
    uint64_t next_offset_{0};
    uint64_t log_size_{0};
    uint64_t max_bytes_{0};

    int log_fd_{-1};
    int index_fd_{-1};

    // In-memory sparse index. On Open(), loaded from the .index file via mmap.
    // Kept sorted by offset for O(log n) binary search.
    std::vector<IndexEntry> index_;

    // Bytes written to the log since the last index entry was recorded.
    uint32_t bytes_since_index_{0};
};
