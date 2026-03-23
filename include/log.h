#pragma once

#include "segment.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

inline constexpr uint64_t kDefaultMaxSegmentBytes = 1ULL * 1024 * 1024 * 1024; // 1 GiB

// Log manages an ordered sequence of Segments that together form a single
// append-only partition log.  Offsets are monotonically increasing integers
// assigned at append time; each segment covers a contiguous range of offsets.
class Log
{
  public:
    // Open (or create) a log in the given directory.
    explicit Log(std::string dir, uint64_t max_segment_bytes = kDefaultMaxSegmentBytes);

    ~Log() = default;

    Log(const Log &) = delete;
    Log &operator=(const Log &) = delete;

    // Append raw bytes.  Returns the assigned logical offset, or UINT64_MAX on error.
    uint64_t Append(std::span<const std::byte> data);

    // Convenience overload for string data.
    uint64_t Append(std::string_view data);

    // Read the record at the given logical offset.
    // Returns an empty vector if the offset does not exist.
    std::vector<std::byte> Read(uint64_t offset) const;

    // Read the record at the given logical offset as a string.
    std::string ReadString(uint64_t offset) const;

    // Force rotation to a new active segment immediately.
    // Normally rotation happens automatically when the active segment is full.
    void Roll();

    // Delete all segments whose records are entirely before up_to_offset.
    // Records at or after up_to_offset are preserved.
    void DeleteBefore(uint64_t up_to_offset);

    uint64_t NextOffset() const;

    // Base offset of the first segment (earliest available offset).
    uint64_t BaseOffset() const;

    // Read all records from closed (non-active) segments.
    // Each record is returned as raw bytes at its logical offset.
    struct RawRecord
    {
        uint64_t offset;
        std::vector<std::byte> data;
    };
    std::vector<RawRecord> ReadClosedSegments() const;

    // Replace all closed segments with a single compacted segment containing
    // the given records.  The active segment is left untouched.
    void ReplaceClosedSegments(const std::vector<RawRecord> &records);

    size_t NumSegments() const
    {
        return segments_.size();
    }

  private:
    void LoadSegments();
    Segment *ActiveSegment();
    Segment *FindSegment(uint64_t offset) const;

    std::filesystem::path dir_;
    uint64_t max_segment_bytes_;
    std::vector<std::unique_ptr<Segment>> segments_;
};
