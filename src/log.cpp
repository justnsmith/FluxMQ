#include "log.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <iostream>

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

Log::Log(std::string dir, uint64_t max_segment_bytes) : dir_(std::move(dir)), max_segment_bytes_(max_segment_bytes)
{
    std::filesystem::create_directories(dir_);
    LoadSegments();
}

// ---------------------------------------------------------------------------
// LoadSegments — scan the directory for existing .log files and open each one
// ---------------------------------------------------------------------------

void Log::LoadSegments()
{
    for (const auto &entry : std::filesystem::directory_iterator(dir_)) {
        if (entry.path().extension() != ".log")
            continue;

        std::string stem = entry.path().stem().string();
        if (stem.size() != 20)
            continue; // not a segment filename

        uint64_t base_offset = std::stoull(stem);
        auto seg = Segment::Open(dir_, base_offset, max_segment_bytes_);
        if (seg) {
            segments_.push_back(std::move(seg));
        }
    }

    std::sort(segments_.begin(), segments_.end(), [](const auto &a, const auto &b) { return a->BaseOffset() < b->BaseOffset(); });
}

// ---------------------------------------------------------------------------
// ActiveSegment — returns the writable segment, creating one if needed
// ---------------------------------------------------------------------------

Segment *Log::ActiveSegment()
{
    if (segments_.empty()) {
        segments_.emplace_back(std::make_unique<Segment>(dir_, 0, max_segment_bytes_));
    }
    return segments_.back().get();
}

// ---------------------------------------------------------------------------
// FindSegment — binary search for the segment containing the given offset
// ---------------------------------------------------------------------------

Segment *Log::FindSegment(uint64_t offset) const
{
    if (segments_.empty())
        return nullptr;

    // Find the last segment whose BaseOffset <= offset.
    auto it = std::upper_bound(segments_.begin(), segments_.end(), offset,
                               [](uint64_t val, const std::unique_ptr<Segment> &seg) { return val < seg->BaseOffset(); });

    if (it == segments_.begin())
        return nullptr;
    --it;

    Segment *seg = it->get();
    if (offset >= seg->NextOffset())
        return nullptr; // offset not yet written
    return seg;
}

// ---------------------------------------------------------------------------
// Append (raw bytes)
// ---------------------------------------------------------------------------

uint64_t Log::Append(std::span<const std::byte> data)
{
    Segment *active = ActiveSegment();
    if (active->IsFull()) {
        Roll();
        active = ActiveSegment();
    }
    return active->Append(data);
}

// ---------------------------------------------------------------------------
// Append (string_view convenience overload)
// ---------------------------------------------------------------------------

uint64_t Log::Append(std::string_view data)
{
    return Append(std::span<const std::byte>(reinterpret_cast<const std::byte *>(data.data()), data.size()));
}

// ---------------------------------------------------------------------------
// Read (raw bytes)
// ---------------------------------------------------------------------------

std::vector<std::byte> Log::Read(uint64_t offset) const
{
    Segment *seg = FindSegment(offset);
    if (!seg)
        return {};
    return seg->Read(offset);
}

// ---------------------------------------------------------------------------
// ReadString
// ---------------------------------------------------------------------------

std::string Log::ReadString(uint64_t offset) const
{
    auto bytes = Read(offset);
    return std::string(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

// ---------------------------------------------------------------------------
// Roll — create a new active segment
// ---------------------------------------------------------------------------

void Log::Roll()
{
    uint64_t new_base = NextOffset();
    segments_.emplace_back(std::make_unique<Segment>(dir_, new_base, max_segment_bytes_));
}

// ---------------------------------------------------------------------------
// DeleteBefore — retention: remove segments entirely before up_to_offset
// ---------------------------------------------------------------------------

void Log::DeleteBefore(uint64_t up_to_offset)
{
    while (segments_.size() > 1) {
        const auto &front = segments_.front();
        if (front->NextOffset() > up_to_offset)
            break; // still has live data

        // Remove files from disk.
        std::filesystem::path log_path = dir_ / (front->BaseOffset() == 0 ? "00000000000000000000" : "");
        // Build paths the same way Segment does.
        char buf[24];
        snprintf(buf, sizeof(buf), "%020" PRIu64, front->BaseOffset());
        auto base = dir_ / std::string(buf);
        std::filesystem::remove(std::string(base) + ".log");
        std::filesystem::remove(std::string(base) + ".index");

        segments_.erase(segments_.begin());
    }
}

// ---------------------------------------------------------------------------
// NextOffset
// ---------------------------------------------------------------------------

uint64_t Log::NextOffset() const
{
    if (segments_.empty())
        return 0;
    return segments_.back()->NextOffset();
}

// ---------------------------------------------------------------------------
// BaseOffset
// ---------------------------------------------------------------------------

uint64_t Log::BaseOffset() const
{
    if (segments_.empty())
        return 0;
    return segments_.front()->BaseOffset();
}

// ---------------------------------------------------------------------------
// ReadClosedSegments — read all records from non-active segments
// ---------------------------------------------------------------------------

std::vector<Log::RawRecord> Log::ReadClosedSegments() const
{
    std::vector<RawRecord> result;
    if (segments_.size() <= 1)
        return result; // only active segment (or empty)

    // All segments except the last one are "closed".
    for (size_t i = 0; i + 1 < segments_.size(); ++i) {
        const auto &seg = segments_[i];
        for (uint64_t off = seg->BaseOffset(); off < seg->NextOffset(); ++off) {
            auto data = seg->Read(off);
            if (!data.empty()) {
                result.push_back({off, std::move(data)});
            }
        }
    }
    return result;
}

// ---------------------------------------------------------------------------
// ReplaceClosedSegments — swap closed segments with compacted records
// ---------------------------------------------------------------------------

void Log::ReplaceClosedSegments(const std::vector<RawRecord> &records)
{
    if (segments_.size() <= 1)
        return; // nothing to replace

    // Determine the base offset for the compacted segment.
    uint64_t compacted_base = segments_.front()->BaseOffset();

    // Delete old closed segment files from disk and remove from vector.
    // Keep only the active segment (last one).
    auto active = std::move(segments_.back());
    for (size_t i = 0; i + 1 < segments_.size(); ++i) {
        const auto &seg = segments_[i];
        // Build file paths and remove.
        char buf[24];
        snprintf(buf, sizeof(buf), "%020" PRIu64, seg->BaseOffset());
        auto base = dir_ / std::string(buf);
        std::filesystem::remove(std::string(base) + ".log");
        std::filesystem::remove(std::string(base) + ".index");
    }
    segments_.clear();

    // Create a new segment and write the compacted records into it.
    if (!records.empty()) {
        auto compacted = std::make_unique<Segment>(dir_, compacted_base, max_segment_bytes_);
        for (const auto &rec : records) {
            compacted->Append(rec.data);
        }
        segments_.push_back(std::move(compacted));
    }

    // Restore the active segment at the end.
    segments_.push_back(std::move(active));
}
