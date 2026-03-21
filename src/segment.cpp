#include "segment.h"
#include "crc32.h"

#include <algorithm>
#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

// On-disk record: [4B payload_length][4B crc32][payload...]
static constexpr uint32_t kRecordHeaderSize = sizeof(uint32_t) + sizeof(uint32_t);

// ---------------------------------------------------------------------------
// Filename helpers
// ---------------------------------------------------------------------------

static std::string FormatBaseName(uint64_t base_offset)
{
    char buf[24];
    snprintf(buf, sizeof(buf), "%020" PRIu64, base_offset);
    return buf;
}

static void BuildPaths(const std::filesystem::path &dir, uint64_t base_offset, std::filesystem::path &log_out,
                       std::filesystem::path &index_out)
{
    auto base = dir / FormatBaseName(base_offset);
    log_out = std::string(base) + ".log";
    index_out = std::string(base) + ".index";
}

// ---------------------------------------------------------------------------
// Public constructor — creates a new, empty segment
// ---------------------------------------------------------------------------

Segment::Segment(const std::filesystem::path &dir, uint64_t base_offset, uint64_t max_bytes)
    : base_offset_(base_offset), next_offset_(base_offset), log_size_(0), max_bytes_(max_bytes)
{
    BuildPaths(dir, base_offset, log_path_, index_path_);
    OpenFiles(/*create=*/true);
}

// ---------------------------------------------------------------------------
// Factory — opens an existing segment from disk
// ---------------------------------------------------------------------------

std::unique_ptr<Segment> Segment::Open(const std::filesystem::path &dir, uint64_t base_offset, uint64_t max_bytes)
{
    auto seg = std::unique_ptr<Segment>(new Segment());
    seg->base_offset_ = base_offset;
    seg->next_offset_ = base_offset;
    seg->log_size_ = 0;
    seg->max_bytes_ = max_bytes;

    BuildPaths(dir, base_offset, seg->log_path_, seg->index_path_);

    if (!seg->OpenFiles(/*create=*/false)) {
        return nullptr;
    }
    seg->LoadFromDisk();
    return seg;
}

// ---------------------------------------------------------------------------
// Destructor
// ---------------------------------------------------------------------------

Segment::~Segment()
{
    if (log_fd_ >= 0)
        close(log_fd_);
    if (index_fd_ >= 0)
        close(index_fd_);
}

// ---------------------------------------------------------------------------
// OpenFiles
// ---------------------------------------------------------------------------

bool Segment::OpenFiles(bool create)
{
    int flags = O_RDWR | (create ? O_CREAT : 0);
    log_fd_ = open(log_path_.c_str(), flags, 0644);
    if (log_fd_ < 0) {
        std::cerr << "Segment: cannot open log " << log_path_ << ": " << strerror(errno) << "\n";
        return false;
    }

    index_fd_ = open(index_path_.c_str(), flags, 0644);
    if (index_fd_ < 0) {
        std::cerr << "Segment: cannot open index " << index_path_ << ": " << strerror(errno) << "\n";
        close(log_fd_);
        log_fd_ = -1;
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// LoadFromDisk — recover state from existing segment files
// ---------------------------------------------------------------------------

void Segment::LoadFromDisk()
{
    // Step 1: Load sparse index from the .index file.
    // We mmap the entire file for a zero-copy bulk read into the index_ vector,
    // then immediately unmap — the data lives in the vector from this point on.
    {
        struct stat ist
        {
        };
        if (fstat(index_fd_, &ist) == 0 && ist.st_size > 0) {
            auto map_size = static_cast<size_t>(ist.st_size);
            void *mapped = mmap(nullptr, map_size, PROT_READ, MAP_PRIVATE, index_fd_, 0);
            if (mapped != MAP_FAILED) {
                size_t count = map_size / sizeof(IndexEntry);
                const auto *entries = static_cast<const IndexEntry *>(mapped);
                index_.assign(entries, entries + count);
                munmap(mapped, map_size);
            }
        }
    }

    // Step 2: Determine the current log file size.
    {
        struct stat lst
        {
        };
        if (fstat(log_fd_, &lst) != 0 || lst.st_size == 0) {
            next_offset_ = base_offset_;
            return;
        }
        log_size_ = static_cast<uint64_t>(lst.st_size);
    }

    // Step 3: Scan the log tail from the last indexed position to EOF.
    // This is O(kIndexIntervalBytes / avg_record_size) — a very short scan.
    // Its purpose is to:
    //   (a) determine next_offset_ (the next offset to be assigned), and
    //   (b) restore bytes_since_index_ so future writes continue the sparse
    //       index at the right interval.
    uint32_t scan_pos;
    uint64_t scan_logical;
    if (!index_.empty()) {
        scan_pos = index_.back().position;
        scan_logical = index_.back().offset;
    }
    else {
        scan_pos = 0;
        scan_logical = base_offset_;
    }

    uint32_t cur_pos = scan_pos;
    uint64_t cur_logical = scan_logical;

    while (cur_pos < log_size_) {
        uint32_t len = 0;
        ssize_t r = pread(log_fd_, &len, sizeof(len), static_cast<off_t>(cur_pos));
        if (r != static_cast<ssize_t>(sizeof(len)))
            break;

        uint32_t record_total = kRecordHeaderSize + len;
        if (static_cast<uint64_t>(cur_pos) + record_total > log_size_)
            break; // truncated

        cur_pos += record_total;
        cur_logical++;
    }

    next_offset_ = cur_logical;

    // Restore the byte counter: how many bytes have been written since the last
    // index entry.  The next Append() call uses this to decide when to index.
    if (index_.empty()) {
        bytes_since_index_ = static_cast<uint32_t>(log_size_);
    }
    else {
        bytes_since_index_ = static_cast<uint32_t>(log_size_ - index_.back().position);
    }
}

// ---------------------------------------------------------------------------
// WriteIndexEntry
// ---------------------------------------------------------------------------

void Segment::WriteIndexEntry(uint64_t offset, uint32_t position)
{
    IndexEntry entry{offset, position};
    off_t write_pos = static_cast<off_t>(index_.size() * sizeof(IndexEntry));
    if (pwrite(index_fd_, &entry, sizeof(entry), write_pos) == static_cast<ssize_t>(sizeof(entry))) {
        index_.push_back(entry);
    }
}

// ---------------------------------------------------------------------------
// FindNearestIndex
// ---------------------------------------------------------------------------

std::pair<uint64_t, uint32_t> Segment::FindNearestIndex(uint64_t target_offset) const
{
    if (index_.empty())
        return {UINT64_MAX, UINT32_MAX};

    // upper_bound gives the first entry with .offset > target_offset.
    // The comparator is extracted to avoid formatting edge cases near ColumnLimit.
    auto cmp = [](uint64_t val, const IndexEntry &e) { return val < e.offset; };
    auto it = std::upper_bound(index_.begin(), index_.end(), target_offset, cmp);

    if (it == index_.begin())
        return {UINT64_MAX, UINT32_MAX}; // all entries are > target
    --it;
    return {it->offset, it->position};
}

// ---------------------------------------------------------------------------
// Append
// ---------------------------------------------------------------------------

uint64_t Segment::Append(std::span<const std::byte> data)
{
    if (IsFull())
        return UINT64_MAX;

    auto len = static_cast<uint32_t>(data.size());
    uint32_t crc = crc32::compute(data.data(), data.size());
    uint32_t record_total = kRecordHeaderSize + len;

    // Write a sparse index entry for the first record of each segment and
    // whenever kIndexIntervalBytes of log data has accumulated.
    if (index_.empty() || bytes_since_index_ >= kIndexIntervalBytes) {
        WriteIndexEntry(next_offset_, static_cast<uint32_t>(log_size_));
        bytes_since_index_ = 0;
    }

    // Write [length][crc32][payload] to the log file.
    // pwrite() at explicit offsets avoids any dependency on the file-position
    // cursor and is safe to use alongside pread() for concurrent reads.
    auto base = static_cast<off_t>(log_size_);
    if (pwrite(log_fd_, &len, sizeof(len), base) != static_cast<ssize_t>(sizeof(len)))
        return UINT64_MAX;
    if (pwrite(log_fd_, &crc, sizeof(crc), base + sizeof(len)) != static_cast<ssize_t>(sizeof(crc)))
        return UINT64_MAX;
    if (pwrite(log_fd_, data.data(), len, base + kRecordHeaderSize) != static_cast<ssize_t>(len))
        return UINT64_MAX;

    uint64_t assigned = next_offset_;
    log_size_ += record_total;
    bytes_since_index_ += record_total;
    next_offset_++;
    return assigned;
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

std::vector<std::byte> Segment::Read(uint64_t offset) const
{
    if (offset < base_offset_ || offset >= next_offset_)
        return {};

    auto [indexed_offset, start_pos] = FindNearestIndex(offset);
    if (indexed_offset == UINT64_MAX)
        return {};

    // Linear scan from start_pos, skipping records until we reach the target.
    // The number of records to skip is (offset - indexed_offset).
    // Each skip costs one pread for the 4-byte length field.
    uint32_t cur_pos = start_pos;
    for (uint64_t i = indexed_offset; i < offset; i++) {
        uint32_t len = 0;
        if (pread(log_fd_, &len, sizeof(len), static_cast<off_t>(cur_pos)) != static_cast<ssize_t>(sizeof(len)))
            return {};
        cur_pos += kRecordHeaderSize + len;
    }

    // Read the target record header.
    uint32_t len = 0, stored_crc = 0;
    if (pread(log_fd_, &len, sizeof(len), static_cast<off_t>(cur_pos)) != static_cast<ssize_t>(sizeof(len)))
        return {};
    if (pread(log_fd_, &stored_crc, sizeof(stored_crc), static_cast<off_t>(cur_pos) + sizeof(len)) !=
        static_cast<ssize_t>(sizeof(stored_crc)))
        return {};

    // Read payload.
    std::vector<std::byte> payload(len);
    if (pread(log_fd_, payload.data(), len, static_cast<off_t>(cur_pos) + kRecordHeaderSize) != static_cast<ssize_t>(len))
        return {};

    // Verify CRC32.
    uint32_t actual_crc = crc32::compute(payload.data(), len);
    if (actual_crc != stored_crc) {
        std::cerr << "Segment: CRC mismatch at offset " << offset << " (stored=" << stored_crc << " actual=" << actual_crc << ")\n";
        return {};
    }

    return payload;
}
