#include "partition.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <unistd.h>

Partition::Partition(const std::filesystem::path &dir, int id, uint64_t max_seg_bytes)
    : id_(id), log_(dir.string(), max_seg_bytes), offsets_dir_(dir / "consumer_offsets")
{
    std::filesystem::create_directories(offsets_dir_);
    LoadOffsets();
}

// ---------------------------------------------------------------------------
// Append
// ---------------------------------------------------------------------------

uint64_t Partition::Append(const uint8_t *data, size_t len)
{
    std::lock_guard<std::mutex> lock(mu_);
    auto bytes = std::span<const std::byte>(reinterpret_cast<const std::byte *>(data), len);
    uint64_t offset = log_.Append(bytes);
    // Notify while holding the lock (waiters will re-acquire after wakeup).
    cv_.notify_all();
    return offset;
}

// ---------------------------------------------------------------------------
// ReadBatchUpTo (internal shared logic)
// ---------------------------------------------------------------------------

std::vector<Record> Partition::ReadBatchUpTo(uint64_t fetch_offset, uint32_t max_bytes, uint64_t limit) const
{
    std::vector<Record> result;
    uint64_t cur = fetch_offset;
    uint32_t total = 0;

    while (cur < limit) {
        auto raw = log_.Read(cur);
        if (raw.empty())
            break;

        auto sz = static_cast<uint32_t>(raw.size());
        if (total + sz > max_bytes && !result.empty())
            break;

        Record rec;
        rec.offset = cur;
        rec.value.resize(raw.size());
        std::memcpy(rec.value.data(), raw.data(), raw.size());

        result.push_back(std::move(rec));
        total += sz;
        ++cur;

        if (total >= max_bytes)
            break;
    }
    return result;
}

// ---------------------------------------------------------------------------
// ReadBatch  (consumer-facing; capped at HighWatermark)
// ---------------------------------------------------------------------------

std::vector<Record> Partition::ReadBatch(uint64_t fetch_offset, uint32_t max_bytes) const
{
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t h = hwm_.load(std::memory_order_acquire);
    uint64_t limit = (h == kUnreplicatedHWM) ? log_.NextOffset() : h;
    return ReadBatchUpTo(fetch_offset, max_bytes, limit);
}

// ---------------------------------------------------------------------------
// ReadBatchForReplication  (reads up to NextOffset, ignoring HWM)
// ---------------------------------------------------------------------------

std::vector<Record> Partition::ReadBatchForReplication(uint64_t fetch_offset, uint32_t max_bytes) const
{
    std::lock_guard<std::mutex> lock(mu_);
    return ReadBatchUpTo(fetch_offset, max_bytes, log_.NextOffset());
}

// ---------------------------------------------------------------------------
// WaitForData
// ---------------------------------------------------------------------------

bool Partition::WaitForData(uint64_t fetch_offset, std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_until(lock, deadline, [&] {
        uint64_t h = hwm_.load(std::memory_order_relaxed);
        uint64_t limit = (h == kUnreplicatedHWM) ? log_.NextOffset() : h;
        return limit > fetch_offset;
    });
}

// ---------------------------------------------------------------------------
// HighWatermark / SetHighWatermark
// ---------------------------------------------------------------------------

uint64_t Partition::HighWatermark() const
{
    uint64_t h = hwm_.load(std::memory_order_acquire);
    if (h == kUnreplicatedHWM) {
        std::lock_guard<std::mutex> lock(mu_);
        return log_.NextOffset();
    }
    return h;
}

void Partition::SetHighWatermark(uint64_t hwm)
{
    hwm_.store(hwm, std::memory_order_release);
    cv_.notify_all();
}

// ---------------------------------------------------------------------------
// NextOffset
// ---------------------------------------------------------------------------

uint64_t Partition::NextOffset() const
{
    std::lock_guard<std::mutex> lock(mu_);
    return log_.NextOffset();
}

// ---------------------------------------------------------------------------
// CommitOffset / FetchCommittedOffset
// ---------------------------------------------------------------------------

void Partition::CommitOffset(std::string_view group, uint64_t offset)
{
    // Atomic write: write temp file → fsync → rename over destination.
    std::string group_str(group);
    auto tmp = offsets_dir_ / (group_str + ".offset.tmp");
    auto dst = offsets_dir_ / (group_str + ".offset");

    {
        std::ofstream f(tmp);
        f << offset << "\n";
    }
    {
        int fd = open(tmp.c_str(), O_RDWR);
        if (fd >= 0) {
            fsync(fd);
            close(fd);
        }
    }
    std::filesystem::rename(tmp, dst);

    std::lock_guard<std::mutex> lock(mu_);
    offsets_[group_str] = offset;
}

uint64_t Partition::FetchCommittedOffset(std::string_view group) const
{
    std::lock_guard<std::mutex> lock(mu_);
    auto it = offsets_.find(std::string(group));
    return it != offsets_.end() ? it->second : 0;
}

// ---------------------------------------------------------------------------
// LoadOffsets — recover committed offsets from disk on startup
// ---------------------------------------------------------------------------

void Partition::LoadOffsets()
{
    if (!std::filesystem::exists(offsets_dir_))
        return;
    for (const auto &entry : std::filesystem::directory_iterator(offsets_dir_)) {
        if (entry.path().extension() == ".offset") {
            std::string group = entry.path().stem().string();
            std::ifstream f(entry.path());
            uint64_t off = 0;
            if (f >> off)
                offsets_[group] = off;
        }
    }
}
