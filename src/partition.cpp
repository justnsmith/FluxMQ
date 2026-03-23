#include "partition.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <unistd.h>
#include <unordered_map>

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
// AppendKV — encode key + value into a framed record
// ---------------------------------------------------------------------------
// On-disk payload format: [2B key_len][key_bytes][value_bytes]
// This allows Compact() to extract keys for dedup.

uint64_t Partition::AppendKV(const uint8_t *key, size_t key_len, const uint8_t *value, size_t value_len)
{
    std::lock_guard<std::mutex> lock(mu_);

    // Encode: [2B key_len][key][value]
    size_t frame_len = 2 + key_len + value_len;
    std::vector<std::byte> frame(frame_len);
    auto kl = static_cast<uint16_t>(key_len);
    frame[0] = static_cast<std::byte>(kl >> 8);
    frame[1] = static_cast<std::byte>(kl & 0xFF);
    if (key_len > 0) {
        std::memcpy(frame.data() + 2, key, key_len);
    }
    if (value_len > 0) {
        std::memcpy(frame.data() + 2 + key_len, value, value_len);
    }

    uint64_t offset = log_.Append(std::span<const std::byte>(frame));
    cv_.notify_all();
    return offset;
}

// ---------------------------------------------------------------------------
// Compact — key-based log compaction on closed segments
// ---------------------------------------------------------------------------

size_t Partition::Compact()
{
    std::lock_guard<std::mutex> lock(mu_);

    auto all_records = log_.ReadClosedSegments();
    if (all_records.empty())
        return 0;

    // Build key → index-of-latest-record map.
    // Records with empty keys are always kept.
    std::unordered_map<std::string, size_t> latest; // key → index in all_records
    for (size_t i = 0; i < all_records.size(); ++i) {
        const auto &rec = all_records[i];
        if (rec.data.size() < 2)
            continue;

        uint16_t kl =
            (static_cast<uint16_t>(static_cast<uint8_t>(rec.data[0])) << 8) | static_cast<uint16_t>(static_cast<uint8_t>(rec.data[1]));

        if (kl == 0)
            continue; // null-key: always kept

        if (2 + kl > rec.data.size())
            continue; // malformed

        std::string key(reinterpret_cast<const char *>(rec.data.data() + 2), kl);
        latest[key] = i; // later offsets overwrite earlier ones
    }

    // Build the set of indices to keep.
    std::vector<bool> keep(all_records.size(), false);
    size_t keep_count = 0;

    for (size_t i = 0; i < all_records.size(); ++i) {
        const auto &rec = all_records[i];
        if (rec.data.size() < 2) {
            keep[i] = true;
            keep_count++;
            continue;
        }

        uint16_t kl =
            (static_cast<uint16_t>(static_cast<uint8_t>(rec.data[0])) << 8) | static_cast<uint16_t>(static_cast<uint8_t>(rec.data[1]));

        if (kl == 0) {
            // Null-key record: always keep.
            keep[i] = true;
            keep_count++;
        }
        else {
            std::string key(reinterpret_cast<const char *>(rec.data.data() + 2), kl);
            auto it = latest.find(key);
            if (it != latest.end() && it->second == i) {
                keep[i] = true;
                keep_count++;
            }
        }
    }

    size_t removed = all_records.size() - keep_count;
    if (removed == 0)
        return 0; // nothing to compact

    // Build compacted record list (preserving order).
    std::vector<Log::RawRecord> compacted;
    compacted.reserve(keep_count);
    for (size_t i = 0; i < all_records.size(); ++i) {
        if (keep[i]) {
            compacted.push_back(std::move(all_records[i]));
        }
    }

    log_.ReplaceClosedSegments(compacted);
    return removed;
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

        // Decode framed key-value record: [2B key_len][key][value]
        // If the record is too small or was written by old-style Append()
        // (which doesn't frame), treat the entire payload as value.
        const auto *p = reinterpret_cast<const uint8_t *>(raw.data());
        size_t raw_len = raw.size();

        if (raw_len >= 2) {
            uint16_t kl = (static_cast<uint16_t>(p[0]) << 8) | static_cast<uint16_t>(p[1]);
            if (2 + kl <= raw_len) {
                // Valid framing.
                rec.key.assign(p + 2, p + 2 + kl);
                rec.value.assign(p + 2 + kl, p + raw_len);
            }
            else {
                // Malformed or legacy record — treat as raw value.
                rec.value.resize(raw_len);
                std::memcpy(rec.value.data(), raw.data(), raw_len);
            }
        }
        else {
            rec.value.resize(raw_len);
            std::memcpy(rec.value.data(), raw.data(), raw_len);
        }

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
