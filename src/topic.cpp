#include "topic.h"

#include <cstring>
#include <stdexcept>

// ---------------------------------------------------------------------------
// MurmurHash3 (32-bit) — key-to-partition routing
// ---------------------------------------------------------------------------

static uint32_t murmur3(const void *key, size_t len)
{
    const auto *data = static_cast<const uint8_t *>(key);
    uint32_t h = 0x9747b28cU;

    for (size_t i = 0; i + 4 <= len; i += 4) {
        uint32_t k;
        std::memcpy(&k, data + i, 4);
        k *= 0xcc9e2d51U;
        k = (k << 15) | (k >> 17);
        k *= 0x1b873593U;
        h ^= k;
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64U;
    }

    size_t rem = len & 3U;
    if (rem) {
        uint32_t k = 0;
        for (size_t i = 0; i < rem; ++i)
            k |= static_cast<uint32_t>(data[(len & ~3U) + i]) << (i * 8);
        k *= 0xcc9e2d51U;
        k = (k << 15) | (k >> 17);
        k *= 0x1b873593U;
        h ^= k;
    }

    h ^= static_cast<uint32_t>(len);
    h ^= h >> 16;
    h *= 0x85ebca6bU;
    h ^= h >> 13;
    h *= 0xc2b2ae35U;
    h ^= h >> 16;
    return h;
}

// ---------------------------------------------------------------------------
// Topic
// ---------------------------------------------------------------------------

Topic::Topic(std::string name, int num_partitions, const std::filesystem::path &base_dir, uint64_t max_seg_bytes) : name_(std::move(name))
{
    for (int i = 0; i < num_partitions; ++i) {
        auto part_dir = base_dir / ("partition-" + std::to_string(i));
        std::filesystem::create_directories(part_dir);
        partitions_.push_back(std::make_unique<Partition>(part_dir, i, max_seg_bytes));
    }
}

std::pair<int, uint64_t> Topic::Publish(const uint8_t *value, size_t value_len, const uint8_t *key, size_t key_len)
{
    int part_id;
    if (key == nullptr || key_len == 0) {
        part_id = rr_counter_.fetch_add(1, std::memory_order_relaxed) % NumPartitions();
    }
    else {
        uint32_t h = murmur3(key, key_len);
        part_id = static_cast<int>(h % static_cast<uint32_t>(NumPartitions()));
    }
    uint64_t offset = partitions_[part_id]->Append(value, value_len);
    return {part_id, offset};
}

Partition &Topic::GetPartition(int id)
{
    if (id < 0 || id >= NumPartitions())
        throw std::out_of_range("Topic::GetPartition: invalid id " + std::to_string(id));
    return *partitions_[id];
}

const Partition &Topic::GetPartition(int id) const
{
    if (id < 0 || id >= NumPartitions())
        throw std::out_of_range("Topic::GetPartition: invalid id " + std::to_string(id));
    return *partitions_[id];
}
