#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <unordered_map>

// ProducerStateManager implements the broker-side state for idempotent producers.
//
// Each producer is assigned a unique (producer_id, epoch) pair via InitProducerId.
// On each produce request (v1), the producer includes its PID, epoch, and a
// per-partition sequence number.  The broker tracks the last accepted sequence
// for each (PID, partition) tuple and:
//
//   - seq == expected (last+1 or 0 for first):  accept, update state
//   - seq == last (duplicate):                   return cached offset, don't re-append
//   - seq != expected && seq != last:            reject as out-of-order
//
// This is the same model Kafka uses for its idempotent producer (KIP-98).
class ProducerStateManager
{
  public:
    // Result of a sequence check.
    enum class CheckResult
    {
        kAccept,     // New, in-order sequence — proceed with append
        kDuplicate,  // Same as last accepted sequence — return cached offset
        kOutOfOrder, // Sequence gap or regression
        kUnknownPID, // Producer ID not registered
    };

    struct CheckOutput
    {
        CheckResult result;
        uint64_t cached_offset{0}; // valid only when result == kDuplicate
    };

    // Allocate a new producer ID with epoch 0.
    uint64_t AllocateProducerId()
    {
        return next_pid_.fetch_add(1, std::memory_order_relaxed);
    }

    // Register a PID so the broker recognises it.
    void RegisterProducer(uint64_t pid, uint16_t epoch)
    {
        std::lock_guard lock(mu_);
        producers_[pid] = epoch;
    }

    // Check whether a produce request with (pid, epoch, partition, seq) should be
    // accepted, is a duplicate, or is out of order.
    CheckOutput Check(uint64_t pid, uint16_t epoch, int32_t partition, int32_t seq) const
    {
        std::lock_guard lock(mu_);

        auto pit = producers_.find(pid);
        if (pit == producers_.end()) {
            return {CheckResult::kUnknownPID, 0};
        }

        // Epoch mismatch means the producer was fenced (future extension).
        if (pit->second != epoch) {
            return {CheckResult::kUnknownPID, 0};
        }

        PartitionKey key{pid, partition};
        auto it = seq_state_.find(key);

        if (it == seq_state_.end()) {
            // First produce for this (pid, partition) — must be seq 0.
            if (seq == 0)
                return {CheckResult::kAccept, 0};
            return {CheckResult::kOutOfOrder, 0};
        }

        const auto &state = it->second;
        int32_t expected = state.last_seq + 1;

        if (seq == expected)
            return {CheckResult::kAccept, 0};
        if (seq == state.last_seq)
            return {CheckResult::kDuplicate, state.last_offset};

        return {CheckResult::kOutOfOrder, 0};
    }

    // Record a successfully appended produce so future duplicates can be detected.
    void RecordAppend(uint64_t pid, int32_t partition, int32_t seq, uint64_t offset)
    {
        std::lock_guard lock(mu_);
        PartitionKey key{pid, partition};
        seq_state_[key] = SeqState{seq, offset};
    }

    // Check if a PID is registered.
    bool IsRegistered(uint64_t pid) const
    {
        std::lock_guard lock(mu_);
        return producers_.find(pid) != producers_.end();
    }

  private:
    struct PartitionKey
    {
        uint64_t pid;
        int32_t partition;

        bool operator==(const PartitionKey &o) const
        {
            return pid == o.pid && partition == o.partition;
        }
    };

    struct PartitionKeyHash
    {
        size_t operator()(const PartitionKey &k) const
        {
            // Simple hash combining.
            return std::hash<uint64_t>()(k.pid) ^ (std::hash<int32_t>()(k.partition) << 1);
        }
    };

    struct SeqState
    {
        int32_t last_seq{-1};
        uint64_t last_offset{0};
    };

    std::atomic<uint64_t> next_pid_{1}; // 0 is reserved for "no PID"
    mutable std::mutex mu_;

    // Registered producers: pid → epoch
    std::unordered_map<uint64_t, uint16_t> producers_;

    // Per (pid, partition) sequence tracking.
    std::unordered_map<PartitionKey, SeqState, PartitionKeyHash> seq_state_;
};
