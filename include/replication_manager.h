#pragma once

#include "cluster_store.h"
#include "topic_manager.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using SteadyClock = std::chrono::steady_clock;

// ── FollowerState ────────────────────────────────────────────────────────────
// Tracked on the leader for each replica in the partition's replica list.
struct FollowerState
{
    int32_t broker_id{0};
    uint64_t fetch_offset{0};
    SteadyClock::time_point last_fetch_time;
    bool in_isr{false};
};

// ── ReplicationManager ───────────────────────────────────────────────────────
//
// Responsibilities:
//   Leader side  — receives OnFollowerFetch() calls from the request handler
//                  and advances the partition HWM as ISR members converge.
//   Follower side — one background thread per assigned follower partition
//                   continuously pulls records from the current leader.
//   Maintenance  — a single background thread handles ISR shrink/grow and
//                  starts follower threads for newly-assigned partitions.
class ReplicationManager
{
  public:
    ReplicationManager(ClusterStore &cs, TopicManager &tm, std::chrono::milliseconds replica_lag_ms = std::chrono::milliseconds(10000));
    ~ReplicationManager();

    ReplicationManager(const ReplicationManager &) = delete;
    ReplicationManager &operator=(const ReplicationManager &) = delete;

    // Start all background threads.  Call once after the broker is fully wired.
    void Start();

    // Called by the request handler when a ReplicaFetch arrives from a follower.
    // Updates the follower's tracked offset and recomputes the partition HWM.
    void OnFollowerFetch(const std::string &topic, int32_t partition, int32_t follower_id, uint64_t follower_fetch_offset);

  private:
    // ── Leader-side per-partition state ─────────────────────────────────────

    struct PartitionReplicaState
    {
        std::vector<FollowerState> followers;
        std::mutex mu;
    };

    // Returns the per-partition state, creating it if necessary.
    PartitionReplicaState &GetOrCreateState(const std::string &key);

    // Recompute and apply the HWM for a leader partition.
    // topic:partition must match state.  Must NOT hold state.mu.
    void UpdateHWM(const std::string &topic, int32_t partition, PartitionReplicaState &state);

    // ── Background threads ───────────────────────────────────────────────────

    // Single maintenance thread: ISR trimming + starting new follower threads.
    void MaintenanceLoop();

    // One thread per followed partition.
    void FollowerLoop(std::string topic, int32_t partition);

    // Initialise follower tracking for leader partitions.
    void InitLeaderState();

    // ── Members ──────────────────────────────────────────────────────────────

    ClusterStore &cs_;
    TopicManager &tm_;
    std::chrono::milliseconds replica_lag_ms_;

    std::atomic<bool> running_{false};
    std::thread maintenance_thread_;

    // Per-partition follower states.  Key = "topic:partition".
    std::unordered_map<std::string, PartitionReplicaState> states_;
    std::mutex states_mu_;

    // Active follower threads.
    std::vector<std::thread> follower_threads_;
};
