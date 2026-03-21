#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

// ── BrokerInfo ──────────────────────────────────────────────────────────────
struct BrokerInfo
{
    int32_t id{0};
    std::string host;
    uint16_t port{0};
    int64_t last_heartbeat_ms{0}; // epoch ms
};

// ── PartitionAssignment ─────────────────────────────────────────────────────
struct PartitionAssignment
{
    std::string topic;
    int32_t partition{0};
    int32_t leader_id{-1}; // -1 = no leader elected
    int32_t leader_epoch{0};
    std::vector<int32_t> replicas; // ordered; first is preferred leader
    std::vector<int32_t> isr;      // in-sync replicas
};

// ── ClusterStore ────────────────────────────────────────────────────────────
//
// File-backed broker registry and partition-assignment store shared across
// broker processes via POSIX advisory locks (flock).
//
// Layout under cluster_dir:
//   state.lock        — flock target (never read)
//   brokers.txt       — one line per broker: "id host port last_heartbeat_ms"
//   assignments.txt   — one line per partition:
//                       "topic partition leader_id leader_epoch replicas_csv isr_csv"
//
// All public methods are thread-safe.  Modifications acquire both the
// in-process shared_mutex AND the flock so that concurrent broker processes
// do not clobber each other.
class ClusterStore
{
  public:
    explicit ClusterStore(std::filesystem::path cluster_dir, int32_t self_id);

    // Write self's (host, port) to the broker registry and stamp a heartbeat.
    void RegisterSelf(const std::string &host, uint16_t port);

    // Refresh the heartbeat timestamp for this broker without changing host/port.
    void SendHeartbeat();

    // Reload cluster state from disk into the in-process cache.
    void Reload();

    // ── Queries (lock-free after last Reload/Register) ───────────────────────

    std::optional<PartitionAssignment> GetAssignment(std::string_view topic, int32_t partition) const;
    std::vector<PartitionAssignment> LeaderAssignments() const;
    std::vector<PartitionAssignment> FollowerAssignments() const;
    bool IsLeader(std::string_view topic, int32_t partition) const;
    std::vector<BrokerInfo> GetBrokers() const;

    // ── Mutations ────────────────────────────────────────────────────────────

    // Write or overwrite a partition assignment; safe to call concurrently.
    void CommitAssignment(const PartitionAssignment &asgn);

    // Update the ISR list for a partition (must be called by current leader).
    void UpdateISR(std::string_view topic, int32_t partition, std::vector<int32_t> new_isr);

    // Create initial assignments for a new topic (self as leader for all partitions).
    // Picks replicas from the live broker list up to replication_factor.
    void AssignNewTopic(std::string_view topic_name, int num_partitions, int replication_factor, std::chrono::milliseconds broker_timeout);

    int32_t SelfId() const
    {
        return self_id_;
    }

  private:
    void LoadLocked();
    void SaveLocked() const;
    int AcquireLock() const;
    static void ReleaseLock(int fd);
    static int64_t NowMs();

    static std::vector<std::string> Split(const std::string &s, char delim);
    static std::vector<int32_t> ParseIntList(const std::string &s);
    static std::string JoinIntList(const std::vector<int32_t> &v);

    std::filesystem::path cluster_dir_;
    int32_t self_id_;

    mutable std::shared_mutex mu_;
    std::vector<BrokerInfo> brokers_;
    std::vector<PartitionAssignment> assignments_;
};
