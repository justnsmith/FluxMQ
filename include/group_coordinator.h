#pragma once

#include "errors.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// ── Result types ─────────────────────────────────────────────────────────────

struct JoinGroupResult
{
    int16_t error{0};
    int32_t generation_id{0};
    std::string leader;
    std::string member_id;
    std::vector<std::string> members; // non-empty only when caller is the leader
};

struct SyncGroupResult
{
    int16_t error{0};
    std::vector<int32_t> partitions;
};

struct HeartbeatResult
{
    int16_t error{0};
};

// ── GroupCoordinator ─────────────────────────────────────────────────────────
//
// State machine per group:
//
//   EMPTY ──► SYNCING ──► STABLE
//               ▲           │ (new member joins or member evicted)
//               │           ▼
//               └──── REBALANCING
//
// Join / Sync / Heartbeat are all non-blocking.  Clients receive
// kRebalanceInProgress when a rebalance is in progress and must retry.
//
// A background reaper thread evicts members whose last heartbeat exceeds
// session_timeout, triggering a rebalance for the remaining members.
class GroupCoordinator
{
  public:
    explicit GroupCoordinator(std::chrono::milliseconds session_timeout = std::chrono::milliseconds(10000));
    ~GroupCoordinator();

    GroupCoordinator(const GroupCoordinator &) = delete;
    GroupCoordinator &operator=(const GroupCoordinator &) = delete;

    // Add or re-add a member.  Triggers a rebalance when the membership changes.
    JoinGroupResult Join(std::string_view group_id, std::string_view member_id, int num_topic_partitions);

    // Submit (leader) or retrieve (follower) partition assignments.
    SyncGroupResult Sync(std::string_view group_id, int32_t generation_id, std::string_view member_id,
                         const std::vector<std::pair<std::string, std::vector<int32_t>>> &assignments);

    HeartbeatResult Heartbeat(std::string_view group_id, int32_t generation_id, std::string_view member_id);

    // Graceful leave — triggers an immediate rebalance for remaining members.
    // Returns kOk, kGroupNotFound, or kUnknownMemberId.
    int16_t Leave(std::string_view group_id, std::string_view member_id);

  private:
    enum class GroupState
    {
        EMPTY,
        SYNCING,
        STABLE,
        REBALANCING
    };

    struct MemberInfo
    {
        std::chrono::steady_clock::time_point last_heartbeat;
        std::vector<int32_t> assigned_partitions;
    };

    struct GroupInfo
    {
        GroupState state{GroupState::EMPTY};
        int32_t generation_id{0};
        int num_partitions{0};
        // std::map gives sorted iteration; leader = members.begin()->first.
        std::map<std::string, MemberInfo> members;
        std::set<std::string> rejoined; // members who have re-joined in REBALANCING
        // Assignments submitted by the leader during SYNCING.
        std::unordered_map<std::string, std::vector<int32_t>> pending_sync;
    };

    GroupInfo &GetOrCreate(std::string_view group_id, int num_partitions);
    static JoinGroupResult MakeJoinResult(const GroupInfo &g, std::string_view member_id);

    // Remove a member and update group state accordingly.
    // Called with mu_ held.
    void EvictMember(GroupInfo &g, const std::string &member_id);

    // Background thread: periodically scans for members whose heartbeat has
    // expired and evicts them.
    void ReaperLoop();

    std::mutex mu_;
    std::unordered_map<std::string, GroupInfo> groups_;

    std::chrono::milliseconds session_timeout_;
    std::atomic<bool> running_{true};
    std::thread reaper_thread_;
};
