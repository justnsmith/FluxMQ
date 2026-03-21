#pragma once

#include "errors.h"

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
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
//               ▲           │ (new member joins)
//               │           ▼
//               └──── REBALANCING
//
// Join / Sync / Heartbeat are all non-blocking.  Clients receive
// kRebalanceInProgress when a rebalance is in progress and must retry.
class GroupCoordinator
{
  public:
    GroupCoordinator() = default;

    // Add or re-add a member.  Triggers a rebalance when the membership changes.
    // num_topic_partitions is used to compute balanced assignments.
    JoinGroupResult Join(std::string_view group_id, std::string_view member_id, int num_topic_partitions);

    // Submit (leader) or retrieve (follower) partition assignments.
    // Leader passes non-empty assignments; followers pass an empty vector.
    SyncGroupResult Sync(std::string_view group_id, int32_t generation_id, std::string_view member_id,
                         const std::vector<std::pair<std::string, std::vector<int32_t>>> &assignments);

    HeartbeatResult Heartbeat(std::string_view group_id, int32_t generation_id, std::string_view member_id);

    // Returns kOk or kGroupNotFound / kUnknownMemberId.
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

    std::mutex mu_;
    std::unordered_map<std::string, GroupInfo> groups_;
};
