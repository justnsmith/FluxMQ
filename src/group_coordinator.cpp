#include "group_coordinator.h"

#include <algorithm>
#include <thread>

using Clock = std::chrono::steady_clock;

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------

GroupCoordinator::GroupCoordinator(std::chrono::milliseconds session_timeout) : session_timeout_(session_timeout)
{
    reaper_thread_ = std::thread([this] { ReaperLoop(); });
}

GroupCoordinator::~GroupCoordinator()
{
    running_ = false;
    reaper_thread_.join();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

GroupCoordinator::GroupInfo &GroupCoordinator::GetOrCreate(std::string_view group_id, int num_partitions)
{
    std::string key(group_id);
    auto [it, inserted] = groups_.emplace(key, GroupInfo{});
    if (inserted)
        it->second.num_partitions = num_partitions;
    return it->second;
}

JoinGroupResult GroupCoordinator::MakeJoinResult(const GroupInfo &g, std::string_view member_id)
{
    JoinGroupResult r;
    r.error = err::kOk;
    r.generation_id = g.generation_id;
    r.member_id = std::string(member_id);
    if (!g.members.empty())
        r.leader = g.members.begin()->first;
    // Populate member list only for the leader (after reaching SYNCING).
    if (r.member_id == r.leader && g.state == GroupState::SYNCING) {
        for (const auto &[id, _] : g.members)
            r.members.push_back(id);
    }
    return r;
}

// ---------------------------------------------------------------------------
// Join
// ---------------------------------------------------------------------------

JoinGroupResult GroupCoordinator::Join(std::string_view group_id, std::string_view member_id, int num_topic_partitions)
{
    std::lock_guard lock(mu_);
    auto &g = GetOrCreate(group_id, num_topic_partitions);
    std::string mid(member_id);
    auto now = Clock::now();

    switch (g.state) {
    case GroupState::EMPTY:
        // First member ever.
        g.members[mid] = {now, {}};
        g.generation_id = 1;
        g.state = GroupState::SYNCING;
        g.rejoined.clear();
        g.pending_sync.clear();
        return MakeJoinResult(g, member_id);

    case GroupState::SYNCING:
        if (g.members.count(mid) == 0) {
            // New member arrives while syncing — restart rebalance.
            g.members[mid] = {now, {}};
            g.generation_id++;
            g.state = GroupState::REBALANCING;
            g.rejoined.clear();
            g.rejoined.insert(mid);
            g.pending_sync.clear();
        }
        else {
            g.members[mid].last_heartbeat = now;
        }
        return MakeJoinResult(g, member_id);

    case GroupState::STABLE:
        if (g.members.count(mid) == 0) {
            // New member joins a stable group — trigger rebalance.
            g.members[mid] = {now, {}};
            g.generation_id++;
            g.state = GroupState::REBALANCING;
            g.rejoined.clear();
            g.rejoined.insert(mid);
            g.pending_sync.clear();
        }
        else {
            // Existing member re-joins stable group (client confused?).
            // Treat as a rejoin that triggers a new rebalance cycle.
            g.members[mid].last_heartbeat = now;
            g.generation_id++;
            g.state = GroupState::REBALANCING;
            g.rejoined.clear();
            g.rejoined.insert(mid);
            g.pending_sync.clear();
        }
        return MakeJoinResult(g, member_id);

    case GroupState::REBALANCING:
        if (g.members.count(mid) == 0) {
            // Brand-new member arrives during an ongoing rebalance — restart.
            g.members[mid] = {now, {}};
            g.generation_id++;
            g.rejoined.clear();
            g.rejoined.insert(mid);
            g.pending_sync.clear();
        }
        else {
            // Existing member re-joining for the current generation.
            g.members[mid].last_heartbeat = now;
            g.rejoined.insert(mid);

            // Transition to SYNCING once all members have re-joined.
            if (g.rejoined.size() == g.members.size()) {
                g.state = GroupState::SYNCING;
                g.pending_sync.clear();
            }
        }
        return MakeJoinResult(g, member_id);
    }

    // Unreachable.
    JoinGroupResult err;
    err.error = err::kInvalidRequest;
    return err;
}

// ---------------------------------------------------------------------------
// Sync
// ---------------------------------------------------------------------------

SyncGroupResult GroupCoordinator::Sync(std::string_view group_id, int32_t generation_id, std::string_view member_id,
                                       const std::vector<std::pair<std::string, std::vector<int32_t>>> &assignments)
{
    std::lock_guard lock(mu_);
    auto it = groups_.find(std::string(group_id));
    if (it == groups_.end())
        return {err::kGroupNotFound, {}};

    auto &g = it->second;
    if (generation_id != g.generation_id)
        return {err::kIllegalGeneration, {}};

    // If the group is already STABLE (leader already synced), return stored assignment.
    if (g.state == GroupState::STABLE) {
        auto mit = g.members.find(std::string(member_id));
        if (mit == g.members.end())
            return {err::kUnknownMemberId, {}};
        return {err::kOk, mit->second.assigned_partitions};
    }

    if (g.state != GroupState::SYNCING)
        return {err::kRebalanceInProgress, {}};

    // Leader submits assignments.
    std::string mid(member_id);
    if (!g.members.empty() && mid == g.members.begin()->first && !assignments.empty()) {
        for (const auto &[m, parts] : assignments) {
            g.pending_sync[m] = parts;
            auto mit = g.members.find(m);
            if (mit != g.members.end())
                mit->second.assigned_partitions = parts;
        }
        g.state = GroupState::STABLE;
    }

    // Return this member's assignment (available once leader has synced).
    auto sit = g.pending_sync.find(mid);
    if (sit != g.pending_sync.end())
        return {err::kOk, sit->second};

    // Leader hasn't synced yet — follower must retry.
    return {err::kRebalanceInProgress, {}};
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

HeartbeatResult GroupCoordinator::Heartbeat(std::string_view group_id, int32_t generation_id, std::string_view member_id)
{
    std::lock_guard lock(mu_);
    auto it = groups_.find(std::string(group_id));
    if (it == groups_.end())
        return {err::kGroupNotFound};

    auto &g = it->second;

    // Old generation — rebalance is in progress; client must rejoin.
    if (generation_id < g.generation_id)
        return {err::kRebalanceInProgress};

    if (generation_id != g.generation_id)
        return {err::kIllegalGeneration};

    auto mit = g.members.find(std::string(member_id));
    if (mit == g.members.end())
        return {err::kUnknownMemberId};

    mit->second.last_heartbeat = Clock::now();

    // Ongoing rebalance — tell the client to rejoin.
    if (g.state == GroupState::REBALANCING)
        return {err::kRebalanceInProgress};

    return {err::kOk};
}

// ---------------------------------------------------------------------------
// Leave
// ---------------------------------------------------------------------------

int16_t GroupCoordinator::Leave(std::string_view group_id, std::string_view member_id)
{
    std::lock_guard lock(mu_);
    auto it = groups_.find(std::string(group_id));
    if (it == groups_.end())
        return err::kGroupNotFound;

    auto &g = it->second;
    auto mit = g.members.find(std::string(member_id));
    if (mit == g.members.end())
        return err::kUnknownMemberId;

    g.members.erase(mit);
    g.rejoined.erase(std::string(member_id));

    if (g.members.empty()) {
        g.state = GroupState::EMPTY;
        g.generation_id = 0;
        g.pending_sync.clear();
        g.rejoined.clear();
    }
    else {
        // Trigger rebalance without the departed member.
        g.generation_id++;
        g.state = GroupState::REBALANCING;
        g.rejoined.clear();
        g.pending_sync.clear();
    }
    return err::kOk;
}

// ---------------------------------------------------------------------------
// EvictMember  (called with mu_ held)
// ---------------------------------------------------------------------------

void GroupCoordinator::EvictMember(GroupInfo &g, const std::string &mid)
{
    g.members.erase(mid);
    g.rejoined.erase(mid);

    if (g.members.empty()) {
        g.state = GroupState::EMPTY;
        g.generation_id = 0;
        g.pending_sync.clear();
        g.rejoined.clear();
        return;
    }

    if (g.state == GroupState::REBALANCING) {
        // Evicting a member that hadn't rejoined yet may allow the remaining
        // members (who already rejoined) to transition to SYNCING.
        if (g.rejoined.size() == g.members.size()) {
            g.state = GroupState::SYNCING;
            g.pending_sync.clear();
        }
    }
    else {
        // STABLE or SYNCING: trigger a new rebalance.
        g.generation_id++;
        g.state = GroupState::REBALANCING;
        g.rejoined.clear();
        g.pending_sync.clear();
    }
}

// ---------------------------------------------------------------------------
// ReaperLoop  (background thread)
// ---------------------------------------------------------------------------

void GroupCoordinator::ReaperLoop()
{
    while (running_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (!running_)
            break;

        auto now = Clock::now();
        std::lock_guard lock(mu_);

        for (auto &[group_id, g] : groups_) {
            if (g.state == GroupState::EMPTY)
                continue;

            std::vector<std::string> to_evict;
            for (auto &[mid, info] : g.members) {
                if (now - info.last_heartbeat > session_timeout_)
                    to_evict.push_back(mid);
            }
            for (const auto &mid : to_evict)
                EvictMember(g, mid);
        }
    }
}
