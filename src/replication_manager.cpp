#include "replication_manager.h"

#include "errors.h"
#include "replica_client.h"

#include <algorithm>
#include <iostream>
#include <thread>

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------

ReplicationManager::ReplicationManager(ClusterStore &cs, TopicManager &tm, std::chrono::milliseconds replica_lag_ms)
    : cs_(cs), tm_(tm), replica_lag_ms_(replica_lag_ms)
{
}

ReplicationManager::~ReplicationManager()
{
    running_ = false;
    if (maintenance_thread_.joinable())
        maintenance_thread_.join();
    for (auto &t : follower_threads_)
        if (t.joinable())
            t.join();
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

void ReplicationManager::Start()
{
    running_ = true;
    InitLeaderState();

    // Start follower threads for partitions where we are a replica but not leader.
    for (const auto &asgn : cs_.FollowerAssignments()) {
        follower_threads_.emplace_back([this, asgn] { FollowerLoop(asgn.topic, asgn.partition); });
    }

    maintenance_thread_ = std::thread([this] { MaintenanceLoop(); });
}

// ---------------------------------------------------------------------------
// InitLeaderState
// ---------------------------------------------------------------------------

void ReplicationManager::InitLeaderState()
{
    for (const auto &asgn : cs_.LeaderAssignments()) {
        std::string key = asgn.topic + ":" + std::to_string(asgn.partition);
        PartitionReplicaState &state = GetOrCreateState(key);
        std::lock_guard lock(state.mu);

        // Initialise FollowerState for each non-self replica.
        state.followers.clear();
        for (int32_t rid : asgn.replicas) {
            if (rid == cs_.SelfId())
                continue;
            FollowerState fs;
            fs.broker_id = rid;
            fs.fetch_offset = 0;
            fs.last_fetch_time = SteadyClock::now();
            fs.in_isr = (std::find(asgn.isr.begin(), asgn.isr.end(), rid) != asgn.isr.end());
            state.followers.push_back(fs);
        }

        // When ISR = {leader only}, advance HWM immediately.
        bool leader_only_isr = (asgn.isr.size() == 1 && asgn.isr[0] == cs_.SelfId());
        if (leader_only_isr) {
            Topic *t = tm_.FindTopic(asgn.topic);
            if (t) {
                Partition &part = t->GetPartition(asgn.partition);
                part.SetHighWatermark(part.NextOffset());
            }
        }
        else {
            // Replicated: HWM starts at 0; follower fetch calls will advance it.
            Topic *t = tm_.FindTopic(asgn.topic);
            if (t)
                t->GetPartition(asgn.partition).SetHighWatermark(0);
        }
    }
}

// ---------------------------------------------------------------------------
// GetOrCreateState
// ---------------------------------------------------------------------------

ReplicationManager::PartitionReplicaState &ReplicationManager::GetOrCreateState(const std::string &key)
{
    std::lock_guard lock(states_mu_);
    return states_[key];
}

// ---------------------------------------------------------------------------
// OnFollowerFetch  (called from the request-handler thread)
// ---------------------------------------------------------------------------

void ReplicationManager::OnFollowerFetch(const std::string &topic, int32_t partition, int32_t follower_id, uint64_t follower_fetch_offset)
{
    std::string key = topic + ":" + std::to_string(partition);

    PartitionReplicaState *state = nullptr;
    {
        std::lock_guard lock(states_mu_);
        auto it = states_.find(key);
        if (it == states_.end())
            return;
        state = &it->second;
    }

    {
        std::lock_guard lock(state->mu);
        bool found = false;
        for (auto &f : state->followers) {
            if (f.broker_id == follower_id) {
                f.fetch_offset = follower_fetch_offset;
                f.last_fetch_time = SteadyClock::now();
                found = true;
                break;
            }
        }
        if (!found) {
            // New follower we haven't seen yet.
            FollowerState fs;
            fs.broker_id = follower_id;
            fs.fetch_offset = follower_fetch_offset;
            fs.last_fetch_time = SteadyClock::now();
            fs.in_isr = false;
            state->followers.push_back(fs);
        }
    }

    UpdateHWM(topic, partition, *state);
}

// ---------------------------------------------------------------------------
// UpdateHWM
// ---------------------------------------------------------------------------

void ReplicationManager::UpdateHWM(const std::string &topic, int32_t partition, PartitionReplicaState &state)
{
    Topic *t = tm_.FindTopic(topic);
    if (!t)
        return;
    Partition &part = t->GetPartition(partition);

    auto asgn = cs_.GetAssignment(topic, partition);
    if (!asgn)
        return;

    uint64_t leader_leo = part.NextOffset();
    uint64_t min_offset = leader_leo;

    {
        std::lock_guard lock(state.mu);
        for (const auto &f : state.followers) {
            bool in_isr = (std::find(asgn->isr.begin(), asgn->isr.end(), f.broker_id) != asgn->isr.end());
            if (in_isr)
                min_offset = std::min(min_offset, f.fetch_offset);
        }
    }

    part.SetHighWatermark(min_offset);
}

// ---------------------------------------------------------------------------
// MaintenanceLoop
// ---------------------------------------------------------------------------

void ReplicationManager::MaintenanceLoop()
{
    while (running_) {
        // Sleep 5 seconds between ISR maintenance passes.
        for (int i = 0; i < 50 && running_; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!running_)
            break;

        cs_.Reload();

        for (const auto &asgn : cs_.LeaderAssignments()) {
            std::string key = asgn.topic + ":" + std::to_string(asgn.partition);
            PartitionReplicaState &state = GetOrCreateState(key);

            Topic *t = tm_.FindTopic(asgn.topic);
            if (!t)
                continue;
            Partition &part = t->GetPartition(asgn.partition);
            uint64_t leader_leo = part.NextOffset();

            auto now = SteadyClock::now();
            std::vector<int32_t> new_isr = asgn.isr;
            bool isr_changed = false;

            {
                std::lock_guard lock(state.mu);
                for (auto &f : state.followers) {
                    bool lagging =
                        (now - f.last_fetch_time > replica_lag_ms_) || (leader_leo > f.fetch_offset && leader_leo - f.fetch_offset > 4000);

                    bool cur_in_isr = (std::find(new_isr.begin(), new_isr.end(), f.broker_id) != new_isr.end());

                    if (cur_in_isr && lagging) {
                        // Remove from ISR.
                        new_isr.erase(std::remove(new_isr.begin(), new_isr.end(), f.broker_id), new_isr.end());
                        f.in_isr = false;
                        isr_changed = true;
                        std::cerr << "ReplicationManager: broker " << f.broker_id << " removed from ISR of " << asgn.topic << "/"
                                  << asgn.partition << "\n";
                    }
                    else if (!cur_in_isr && !lagging && f.fetch_offset >= leader_leo) {
                        // Caught up — add back to ISR.
                        new_isr.push_back(f.broker_id);
                        f.in_isr = true;
                        isr_changed = true;
                        std::cerr << "ReplicationManager: broker " << f.broker_id << " added back to ISR of " << asgn.topic << "/"
                                  << asgn.partition << "\n";
                    }
                }
            }

            if (isr_changed)
                cs_.UpdateISR(asgn.topic, asgn.partition, new_isr);

            // Recompute HWM after possible ISR change.
            UpdateHWM(asgn.topic, asgn.partition, state);
        }
    }
}

// ---------------------------------------------------------------------------
// FollowerLoop
// ---------------------------------------------------------------------------

void ReplicationManager::FollowerLoop(std::string topic, int32_t partition)
{
    ReplicaClient client;

    while (running_) {
        // Refresh assignment.
        cs_.Reload();
        auto asgn = cs_.GetAssignment(topic, partition);
        if (!asgn) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        // If we became the leader, stop the follower loop.
        if (asgn->leader_id == cs_.SelfId()) {
            InitLeaderState(); // Re-init leader state for this partition.
            break;
        }

        // Find leader address.
        std::string leader_host;
        uint16_t leader_port = 0;
        for (const auto &b : cs_.GetBrokers()) {
            if (b.id == asgn->leader_id) {
                leader_host = b.host;
                leader_port = b.port;
                break;
            }
        }

        if (leader_host.empty() || leader_port == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        // Connect if necessary.
        if (!client.IsConnected()) {
            if (!client.Connect(leader_host, leader_port)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
        }

        // Ensure the topic exists locally (it may have been created on another broker).
        Topic *t = tm_.FindTopic(topic);
        if (!t) {
            // Determine partition count from the cluster store.
            int max_part = asgn->partition;
            for (const auto &a : cs_.AllAssignments())
                if (a.topic == topic && a.partition > max_part)
                    max_part = a.partition;
            tm_.CreateTopic(topic, max_part + 1); // idempotent: ignores kTopicAlreadyExists
            t = tm_.FindTopic(topic);
        }
        if (!t) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }
        Partition &part = t->GetPartition(partition);
        uint64_t fetch_offset = part.NextOffset();

        auto result = client.Fetch(cs_.SelfId(), topic, partition, fetch_offset, asgn->leader_epoch);

        if (result.error != 0) {
            if (result.error == err::kNotLeader || result.error == err::kFencedLeaderEpoch) {
                // Leader changed; reconnect.
                client.Disconnect();
            }
            else if (result.error == -1) {
                // Connection error.
                client.Disconnect();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        // Append all received records.
        for (const auto &rec : result.records) {
            part.Append(rec.value.data(), rec.value.size());
        }

        // Apply HWM from leader.
        if (result.high_watermark > 0)
            part.SetHighWatermark(result.high_watermark);

        // If no records arrived, back off briefly to avoid busy-spin.
        if (result.records.empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}
