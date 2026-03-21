#include "leader_elector.h"

#include <chrono>
#include <iostream>
#include <thread>

using Clock = std::chrono::system_clock;

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------

LeaderElector::LeaderElector(ClusterStore &cs, TopicManager &tm, ReplicationManager &rm, std::chrono::milliseconds broker_timeout,
                             std::chrono::milliseconds check_interval)
    : cs_(cs), tm_(tm), rm_(rm), broker_timeout_(broker_timeout), check_interval_(check_interval)
{
}

LeaderElector::~LeaderElector()
{
    running_ = false;
    if (thread_.joinable())
        thread_.join();
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

void LeaderElector::Start()
{
    running_ = true;
    thread_ = std::thread([this] { ElectionLoop(); });
}

// ---------------------------------------------------------------------------
// ElectionLoop
// ---------------------------------------------------------------------------

void LeaderElector::ElectionLoop()
{
    while (running_) {
        // Sleep in small increments so we can exit quickly on shutdown.
        for (int i = 0; i < 20 && running_; ++i)
            std::this_thread::sleep_for(check_interval_ / 20);
        if (!running_)
            break;

        // Refresh our heartbeat so other brokers know we're alive.
        cs_.SendHeartbeat();
        cs_.Reload();

        int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count();

        // Build a map of broker_id → last_heartbeat_ms for quick lookup.
        auto brokers = cs_.GetBrokers();
        auto broker_hb = [&](int32_t id) -> int64_t {
            for (const auto &b : brokers)
                if (b.id == id)
                    return b.last_heartbeat_ms;
            return 0;
        };

        for (const auto &asgn : cs_.LeaderAssignments()) {
            // We're already the leader; nothing to do.
            (void)asgn;
        }

        // Check partitions where the current leader may have died.
        // We need to iterate all assignments (not just our own).
        for (const auto &b : brokers) {
            // Skip self — already sent heartbeat.
            if (b.id == cs_.SelfId())
                continue;

            // If this broker is healthy, no need to take over its partitions.
            if (now_ms - b.last_heartbeat_ms <= broker_timeout_.count())
                continue;

            // Broker b appears dead.  Check each of its leader partitions.
            // We only know about partitions via GetBrokers/GetAssignment so we
            // scan all assignments stored in the ClusterStore.
            // Re-use FollowerAssignments as a proxy for "leader might be dead".
            for (const auto &asgn : cs_.FollowerAssignments()) {
                if (asgn.leader_id != b.id)
                    continue;

                // Is self the first ISR member that is not the dead broker?
                // (isr[0] is typically the current leader, which is dead here,
                //  so we skip it when looking for the highest-priority survivor.)
                int32_t first_survivor = -1;
                for (int32_t m : asgn.isr) {
                    if (m == b.id)
                        continue;
                    first_survivor = m;
                    break;
                }
                if (first_survivor != cs_.SelfId())
                    continue;

                // Attempt to take leadership.
                // Double-check: reload and re-examine under the file lock.
                cs_.Reload();
                auto fresh = cs_.GetAssignment(asgn.topic, asgn.partition);
                if (!fresh)
                    continue;

                // Verify the leader is still the dead broker.
                int64_t leader_hb = broker_hb(fresh->leader_id);
                if (fresh->leader_id == cs_.SelfId())
                    continue; // Someone else already elected us.
                if (now_ms - leader_hb <= broker_timeout_.count())
                    continue; // Leader recovered in the meantime.

                // Re-check: still the first surviving ISR member?
                int32_t fresh_survivor = -1;
                for (int32_t m : fresh->isr) {
                    if (m == fresh->leader_id)
                        continue;
                    fresh_survivor = m;
                    break;
                }
                if (fresh_survivor != cs_.SelfId())
                    continue; // Not first surviving ISR member anymore.

                // Claim leadership, evicting the dead broker from the ISR.
                PartitionAssignment new_asgn = *fresh;
                new_asgn.leader_id = cs_.SelfId();
                new_asgn.leader_epoch += 1;
                new_asgn.isr.erase(std::remove(new_asgn.isr.begin(), new_asgn.isr.end(), fresh->leader_id),
                                   new_asgn.isr.end());
                cs_.CommitAssignment(new_asgn);

                std::cerr << "LeaderElector: claimed leadership for " << asgn.topic << "/" << asgn.partition << " (epoch "
                          << new_asgn.leader_epoch << ")\n";

                // Initialise leader-side HWM for this partition.
                Topic *t = tm_.FindTopic(asgn.topic);
                if (t) {
                    Partition &part = t->GetPartition(asgn.partition);
                    // Set HWM to current log end (we trust our log is up-to-date
                    // since we were in the ISR).
                    part.SetHighWatermark(part.NextOffset());
                }
            }
        }
    }
}
