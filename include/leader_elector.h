#pragma once

#include "cluster_store.h"
#include "replication_manager.h"
#include "topic_manager.h"

#include <atomic>
#include <chrono>
#include <thread>

// LeaderElector runs a background thread on every broker.
//
// Every check_interval milliseconds it:
//   1. Reloads the ClusterStore.
//   2. Sends a broker heartbeat so others know this broker is alive.
//   3. For each partition where the current leader's heartbeat is stale,
//      and where self is the first member of the ISR, attempts to atomically
//      claim leadership (double-check-after-lock to prevent split-brain).
class LeaderElector
{
  public:
    LeaderElector(ClusterStore &cs, TopicManager &tm, ReplicationManager &rm, std::chrono::milliseconds broker_timeout,
                  std::chrono::milliseconds check_interval = std::chrono::milliseconds(2000));
    ~LeaderElector();

    LeaderElector(const LeaderElector &) = delete;
    LeaderElector &operator=(const LeaderElector &) = delete;

    void Start();

  private:
    void ElectionLoop();

    ClusterStore &cs_;
    TopicManager &tm_;
    [[maybe_unused]] ReplicationManager &rm_;
    std::chrono::milliseconds broker_timeout_;
    std::chrono::milliseconds check_interval_;

    std::atomic<bool> running_{false};
    std::thread thread_;
};
