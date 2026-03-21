#pragma once

#include "connection.h"
#include "group_coordinator.h"
#include "protocol.h"
#include "topic_manager.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

// Callback used by BrokerHandler to deliver async responses (e.g., long-poll FETCH).
// Called from a background thread; must be thread-safe.
using PostFn = std::function<void(int fd, ResponseFrame resp)>;

// BrokerHandler implements the full broker request-dispatch loop.
//
// It is designed to be wrapped in a lambda and passed as the Server's RequestHandler:
//
//   BrokerHandler bh(tm, gc, post_fn);
//   Server server([&bh](Connection& c, RequestFrame f){ bh.Handle(c, f); });
//
// Long-poll FETCH requests that find no data at fetch_offset are registered in
// pending_fetches_.  A background thread polls every ~10 ms and delivers
// responses via post_fn when data arrives or the deadline passes.
class BrokerHandler
{
  public:
    BrokerHandler(TopicManager &tm, GroupCoordinator &gc, PostFn post_fn);
    ~BrokerHandler();

    BrokerHandler(const BrokerHandler &) = delete;
    BrokerHandler &operator=(const BrokerHandler &) = delete;

    // Called from the reactor thread for each decoded request frame.
    void Handle(Connection &conn, RequestFrame frame);

  private:
    // Per-API handlers — all called from the reactor thread.
    void HandleProduce(Connection &conn, const RequestFrame &frame);
    void HandleFetch(Connection &conn, const RequestFrame &frame);
    void HandleCreateTopic(Connection &conn, const RequestFrame &frame);
    void HandleMetadata(Connection &conn, const RequestFrame &frame);
    void HandleJoinGroup(Connection &conn, const RequestFrame &frame);
    void HandleSyncGroup(Connection &conn, const RequestFrame &frame);
    void HandleHeartbeat(Connection &conn, const RequestFrame &frame);
    void HandleOffsetCommit(Connection &conn, const RequestFrame &frame);
    void HandleOffsetFetch(Connection &conn, const RequestFrame &frame);
    void HandleLeaveGroup(Connection &conn, const RequestFrame &frame);

    // Encode a FETCH response from a vector of records.
    static ResponseFrame BuildFetchResponse(uint32_t corr_id, const std::vector<Record> &records);

    // Background long-poll worker.
    void BackgroundLoop();

    struct PendingFetch
    {
        int fd;
        uint32_t corr_id;
        Partition *partition;
        uint64_t fetch_offset;
        uint32_t max_bytes;
        std::chrono::steady_clock::time_point deadline;
    };

    TopicManager &tm_;
    GroupCoordinator &gc_;
    PostFn post_fn_;

    std::mutex pending_mu_;
    std::vector<PendingFetch> pending_fetches_;

    std::atomic<bool> running_{true};
    std::thread bg_thread_;
};
