#include "handler.h"

#include "cluster_store.h"
#include "codec.h"
#include "errors.h"
#include "replication_manager.h"

#include <chrono>
#include <iostream>
#include <thread>

using Clock = std::chrono::steady_clock;

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------

BrokerHandler::BrokerHandler(TopicManager &tm, GroupCoordinator &gc, PostFn post_fn, ClusterStore *cs, ReplicationManager *rm,
                             int replication_factor, std::chrono::milliseconds broker_timeout)
    : tm_(tm), gc_(gc), post_fn_(std::move(post_fn)), cs_(cs), rm_(rm), replication_factor_(replication_factor),
      broker_timeout_(broker_timeout)
{
    bg_thread_ = std::thread([this] { BackgroundLoop(); });
}

BrokerHandler::~BrokerHandler()
{
    running_ = false;
    bg_thread_.join();
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

void BrokerHandler::Handle(Connection &conn, RequestFrame frame)
{
    try {
        switch (frame.api_key) {
        case api::kProduce:
            HandleProduce(conn, frame);
            break;
        case api::kFetch:
            HandleFetch(conn, frame);
            break;
        case api::kCreateTopic:
            HandleCreateTopic(conn, frame);
            break;
        case api::kMetadata:
            HandleMetadata(conn, frame);
            break;
        case api::kJoinGroup:
            HandleJoinGroup(conn, frame);
            break;
        case api::kSyncGroup:
            HandleSyncGroup(conn, frame);
            break;
        case api::kHeartbeat:
            HandleHeartbeat(conn, frame);
            break;
        case api::kOffsetCommit:
            HandleOffsetCommit(conn, frame);
            break;
        case api::kOffsetFetch:
            HandleOffsetFetch(conn, frame);
            break;
        case api::kLeaveGroup:
            HandleLeaveGroup(conn, frame);
            break;
        case api::kReplicaFetch:
            HandleReplicaFetch(conn, frame);
            break;
        case api::kLeaderEpoch:
            HandleLeaderEpoch(conn, frame);
            break;
        default: {
            // Unknown API key — return a generic error.
            std::vector<uint8_t> body;
            BinaryWriter w(body);
            w.WriteI16(err::kInvalidRequest);
            conn.SendResponse({frame.correlation_id, std::move(body)});
        }
        }
    } catch (const std::exception &ex) {
        std::cerr << "BrokerHandler: exception handling api_key=" << frame.api_key << ": " << ex.what() << "\n";
        std::vector<uint8_t> body;
        BinaryWriter w(body);
        w.WriteI16(err::kInvalidRequest);
        conn.SendResponse({frame.correlation_id, std::move(body)});
    }
}

// ---------------------------------------------------------------------------
// PRODUCE  request:  [2B topic_len][topic][4B partition_id (−1=auto)][2B key_len][key][4B val_len][val]
// PRODUCE  response: [2B error][4B partition_id][8B offset]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleProduce(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string topic = r.ReadString();
    int32_t part_id = r.ReadI32();
    auto key = r.ReadBytes(r.ReadU16());
    auto val = r.ReadBytes(r.ReadU32());

    std::vector<uint8_t> body;
    BinaryWriter w(body);

    Topic *t = tm_.FindTopic(topic);
    if (!t) {
        w.WriteI16(err::kUnknownTopic);
        w.WriteI32(0);
        w.WriteU64(0);
        conn.SendResponse({frame.correlation_id, std::move(body)});
        return;
    }

    // In cluster mode, verify we are the leader for the target partition.
    if (cs_) {
        int target_part = (part_id >= 0) ? part_id : 0; // for kNotLeader check, any partition works
        if (!cs_->IsLeader(topic, target_part)) {
            w.WriteI16(err::kNotLeader);
            w.WriteI32(0);
            w.WriteU64(0);
            conn.SendResponse({frame.correlation_id, std::move(body)});
            return;
        }
    }

    int actual_part;
    uint64_t offset;

    if (part_id < 0) {
        // Auto-assign partition.
        auto [pid, off] = t->Publish(val.data(), val.size(), key.empty() ? nullptr : key.data(), key.size());
        actual_part = pid;
        offset = off;
    }
    else {
        if (part_id >= t->NumPartitions()) {
            w.WriteI16(err::kInvalidPartition);
            w.WriteI32(0);
            w.WriteU64(0);
            conn.SendResponse({frame.correlation_id, std::move(body)});
            return;
        }
        offset = t->GetPartition(part_id).Append(val.data(), val.size());
        actual_part = part_id;
    }

    w.WriteI16(err::kOk);
    w.WriteI32(actual_part);
    w.WriteU64(offset);
    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// FETCH  request:  [2B topic_len][topic][4B partition_id][8B fetch_offset][4B max_bytes][4B max_wait_ms]
// FETCH  response: [2B error][4B num_records] + records: [8B offset][4B val_len][val...]
// ---------------------------------------------------------------------------

ResponseFrame BrokerHandler::BuildFetchResponse(uint32_t corr_id, const std::vector<Record> &records)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteI16(err::kOk);
    w.WriteU32(static_cast<uint32_t>(records.size()));
    for (const auto &rec : records) {
        w.WriteU64(rec.offset);
        w.WriteU32(static_cast<uint32_t>(rec.value.size()));
        w.WriteBytes(rec.value.data(), rec.value.size());
    }
    return {corr_id, std::move(body)};
}

void BrokerHandler::HandleFetch(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string topic = r.ReadString();
    int32_t part_id = r.ReadI32();
    uint64_t fetch_off = r.ReadU64();
    uint32_t max_bytes = r.ReadU32();
    uint32_t max_wait_ms = r.ReadU32();

    std::vector<uint8_t> err_body;
    BinaryWriter ew(err_body);

    Topic *t = tm_.FindTopic(topic);
    if (!t) {
        ew.WriteI16(err::kUnknownTopic);
        ew.WriteU32(0);
        conn.SendResponse({frame.correlation_id, std::move(err_body)});
        return;
    }
    if (part_id < 0 || part_id >= t->NumPartitions()) {
        ew.WriteI16(err::kInvalidPartition);
        ew.WriteU32(0);
        conn.SendResponse({frame.correlation_id, std::move(err_body)});
        return;
    }

    Partition &part = t->GetPartition(part_id);

    // If data is available (up to HWM), respond immediately.
    if (part.HighWatermark() > fetch_off) {
        auto records = part.ReadBatch(fetch_off, max_bytes);
        conn.SendResponse(BuildFetchResponse(frame.correlation_id, records));
        return;
    }

    // No data yet.  If the client wants to wait, register a long-poll entry.
    if (max_wait_ms > 0) {
        PendingFetch pf;
        pf.fd = conn.Fd();
        pf.corr_id = frame.correlation_id;
        pf.partition = &part;
        pf.fetch_offset = fetch_off;
        pf.max_bytes = max_bytes;
        pf.deadline = Clock::now() + std::chrono::milliseconds(max_wait_ms);
        std::lock_guard lock(pending_mu_);
        pending_fetches_.push_back(std::move(pf));
        // Do NOT call SendResponse — the background thread will do it.
        return;
    }

    // max_wait_ms == 0: return empty immediately.
    conn.SendResponse(BuildFetchResponse(frame.correlation_id, {}));
}

// ---------------------------------------------------------------------------
// CREATE_TOPIC  request:  [2B topic_len][topic][4B num_partitions]
// CREATE_TOPIC  response: [2B error]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleCreateTopic(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string topic = r.ReadString();
    int32_t num_parts = r.ReadI32();

    std::vector<uint8_t> body;
    BinaryWriter w(body);
    int16_t rc = tm_.CreateTopic(topic, num_parts);
    if (rc == err::kOk && cs_) {
        // Register partition assignments in the cluster store.
        cs_->AssignNewTopic(topic, num_parts, replication_factor_, broker_timeout_);
        // Initialise leader-side HWM and follower tracking.
        for (int p = 0; p < num_parts; ++p) {
            Topic *t = tm_.FindTopic(topic);
            if (!t)
                continue;
            auto asgn = cs_->GetAssignment(topic, p);
            if (!asgn)
                continue;
            Partition &part = t->GetPartition(p);
            if (asgn->leader_id == cs_->SelfId()) {
                // Leader: HWM tracks NextOffset for ISR-only case.
                if (asgn->isr.size() == 1)
                    part.SetHighWatermark(part.NextOffset());
                else
                    part.SetHighWatermark(0);
            }
        }
    }
    w.WriteI16(rc);
    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// METADATA  request:  (empty)
// METADATA  response: [4B num_topics] + topics: [2B name_len][name][4B num_partitions]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleMetadata(Connection &conn, const RequestFrame &frame)
{
    auto topics = tm_.ListTopics();
    std::vector<uint8_t> body;
    BinaryWriter w(body);

    if (frame.api_version >= 1 && cs_) {
        // Extended v1 response:
        // [4B num_brokers] per broker: [4B id][2B host][2B port]
        // [4B num_topics] per topic: [2B name][4B num_parts]
        //   per partition: [4B id][4B leader_id][4B leader_epoch]
        //                  [4B num_replicas][4B...] [4B num_isr][4B...]
        auto brokers = cs_->GetBrokers();
        w.WriteU32(static_cast<uint32_t>(brokers.size()));
        for (const auto &b : brokers) {
            w.WriteI32(b.id);
            w.WriteString(b.host);
            w.WriteU16(b.port);
        }
        w.WriteU32(static_cast<uint32_t>(topics.size()));
        for (const auto &[name, np] : topics) {
            w.WriteString(name);
            w.WriteU32(static_cast<uint32_t>(np));
            for (int p = 0; p < np; ++p) {
                auto asgn = cs_->GetAssignment(name, p);
                w.WriteI32(p);
                w.WriteI32(asgn ? asgn->leader_id : cs_->SelfId());
                w.WriteI32(asgn ? asgn->leader_epoch : 0);
                auto replicas = asgn ? asgn->replicas : std::vector<int32_t>{cs_->SelfId()};
                auto isr = asgn ? asgn->isr : std::vector<int32_t>{cs_->SelfId()};
                w.WriteU32(static_cast<uint32_t>(replicas.size()));
                for (int32_t r : replicas)
                    w.WriteI32(r);
                w.WriteU32(static_cast<uint32_t>(isr.size()));
                for (int32_t r : isr)
                    w.WriteI32(r);
            }
        }
    }
    else {
        // Legacy v0 response (backward-compatible).
        w.WriteU32(static_cast<uint32_t>(topics.size()));
        for (const auto &[name, np] : topics) {
            w.WriteString(name);
            w.WriteI32(np);
        }
    }

    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// JOIN_GROUP  request:  [2B group_len][group][2B topic_len][topic][2B member_len][member]
// JOIN_GROUP  response: [2B error][4B gen_id][2B leader_len][leader][2B member_len][member]
//                       [4B num_members (0 if not leader)] + members: [2B member_len][member]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleJoinGroup(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string group = r.ReadString();
    std::string topic = r.ReadString();
    std::string member = r.ReadString();

    int num_parts = 1;
    const Topic *t = tm_.FindTopic(topic);
    if (t)
        num_parts = t->NumPartitions();

    auto res = gc_.Join(group, member, num_parts);

    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteI16(res.error);
    w.WriteI32(res.generation_id);
    w.WriteString(res.leader);
    w.WriteString(res.member_id);
    w.WriteU32(static_cast<uint32_t>(res.members.size()));
    for (const auto &m : res.members)
        w.WriteString(m);
    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// SYNC_GROUP  request:  [2B group_len][group][4B gen_id][2B member_len][member]
//                       [4B num_assignments] + assignments: [2B member_len][member][4B num_parts][4B part_id...]
// SYNC_GROUP  response: [2B error][4B num_parts][4B part_id...]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleSyncGroup(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string group = r.ReadString();
    int32_t gen_id = r.ReadI32();
    std::string member = r.ReadString();
    uint32_t num_asgn = r.ReadU32();

    std::vector<std::pair<std::string, std::vector<int32_t>>> assignments;
    for (uint32_t i = 0; i < num_asgn; ++i) {
        std::string m = r.ReadString();
        uint32_t np = r.ReadU32();
        std::vector<int32_t> parts;
        parts.reserve(np);
        for (uint32_t j = 0; j < np; ++j)
            parts.push_back(r.ReadI32());
        assignments.emplace_back(std::move(m), std::move(parts));
    }

    auto res = gc_.Sync(group, gen_id, member, assignments);

    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteI16(res.error);
    w.WriteU32(static_cast<uint32_t>(res.partitions.size()));
    for (int32_t p : res.partitions)
        w.WriteI32(p);
    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// HEARTBEAT  request:  [2B group_len][group][4B gen_id][2B member_len][member]
// HEARTBEAT  response: [2B error]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleHeartbeat(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string group = r.ReadString();
    int32_t gen_id = r.ReadI32();
    std::string member = r.ReadString();

    auto res = gc_.Heartbeat(group, gen_id, member);

    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteI16(res.error);
    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// OFFSET_COMMIT  request:  [2B group_len][group][2B topic_len][topic][4B part_id][8B offset]
// OFFSET_COMMIT  response: [2B error]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleOffsetCommit(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string group = r.ReadString();
    std::string topic = r.ReadString();
    int32_t part_id = r.ReadI32();
    uint64_t offset = r.ReadU64();

    std::vector<uint8_t> body;
    BinaryWriter w(body);

    Topic *t = tm_.FindTopic(topic);
    if (!t) {
        w.WriteI16(err::kUnknownTopic);
        conn.SendResponse({frame.correlation_id, std::move(body)});
        return;
    }
    if (part_id < 0 || part_id >= t->NumPartitions()) {
        w.WriteI16(err::kInvalidPartition);
        conn.SendResponse({frame.correlation_id, std::move(body)});
        return;
    }
    t->GetPartition(part_id).CommitOffset(group, offset);
    w.WriteI16(err::kOk);
    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// OFFSET_FETCH  request:  [2B group_len][group][2B topic_len][topic][4B part_id]
// OFFSET_FETCH  response: [2B error][8B offset]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleOffsetFetch(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string group = r.ReadString();
    std::string topic = r.ReadString();
    int32_t part_id = r.ReadI32();

    std::vector<uint8_t> body;
    BinaryWriter w(body);

    Topic *t = tm_.FindTopic(topic);
    if (!t) {
        w.WriteI16(err::kUnknownTopic);
        w.WriteU64(0);
        conn.SendResponse({frame.correlation_id, std::move(body)});
        return;
    }
    if (part_id < 0 || part_id >= t->NumPartitions()) {
        w.WriteI16(err::kInvalidPartition);
        w.WriteU64(0);
        conn.SendResponse({frame.correlation_id, std::move(body)});
        return;
    }
    uint64_t off = t->GetPartition(part_id).FetchCommittedOffset(group);
    w.WriteI16(err::kOk);
    w.WriteU64(off);
    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// Background long-poll worker
// ---------------------------------------------------------------------------

void BrokerHandler::BackgroundLoop()
{
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        std::vector<PendingFetch> local;
        {
            std::lock_guard lock(pending_mu_);
            local.swap(pending_fetches_);
        }
        if (local.empty())
            continue;

        auto now = Clock::now();
        std::vector<PendingFetch> keep;

        for (auto &pf : local) {
            bool data_ready = pf.partition->HighWatermark() > pf.fetch_offset;
            bool timed_out = now >= pf.deadline;

            if (data_ready || timed_out) {
                auto records = data_ready ? pf.partition->ReadBatch(pf.fetch_offset, pf.max_bytes) : std::vector<Record>{};
                post_fn_(pf.fd, BuildFetchResponse(pf.corr_id, records));
            }
            else {
                keep.push_back(std::move(pf));
            }
        }

        if (!keep.empty()) {
            std::lock_guard lock(pending_mu_);
            for (auto &pf : keep)
                pending_fetches_.push_back(std::move(pf));
        }
    }
}

// ---------------------------------------------------------------------------
// LEAVE_GROUP  request:  [2B group_len][group][2B member_len][member]
// LEAVE_GROUP  response: [2B error]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleLeaveGroup(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string group = r.ReadString();
    std::string member = r.ReadString();

    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteI16(gc_.Leave(group, member));
    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// REPLICA_FETCH  (API key 10) — follower → leader pull replication
//
// request:  [4B follower_id][2B topic][4B partition][8B offset][4B max_bytes][4B leader_epoch]
// response: [2B error][4B leader_epoch][8B high_watermark][4B num_records] + records
// ---------------------------------------------------------------------------

void BrokerHandler::HandleReplicaFetch(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    int32_t follower_id = r.ReadI32();
    std::string topic = r.ReadString();
    int32_t part_id = r.ReadI32();
    uint64_t fetch_off = r.ReadU64();
    uint32_t max_bytes = r.ReadU32();
    int32_t req_epoch = r.ReadI32();

    std::vector<uint8_t> body;
    BinaryWriter w(body);

    // Verify we are the leader (only in cluster mode).
    if (cs_ && !cs_->IsLeader(topic, part_id)) {
        w.WriteI16(err::kNotLeader);
        w.WriteI32(0);
        w.WriteU64(0);
        w.WriteU32(0);
        conn.SendResponse({frame.correlation_id, std::move(body)});
        return;
    }

    // Epoch check.
    int32_t leader_epoch = 0;
    if (cs_) {
        auto asgn = cs_->GetAssignment(topic, part_id);
        if (asgn)
            leader_epoch = asgn->leader_epoch;
        if (req_epoch != 0 && req_epoch != leader_epoch) {
            w.WriteI16(err::kFencedLeaderEpoch);
            w.WriteI32(leader_epoch);
            w.WriteU64(0);
            w.WriteU32(0);
            conn.SendResponse({frame.correlation_id, std::move(body)});
            return;
        }
    }

    Topic *t = tm_.FindTopic(topic);
    if (!t) {
        w.WriteI16(err::kUnknownTopic);
        w.WriteI32(0);
        w.WriteU64(0);
        w.WriteU32(0);
        conn.SendResponse({frame.correlation_id, std::move(body)});
        return;
    }
    if (part_id < 0 || part_id >= t->NumPartitions()) {
        w.WriteI16(err::kInvalidPartition);
        w.WriteI32(0);
        w.WriteU64(0);
        w.WriteU32(0);
        conn.SendResponse({frame.correlation_id, std::move(body)});
        return;
    }

    Partition &part = t->GetPartition(part_id);
    uint64_t hwm = part.HighWatermark();

    // Notify ReplicationManager of follower progress; this may advance HWM.
    if (rm_)
        rm_->OnFollowerFetch(topic, part_id, follower_id, fetch_off);

    // Serve records up to NextOffset (not capped at HWM; followers need all data).
    auto records = part.ReadBatchForReplication(fetch_off, max_bytes);

    w.WriteI16(err::kOk);
    w.WriteI32(leader_epoch);
    w.WriteU64(hwm);
    w.WriteU32(static_cast<uint32_t>(records.size()));
    for (const auto &rec : records) {
        w.WriteU64(rec.offset);
        w.WriteU32(static_cast<uint32_t>(rec.value.size()));
        w.WriteBytes(rec.value.data(), rec.value.size());
    }
    conn.SendResponse({frame.correlation_id, std::move(body)});
}

// ---------------------------------------------------------------------------
// LEADER_EPOCH  (API key 11)
//
// request:  [2B topic][4B partition]
// response: [2B error][4B leader_epoch][4B leader_broker_id][8B log_end_offset]
// ---------------------------------------------------------------------------

void BrokerHandler::HandleLeaderEpoch(Connection &conn, const RequestFrame &frame)
{
    BinaryReader r(frame.payload);
    std::string topic = r.ReadString();
    int32_t part_id = r.ReadI32();

    std::vector<uint8_t> body;
    BinaryWriter w(body);

    int32_t leader_id = cs_ ? cs_->SelfId() : 0;
    int32_t leader_epoch = 0;
    uint64_t log_end = 0;

    if (cs_) {
        auto asgn = cs_->GetAssignment(topic, part_id);
        if (asgn) {
            leader_id = asgn->leader_id;
            leader_epoch = asgn->leader_epoch;
        }
    }

    Topic *t = tm_.FindTopic(topic);
    if (!t) {
        w.WriteI16(err::kUnknownTopic);
        w.WriteI32(0);
        w.WriteI32(0);
        w.WriteU64(0);
    }
    else if (part_id < 0 || part_id >= t->NumPartitions()) {
        w.WriteI16(err::kInvalidPartition);
        w.WriteI32(0);
        w.WriteI32(0);
        w.WriteU64(0);
    }
    else {
        log_end = t->GetPartition(part_id).NextOffset();
        w.WriteI16(err::kOk);
        w.WriteI32(leader_epoch);
        w.WriteI32(leader_id);
        w.WriteU64(log_end);
    }
    conn.SendResponse({frame.correlation_id, std::move(body)});
}
