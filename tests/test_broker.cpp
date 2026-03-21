#include "codec.h"
#include "errors.h"
#include "group_coordinator.h"
#include "handler.h"
#include "protocol.h"
#include "server.h"
#include "topic_manager.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

// ─── Minimal test framework ───────────────────────────────────────────────────

static int g_passed = 0;
static int g_failed = 0;

#define RUN_TEST(fn)                                                                                                                       \
    do {                                                                                                                                   \
        printf("  %-55s", #fn " ...");                                                                                                     \
        fflush(stdout);                                                                                                                    \
        try {                                                                                                                              \
            fn();                                                                                                                          \
            printf("PASS\n");                                                                                                              \
            g_passed++;                                                                                                                    \
        } catch (const std::exception &_ex) {                                                                                              \
            printf("FAIL: %s\n", _ex.what());                                                                                              \
            g_failed++;                                                                                                                    \
        }                                                                                                                                  \
    } while (0)

#define CHECK(expr)                                                                                                                        \
    do {                                                                                                                                   \
        if (!(expr)) {                                                                                                                     \
            throw std::runtime_error("CHECK failed at line " + std::to_string(__LINE__) + ": " #expr);                                     \
        }                                                                                                                                  \
    } while (0)

#define CHECK_EQ(a, b)                                                                                                                     \
    do {                                                                                                                                   \
        auto _a = (a);                                                                                                                     \
        auto _b = (b);                                                                                                                     \
        if (_a != _b) {                                                                                                                    \
            throw std::runtime_error("CHECK_EQ failed at line " + std::to_string(__LINE__) + ": values differ");                           \
        }                                                                                                                                  \
    } while (0)

// ─── Network helpers ──────────────────────────────────────────────────────────

static int connect_to(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        throw std::runtime_error("socket failed");
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    struct sockaddr_in addr
    {
    };
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        close(fd);
        throw std::runtime_error("connect failed");
    }
    return fd;
}

static void send_all(int fd, const void *data, size_t n)
{
    const auto *p = static_cast<const uint8_t *>(data);
    while (n > 0) {
        ssize_t sent = send(fd, p, n, MSG_NOSIGNAL);
        if (sent <= 0)
            throw std::runtime_error("send failed");
        p += sent;
        n -= static_cast<size_t>(sent);
    }
}

static void recv_all(int fd, void *dst, size_t n)
{
    auto *p = static_cast<uint8_t *>(dst);
    while (n > 0) {
        ssize_t r = recv(fd, p, n, 0);
        if (r <= 0)
            throw std::runtime_error("recv failed / connection closed");
        p += r;
        n -= static_cast<size_t>(r);
    }
}

// Send a request frame with the given payload.
static void send_request(int fd, uint16_t api_key, uint32_t corr_id, const std::vector<uint8_t> &payload = {})
{
    RequestFrame req;
    req.api_key = api_key;
    req.api_version = 0;
    req.correlation_id = corr_id;
    req.payload = payload;
    auto encoded = EncodeRequest(req);
    send_all(fd, encoded.data(), encoded.size());
}

// Read one response frame and return payload bytes.
static std::pair<uint32_t, std::vector<uint8_t>> recv_response(int fd)
{
    uint8_t hdr[4];
    recv_all(fd, hdr, 4);
    uint32_t total_len = DecBe32(hdr);

    std::vector<uint8_t> body(total_len);
    recv_all(fd, body.data(), total_len);

    if (total_len < kRespBodyHdrSize)
        throw std::runtime_error("response too short");

    uint32_t corr_id = DecBe32(body.data());
    std::vector<uint8_t> payload(body.begin() + kRespBodyHdrSize, body.end());
    return {corr_id, std::move(payload)};
}

// ─── Payload builders ─────────────────────────────────────────────────────────

static std::vector<uint8_t> make_create_topic(const std::string &name, int32_t num_parts)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(name);
    w.WriteI32(num_parts);
    return body;
}

static std::vector<uint8_t> make_produce(const std::string &topic, int32_t part_id, const std::string &key, const std::string &value)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(topic);
    w.WriteI32(part_id);
    w.WriteU16(static_cast<uint16_t>(key.size()));
    w.WriteBytes(reinterpret_cast<const uint8_t *>(key.data()), key.size());
    w.WriteU32(static_cast<uint32_t>(value.size()));
    w.WriteBytes(reinterpret_cast<const uint8_t *>(value.data()), value.size());
    return body;
}

static std::vector<uint8_t> make_fetch(const std::string &topic, int32_t part_id, uint64_t fetch_offset, uint32_t max_bytes,
                                       uint32_t max_wait_ms)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(topic);
    w.WriteI32(part_id);
    w.WriteU64(fetch_offset);
    w.WriteU32(max_bytes);
    w.WriteU32(max_wait_ms);
    return body;
}

static std::vector<uint8_t> make_join_group(const std::string &group, const std::string &topic, const std::string &member)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(group);
    w.WriteString(topic);
    w.WriteString(member);
    return body;
}

// Build a SYNC_GROUP payload (assignments can be empty for followers).
static std::vector<uint8_t> make_sync_group(const std::string &group, int32_t gen_id, const std::string &member,
                                            const std::vector<std::pair<std::string, std::vector<int32_t>>> &assignments = {})
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(group);
    w.WriteI32(gen_id);
    w.WriteString(member);
    w.WriteU32(static_cast<uint32_t>(assignments.size()));
    for (const auto &[m, parts] : assignments) {
        w.WriteString(m);
        w.WriteU32(static_cast<uint32_t>(parts.size()));
        for (int32_t p : parts)
            w.WriteI32(p);
    }
    return body;
}

static std::vector<uint8_t> make_heartbeat(const std::string &group, int32_t gen_id, const std::string &member)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(group);
    w.WriteI32(gen_id);
    w.WriteString(member);
    return body;
}

static std::vector<uint8_t> make_offset_commit(const std::string &group, const std::string &topic, int32_t part_id, uint64_t offset)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(group);
    w.WriteString(topic);
    w.WriteI32(part_id);
    w.WriteU64(offset);
    return body;
}

static std::vector<uint8_t> make_offset_fetch(const std::string &group, const std::string &topic, int32_t part_id)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(group);
    w.WriteString(topic);
    w.WriteI32(part_id);
    return body;
}

// ─── BrokerServer fixture ─────────────────────────────────────────────────────

struct BrokerServer
{
    std::string data_dir;
    std::unique_ptr<TopicManager> tm;
    std::unique_ptr<GroupCoordinator> gc;
    std::unique_ptr<Server> server;
    std::shared_ptr<BrokerHandler> broker;
    std::thread thread;
    uint16_t port{0};

    BrokerServer()
    {
        // Unique temp directory per test instance.
        auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
        data_dir = (std::filesystem::temp_directory_path() / ("fluxmq_broker_test_" + std::to_string(ns))).string();
        std::filesystem::create_directories(data_dir);

        tm = std::make_unique<TopicManager>(data_dir);
        gc = std::make_unique<GroupCoordinator>();

        // Break circular dep with a deferred pointer.
        server = std::make_unique<Server>([](Connection &, RequestFrame) {});

        broker =
            std::make_shared<BrokerHandler>(*tm, *gc, [this](int fd, ResponseFrame resp) { server->PostResponse(fd, std::move(resp)); });

        // Recreate server with the real handler.
        server = std::make_unique<Server>([this](Connection &conn, RequestFrame frame) { broker->Handle(conn, frame); });

        thread = std::thread([this] { server->Run(0); });
        port = server->WaitReady();
    }

    ~BrokerServer()
    {
        server->Stop();
        thread.join();
        std::filesystem::remove_all(data_dir);
    }
};

// ─── Response decoders ───────────────────────────────────────────────────────

static int16_t read_error(const std::vector<uint8_t> &payload)
{
    BinaryReader r(payload);
    return r.ReadI16();
}

struct ProduceResp
{
    int16_t error;
    int32_t partition_id;
    uint64_t offset;
};
static ProduceResp decode_produce(const std::vector<uint8_t> &payload)
{
    BinaryReader r(payload);
    return {r.ReadI16(), r.ReadI32(), r.ReadU64()};
}

struct FetchRecord
{
    uint64_t offset;
    std::string value;
};
struct FetchResp
{
    int16_t error;
    std::vector<FetchRecord> records;
};
static FetchResp decode_fetch(const std::vector<uint8_t> &payload)
{
    BinaryReader r(payload);
    FetchResp resp;
    resp.error = r.ReadI16();
    uint32_t num_recs = r.ReadU32();
    for (uint32_t i = 0; i < num_recs; ++i) {
        FetchRecord rec;
        rec.offset = r.ReadU64();
        auto val = r.ReadBytes(r.ReadU32());
        rec.value = std::string(val.begin(), val.end());
        resp.records.push_back(std::move(rec));
    }
    return resp;
}

struct JoinResp
{
    int16_t error;
    int32_t gen_id;
    std::string leader;
    std::string member_id;
    std::vector<std::string> members;
};
static JoinResp decode_join(const std::vector<uint8_t> &payload)
{
    BinaryReader r(payload);
    JoinResp resp;
    resp.error = r.ReadI16();
    resp.gen_id = r.ReadI32();
    resp.leader = r.ReadString();
    resp.member_id = r.ReadString();
    uint32_t nm = r.ReadU32();
    for (uint32_t i = 0; i < nm; ++i)
        resp.members.push_back(r.ReadString());
    return resp;
}

struct SyncResp
{
    int16_t error;
    std::vector<int32_t> partitions;
};
static SyncResp decode_sync(const std::vector<uint8_t> &payload)
{
    BinaryReader r(payload);
    SyncResp resp;
    resp.error = r.ReadI16();
    uint32_t np = r.ReadU32();
    for (uint32_t i = 0; i < np; ++i)
        resp.partitions.push_back(r.ReadI32());
    return resp;
}

struct OffsetFetchResp
{
    int16_t error;
    uint64_t offset;
};
static OffsetFetchResp decode_offset_fetch(const std::vector<uint8_t> &payload)
{
    BinaryReader r(payload);
    return {r.ReadI16(), r.ReadU64()};
}

// ─── Tests ────────────────────────────────────────────────────────────────────

static void test_create_topic_and_metadata()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    // Create a topic with 3 partitions.
    send_request(fd, api::kCreateTopic, 1, make_create_topic("events", 3));
    auto [_, payload] = recv_response(fd);
    CHECK_EQ(read_error(payload), static_cast<int16_t>(0)); // err::kOk

    // Duplicate create returns kTopicAlreadyExists.
    send_request(fd, api::kCreateTopic, 2, make_create_topic("events", 3));
    auto [_2, payload2] = recv_response(fd);
    CHECK_EQ(read_error(payload2), static_cast<int16_t>(36)); // err::kTopicAlreadyExists

    // Metadata lists the topic.
    send_request(fd, api::kMetadata, 3);
    auto [_3, meta_payload] = recv_response(fd);
    BinaryReader mr(meta_payload);
    uint32_t num_topics = mr.ReadU32();
    CHECK_EQ(num_topics, 1U);
    std::string name = mr.ReadString();
    int32_t np = mr.ReadI32();
    CHECK_EQ(name, std::string("events"));
    CHECK_EQ(np, static_cast<int32_t>(3));

    close(fd);
}

static void test_produce_and_fetch_immediate()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("orders", 1));
    recv_response(fd);

    // Produce three messages.
    for (int i = 0; i < 3; ++i) {
        send_request(fd, api::kProduce, static_cast<uint32_t>(10 + i), make_produce("orders", 0, "", "msg-" + std::to_string(i)));
        auto [_, payload] = recv_response(fd);
        auto pr = decode_produce(payload);
        CHECK_EQ(pr.error, static_cast<int16_t>(0));
        CHECK_EQ(pr.offset, static_cast<uint64_t>(i));
    }

    // Fetch from offset 0, get all 3.
    send_request(fd, api::kFetch, 20, make_fetch("orders", 0, 0, 1024 * 1024, 0));
    auto [_, fetch_payload] = recv_response(fd);
    auto fr = decode_fetch(fetch_payload);
    CHECK_EQ(fr.error, static_cast<int16_t>(0));
    CHECK_EQ(fr.records.size(), 3U);
    CHECK_EQ(fr.records[0].value, std::string("msg-0"));
    CHECK_EQ(fr.records[2].value, std::string("msg-2"));

    // Fetch from offset 1.
    send_request(fd, api::kFetch, 21, make_fetch("orders", 0, 1, 1024, 0));
    auto [_2, fetch_payload2] = recv_response(fd);
    auto fr2 = decode_fetch(fetch_payload2);
    CHECK_EQ(fr2.records.size(), 2U);
    CHECK_EQ(fr2.records[0].offset, static_cast<uint64_t>(1));

    close(fd);
}

static void test_produce_key_routing()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("payments", 4));
    recv_response(fd);

    // Same key → same partition every time.
    int first_part = -1;
    for (int i = 0; i < 5; ++i) {
        send_request(fd, api::kProduce, static_cast<uint32_t>(i), make_produce("payments", -1, "user-42", "payload"));
        auto [_, payload] = recv_response(fd);
        auto pr = decode_produce(payload);
        CHECK_EQ(pr.error, static_cast<int16_t>(0));
        if (first_part < 0)
            first_part = pr.partition_id;
        else
            CHECK_EQ(pr.partition_id, first_part);
    }

    // Different key → (very likely) different partition.
    send_request(fd, api::kProduce, 99, make_produce("payments", -1, "user-99", "x"));
    auto [_, p99] = recv_response(fd);
    // At least confirm it's a valid partition id.
    auto pr99 = decode_produce(p99);
    CHECK(pr99.partition_id >= 0 && pr99.partition_id < 4);

    close(fd);
}

static void test_fetch_unknown_topic()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kFetch, 1, make_fetch("nonexistent", 0, 0, 1024, 0));
    auto [_, payload] = recv_response(fd);
    CHECK_EQ(read_error(payload), static_cast<int16_t>(3)); // err::kUnknownTopic

    close(fd);
}

static void test_long_poll_fetch()
{
    BrokerServer srv;

    // Consumer connects and issues a long-poll FETCH (max_wait_ms = 500).
    int consumer_fd = connect_to(srv.port);
    int producer_fd = connect_to(srv.port);

    send_request(producer_fd, api::kCreateTopic, 1, make_create_topic("stream", 1));
    recv_response(producer_fd);

    // Consumer fetches from offset 0 with a 500ms wait.
    send_request(consumer_fd, api::kFetch, 42, make_fetch("stream", 0, 0, 65536, 500));

    // Give the server a moment to register the pending fetch, then produce.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    send_request(producer_fd, api::kProduce, 2, make_produce("stream", 0, "", "hello-longpoll"));
    recv_response(producer_fd); // consume produce ack

    // The consumer should receive the response well before the 500ms deadline.
    auto t0 = std::chrono::steady_clock::now();
    auto [corr, payload] = recv_response(consumer_fd);
    double elapsed_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();

    CHECK_EQ(corr, static_cast<uint32_t>(42));
    auto fr = decode_fetch(payload);
    CHECK_EQ(fr.error, static_cast<int16_t>(0));
    CHECK_EQ(fr.records.size(), 1U);
    CHECK_EQ(fr.records[0].value, std::string("hello-longpoll"));
    // Should have arrived within ~400ms (well before the 500ms deadline).
    CHECK(elapsed_ms < 400.0);

    close(consumer_fd);
    close(producer_fd);
}

static void test_long_poll_timeout()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("empty_topic", 1));
    recv_response(fd);

    // Fetch with a short wait; nothing will be produced.
    auto t0 = std::chrono::steady_clock::now();
    send_request(fd, api::kFetch, 7, make_fetch("empty_topic", 0, 0, 1024, 100));
    auto [_, payload] = recv_response(fd);
    double elapsed_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();

    auto fr = decode_fetch(payload);
    CHECK_EQ(fr.error, static_cast<int16_t>(0));
    CHECK_EQ(fr.records.size(), 0U);
    // Should have waited roughly 100ms.
    CHECK(elapsed_ms >= 80.0 && elapsed_ms < 500.0);

    close(fd);
}

static void test_offset_commit_and_fetch()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("commits", 1));
    recv_response(fd);

    // Produce a few messages.
    for (int i = 0; i < 5; ++i) {
        send_request(fd, api::kProduce, static_cast<uint32_t>(i), make_produce("commits", 0, "", "v" + std::to_string(i)));
        recv_response(fd);
    }

    // Initial committed offset is 0.
    send_request(fd, api::kOffsetFetch, 10, make_offset_fetch("my-group", "commits", 0));
    auto [_, of_payload] = recv_response(fd);
    auto ofr = decode_offset_fetch(of_payload);
    CHECK_EQ(ofr.error, static_cast<int16_t>(0));
    CHECK_EQ(ofr.offset, static_cast<uint64_t>(0));

    // Commit offset 3.
    send_request(fd, api::kOffsetCommit, 11, make_offset_commit("my-group", "commits", 0, 3));
    auto [_2, oc_payload] = recv_response(fd);
    CHECK_EQ(read_error(oc_payload), static_cast<int16_t>(0));

    // Fetch offset returns 3.
    send_request(fd, api::kOffsetFetch, 12, make_offset_fetch("my-group", "commits", 0));
    auto [_3, of_payload2] = recv_response(fd);
    auto ofr2 = decode_offset_fetch(of_payload2);
    CHECK_EQ(ofr2.offset, static_cast<uint64_t>(3));

    close(fd);
}

// Helper: balanced range-assignment for N partitions over M members (sorted).
static std::vector<std::pair<std::string, std::vector<int32_t>>> balanced_assignments(const std::vector<std::string> &members,
                                                                                      int num_partitions)
{
    std::vector<std::pair<std::string, std::vector<int32_t>>> result;
    int n = static_cast<int>(members.size());
    for (int i = 0; i < n; ++i) {
        int start = (i * num_partitions) / n;
        int end = ((i + 1) * num_partitions) / n;
        std::vector<int32_t> parts;
        for (int p = start; p < end; ++p)
            parts.push_back(static_cast<int32_t>(p));
        result.emplace_back(members[i], std::move(parts));
    }
    return result;
}

static void test_consumer_group_single_member()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("topic1", 6));
    recv_response(fd);

    // C1 joins.
    send_request(fd, api::kJoinGroup, 2, make_join_group("g1", "topic1", "c1"));
    auto [_, j1_payload] = recv_response(fd);
    auto j1 = decode_join(j1_payload);
    CHECK_EQ(j1.error, static_cast<int16_t>(0));
    CHECK_EQ(j1.gen_id, static_cast<int32_t>(1));
    CHECK_EQ(j1.leader, std::string("c1"));
    CHECK_EQ(j1.member_id, std::string("c1"));
    // C1 is the leader — receives the member list.
    CHECK_EQ(j1.members.size(), 1U);

    // C1 syncs with all 6 partitions assigned to itself.
    auto asgn = balanced_assignments(j1.members, 6);
    send_request(fd, api::kSyncGroup, 3, make_sync_group("g1", j1.gen_id, "c1", asgn));
    auto [_2, s1_payload] = recv_response(fd);
    auto s1 = decode_sync(s1_payload);
    CHECK_EQ(s1.error, static_cast<int16_t>(0));
    CHECK_EQ(s1.partitions.size(), 6U);

    // Heartbeat succeeds.
    send_request(fd, api::kHeartbeat, 4, make_heartbeat("g1", j1.gen_id, "c1"));
    auto [_3, hb_payload] = recv_response(fd);
    CHECK_EQ(read_error(hb_payload), static_cast<int16_t>(0));

    close(fd);
}

static void test_consumer_group_rebalance()
{
    BrokerServer srv;
    int fd1 = connect_to(srv.port);
    int fd2 = connect_to(srv.port);

    send_request(fd1, api::kCreateTopic, 1, make_create_topic("topic2", 6));
    recv_response(fd1);

    // ── Step 1: C1 joins and becomes stable with 6 partitions ────────────────
    send_request(fd1, api::kJoinGroup, 2, make_join_group("g2", "topic2", "c1"));
    auto j1 = decode_join(recv_response(fd1).second);
    CHECK_EQ(j1.error, static_cast<int16_t>(0));
    CHECK_EQ(j1.gen_id, static_cast<int32_t>(1));

    auto asgn1 = balanced_assignments(j1.members, 6); // {c1: [0,1,2,3,4,5]}
    send_request(fd1, api::kSyncGroup, 3, make_sync_group("g2", j1.gen_id, "c1", asgn1));
    auto s1 = decode_sync(recv_response(fd1).second);
    CHECK_EQ(s1.error, static_cast<int16_t>(0));
    CHECK_EQ(s1.partitions.size(), 6U);

    // ── Step 2: C2 joins — triggers rebalance ─────────────────────────────────
    send_request(fd2, api::kJoinGroup, 4, make_join_group("g2", "topic2", "c2"));
    auto j2 = decode_join(recv_response(fd2).second);
    CHECK_EQ(j2.error, static_cast<int16_t>(0));
    CHECK_EQ(j2.gen_id, static_cast<int32_t>(2));

    // ── Step 3: C1 gets kRebalanceInProgress on its next heartbeat (gen=1) ───
    send_request(fd1, api::kHeartbeat, 5, make_heartbeat("g2", 1, "c1"));
    auto hb1 = read_error(recv_response(fd1).second);
    CHECK_EQ(hb1, static_cast<int16_t>(27)); // err::kRebalanceInProgress

    // ── Step 4: C1 re-joins for gen=2 ─────────────────────────────────────────
    send_request(fd1, api::kJoinGroup, 6, make_join_group("g2", "topic2", "c1"));
    auto j1b = decode_join(recv_response(fd1).second);
    CHECK_EQ(j1b.error, static_cast<int16_t>(0));
    CHECK_EQ(j1b.gen_id, static_cast<int32_t>(2));
    // C1 is leader (alphabetically "c1" < "c2"); leader sees both members.
    CHECK_EQ(j1b.leader, std::string("c1"));
    CHECK_EQ(j1b.members.size(), 2U);

    // ── Step 5: Leader (C1) syncs with balanced assignments ───────────────────
    auto asgn2 = balanced_assignments(j1b.members, 6); // {c1:[0,1,2], c2:[3,4,5]}
    send_request(fd1, api::kSyncGroup, 7, make_sync_group("g2", j1b.gen_id, "c1", asgn2));
    auto s1b = decode_sync(recv_response(fd1).second);
    CHECK_EQ(s1b.error, static_cast<int16_t>(0));
    CHECK_EQ(s1b.partitions.size(), 3U);

    // ── Step 6: C2 (follower) syncs and gets its partitions ───────────────────
    send_request(fd2, api::kSyncGroup, 8, make_sync_group("g2", j1b.gen_id, "c2"));
    auto s2 = decode_sync(recv_response(fd2).second);
    CHECK_EQ(s2.error, static_cast<int16_t>(0));
    CHECK_EQ(s2.partitions.size(), 3U);

    // Verify no overlap: C1 has {0,1,2}, C2 has {3,4,5}.
    for (int32_t p : s1b.partitions)
        CHECK(p >= 0 && p < 3);
    for (int32_t p : s2.partitions)
        CHECK(p >= 3 && p < 6);

    close(fd1);
    close(fd2);
}

static void test_consumer_group_three_members()
{
    BrokerServer srv;
    int fd1 = connect_to(srv.port);
    int fd2 = connect_to(srv.port);
    int fd3 = connect_to(srv.port);

    send_request(fd1, api::kCreateTopic, 1, make_create_topic("topic3", 6));
    recv_response(fd1);

    // ── C1 joins → stable with 6 partitions ─────────────────────────────────
    send_request(fd1, api::kJoinGroup, 2, make_join_group("g3", "topic3", "c1"));
    auto j1 = decode_join(recv_response(fd1).second);
    auto a1 = balanced_assignments(j1.members, 6);
    send_request(fd1, api::kSyncGroup, 3, make_sync_group("g3", j1.gen_id, "c1", a1));
    auto s1 = decode_sync(recv_response(fd1).second);
    CHECK_EQ(s1.partitions.size(), 6U);

    // ── C2 joins → rebalance ─────────────────────────────────────────────────
    send_request(fd2, api::kJoinGroup, 4, make_join_group("g3", "topic3", "c2"));
    auto j2 = decode_join(recv_response(fd2).second);
    CHECK_EQ(j2.gen_id, static_cast<int32_t>(2));

    // C1 re-joins.
    send_request(fd1, api::kHeartbeat, 5, make_heartbeat("g3", 1, "c1"));
    recv_response(fd1); // discard kRebalanceInProgress
    send_request(fd1, api::kJoinGroup, 6, make_join_group("g3", "topic3", "c1"));
    auto j1b = decode_join(recv_response(fd1).second);
    CHECK_EQ(j1b.gen_id, static_cast<int32_t>(2));

    // Leader (c1) syncs for 2 members.
    auto a12 = balanced_assignments(j1b.members, 6);
    send_request(fd1, api::kSyncGroup, 7, make_sync_group("g3", j1b.gen_id, "c1", a12));
    auto s1b = decode_sync(recv_response(fd1).second);
    CHECK_EQ(s1b.partitions.size(), 3U);

    send_request(fd2, api::kSyncGroup, 8, make_sync_group("g3", j1b.gen_id, "c2"));
    auto s2 = decode_sync(recv_response(fd2).second);
    CHECK_EQ(s2.partitions.size(), 3U);

    // ── C3 joins → rebalance again ────────────────────────────────────────────
    send_request(fd3, api::kJoinGroup, 9, make_join_group("g3", "topic3", "c3"));
    auto j3 = decode_join(recv_response(fd3).second);
    CHECK_EQ(j3.gen_id, static_cast<int32_t>(3));

    // C1 and C2 re-join.
    send_request(fd1, api::kHeartbeat, 10, make_heartbeat("g3", 2, "c1"));
    recv_response(fd1);
    send_request(fd1, api::kJoinGroup, 11, make_join_group("g3", "topic3", "c1"));
    auto j1c = decode_join(recv_response(fd1).second);

    send_request(fd2, api::kHeartbeat, 12, make_heartbeat("g3", 2, "c2"));
    recv_response(fd2);
    send_request(fd2, api::kJoinGroup, 13, make_join_group("g3", "topic3", "c2"));
    auto j2c = decode_join(recv_response(fd2).second);

    // If neither join response has the full list yet, the group transitioned to
    // SYNCING on c2's join (c2 is not the leader).  The leader (c1) must re-join
    // once more — in SYNCING state it receives the complete member list.
    if (j1c.members.size() != 3 && j2c.members.size() != 3) {
        send_request(fd1, api::kJoinGroup, 17, make_join_group("g3", "topic3", "c1"));
        j1c = decode_join(recv_response(fd1).second);
    }

    JoinResp *leader_resp = (j1c.members.size() == 3) ? &j1c : nullptr;
    if (!leader_resp && j2c.members.size() == 3)
        leader_resp = &j2c;
    CHECK(leader_resp != nullptr);
    CHECK_EQ(leader_resp->members.size(), 3U);

    // Leader syncs with 3-way balanced assignment.
    auto a123 = balanced_assignments(leader_resp->members, 6);
    send_request(fd1, api::kSyncGroup, 14, make_sync_group("g3", leader_resp->gen_id, "c1", a123));
    auto s1c = decode_sync(recv_response(fd1).second);
    CHECK_EQ(s1c.error, static_cast<int16_t>(0));
    CHECK_EQ(s1c.partitions.size(), 2U);

    send_request(fd2, api::kSyncGroup, 15, make_sync_group("g3", leader_resp->gen_id, "c2"));
    auto s2c = decode_sync(recv_response(fd2).second);
    CHECK_EQ(s2c.error, static_cast<int16_t>(0));
    CHECK_EQ(s2c.partitions.size(), 2U);

    send_request(fd3, api::kSyncGroup, 16, make_sync_group("g3", leader_resp->gen_id, "c3"));
    auto s3c = decode_sync(recv_response(fd3).second);
    CHECK_EQ(s3c.error, static_cast<int16_t>(0));
    CHECK_EQ(s3c.partitions.size(), 2U);

    // Total partitions = 6 (2 each), all distinct.
    std::set<int32_t> all;
    for (int32_t p : s1c.partitions)
        all.insert(p);
    for (int32_t p : s2c.partitions)
        all.insert(p);
    for (int32_t p : s3c.partitions)
        all.insert(p);
    CHECK_EQ(all.size(), 6U);

    close(fd1);
    close(fd2);
    close(fd3);
}

// ─── main ─────────────────────────────────────────────────────────────────────

int main()
{
    printf("=== FluxMQ Broker Tests ===\n\n");

    RUN_TEST(test_create_topic_and_metadata);
    RUN_TEST(test_produce_and_fetch_immediate);
    RUN_TEST(test_produce_key_routing);
    RUN_TEST(test_fetch_unknown_topic);
    RUN_TEST(test_long_poll_fetch);
    RUN_TEST(test_long_poll_timeout);
    RUN_TEST(test_offset_commit_and_fetch);
    RUN_TEST(test_consumer_group_single_member);
    RUN_TEST(test_consumer_group_rebalance);
    RUN_TEST(test_consumer_group_three_members);

    printf("\n%d passed, %d failed\n", g_passed, g_failed);
    return g_failed > 0 ? 1 : 0;
}
