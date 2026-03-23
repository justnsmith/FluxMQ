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

// ─── Additional payload builders ──────────────────────────────────────────────

static std::vector<uint8_t> make_leave_group(const std::string &group, const std::string &member)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(group);
    w.WriteString(member);
    return body;
}

// ─── Error handling & edge case tests ─────────────────────────────────────────

static void test_produce_unknown_topic()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kProduce, 1, make_produce("nonexistent", 0, "", "data"));
    auto [_, payload] = recv_response(fd);
    CHECK_EQ(read_error(payload), static_cast<int16_t>(3)); // err::kUnknownTopic

    close(fd);
}

static void test_produce_invalid_partition()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("part-test", 2));
    recv_response(fd);

    // Partition 99 does not exist (only 0 and 1).
    send_request(fd, api::kProduce, 2, make_produce("part-test", 99, "", "data"));
    auto [_, payload] = recv_response(fd);
    CHECK_EQ(read_error(payload), static_cast<int16_t>(10)); // err::kInvalidPartition

    close(fd);
}

static void test_fetch_invalid_partition()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("fetch-part", 2));
    recv_response(fd);

    send_request(fd, api::kFetch, 2, make_fetch("fetch-part", 99, 0, 1024, 0));
    auto [_, payload] = recv_response(fd);
    CHECK_EQ(read_error(payload), static_cast<int16_t>(10)); // err::kInvalidPartition

    close(fd);
}

static void test_unknown_api_key()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    // Send a request with an unknown API key (255).
    send_request(fd, 255, 1);
    auto [_, payload] = recv_response(fd);
    CHECK_EQ(read_error(payload), static_cast<int16_t>(42)); // err::kInvalidRequest

    close(fd);
}

static void test_empty_key_produce_fetch()
{
    // Produce with an empty key but non-empty value, verify roundtrip.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("empty-key", 1));
    recv_response(fd);

    send_request(fd, api::kProduce, 2, make_produce("empty-key", 0, "", "value-only"));
    auto [_, payload] = recv_response(fd);
    auto pr = decode_produce(payload);
    CHECK_EQ(pr.error, static_cast<int16_t>(0));
    CHECK_EQ(pr.offset, static_cast<uint64_t>(0));

    // Fetch it back.
    send_request(fd, api::kFetch, 3, make_fetch("empty-key", 0, 0, 1024, 0));
    auto [_2, fetch_payload] = recv_response(fd);
    auto fr = decode_fetch(fetch_payload);
    CHECK_EQ(fr.error, static_cast<int16_t>(0));
    CHECK_EQ(fr.records.size(), 1U);
    CHECK_EQ(fr.records[0].value, std::string("value-only"));

    close(fd);
}

static void test_large_message_produce_fetch()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("large-msg", 1));
    recv_response(fd);

    // Produce a 64KB message.
    std::string big_value(64 * 1024, 'X');
    send_request(fd, api::kProduce, 2, make_produce("large-msg", 0, "", big_value));
    auto [_, payload] = recv_response(fd);
    auto pr = decode_produce(payload);
    CHECK_EQ(pr.error, static_cast<int16_t>(0));

    // Fetch it back with sufficient max_bytes.
    send_request(fd, api::kFetch, 3, make_fetch("large-msg", 0, 0, 256 * 1024, 0));
    auto [_2, fetch_payload] = recv_response(fd);
    auto fr = decode_fetch(fetch_payload);
    CHECK_EQ(fr.error, static_cast<int16_t>(0));
    CHECK_EQ(fr.records.size(), 1U);
    CHECK_EQ(fr.records[0].value.size(), big_value.size());

    close(fd);
}

static void test_multiple_topics_isolation()
{
    // Data produced to one topic must not leak into another.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("topic-a", 1));
    recv_response(fd);
    send_request(fd, api::kCreateTopic, 2, make_create_topic("topic-b", 1));
    recv_response(fd);

    // Produce to topic-a only.
    send_request(fd, api::kProduce, 3, make_produce("topic-a", 0, "", "a-data"));
    recv_response(fd);

    // Fetch from topic-b — must be empty.
    send_request(fd, api::kFetch, 4, make_fetch("topic-b", 0, 0, 1024, 0));
    auto [_, payload] = recv_response(fd);
    auto fr = decode_fetch(payload);
    CHECK_EQ(fr.error, static_cast<int16_t>(0));
    CHECK_EQ(fr.records.size(), 0U);

    // Fetch from topic-a — must have the record.
    send_request(fd, api::kFetch, 5, make_fetch("topic-a", 0, 0, 1024, 0));
    auto [_2, payload2] = recv_response(fd);
    auto fr2 = decode_fetch(payload2);
    CHECK_EQ(fr2.records.size(), 1U);
    CHECK_EQ(fr2.records[0].value, std::string("a-data"));

    close(fd);
}

static void test_fetch_beyond_log_end()
{
    // Fetching at an offset beyond what's been produced returns 0 records.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("beyond", 1));
    recv_response(fd);

    send_request(fd, api::kProduce, 2, make_produce("beyond", 0, "", "only-one"));
    recv_response(fd);

    // Fetch from offset 100 — nothing there.
    send_request(fd, api::kFetch, 3, make_fetch("beyond", 0, 100, 1024, 0));
    auto [_, payload] = recv_response(fd);
    auto fr = decode_fetch(payload);
    CHECK_EQ(fr.error, static_cast<int16_t>(0));
    CHECK_EQ(fr.records.size(), 0U);

    close(fd);
}

static void test_leave_group_basic()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("leave-t", 4));
    recv_response(fd);

    // Join group.
    send_request(fd, api::kJoinGroup, 2, make_join_group("lg", "leave-t", "m1"));
    auto j = decode_join(recv_response(fd).second);
    CHECK_EQ(j.error, static_cast<int16_t>(0));

    // Sync to stabilize.
    auto asgn = balanced_assignments({"m1"}, 4);
    send_request(fd, api::kSyncGroup, 3, make_sync_group("lg", j.gen_id, "m1", asgn));
    auto s = decode_sync(recv_response(fd).second);
    CHECK_EQ(s.error, static_cast<int16_t>(0));

    // Leave.
    send_request(fd, api::kLeaveGroup, 4, make_leave_group("lg", "m1"));
    auto [_, lp] = recv_response(fd);
    CHECK_EQ(read_error(lp), static_cast<int16_t>(0)); // err::kOk

    close(fd);
}

static void test_leave_unknown_group()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kLeaveGroup, 1, make_leave_group("no-such-group", "m1"));
    auto [_, payload] = recv_response(fd);
    CHECK_EQ(read_error(payload), static_cast<int16_t>(16)); // err::kGroupNotFound

    close(fd);
}

static void test_leave_unknown_member()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("leave-m", 2));
    recv_response(fd);

    // Create group with m1.
    send_request(fd, api::kJoinGroup, 2, make_join_group("lm", "leave-m", "m1"));
    recv_response(fd);

    // Try to leave with unknown member.
    send_request(fd, api::kLeaveGroup, 3, make_leave_group("lm", "ghost"));
    auto [_, payload] = recv_response(fd);
    CHECK_EQ(read_error(payload), static_cast<int16_t>(25)); // err::kUnknownMemberId

    close(fd);
}

static void test_leave_group_triggers_rebalance()
{
    // When one of two members leaves, the remaining member should be notified
    // of a rebalance via kRebalanceInProgress on its next heartbeat.
    BrokerServer srv;
    int fd1 = connect_to(srv.port);
    int fd2 = connect_to(srv.port);

    send_request(fd1, api::kCreateTopic, 1, make_create_topic("leave-reb", 4));
    recv_response(fd1);

    // C1 joins and stabilizes.
    send_request(fd1, api::kJoinGroup, 2, make_join_group("lr", "leave-reb", "c1"));
    auto j1 = decode_join(recv_response(fd1).second);
    auto a1 = balanced_assignments(j1.members, 4);
    send_request(fd1, api::kSyncGroup, 3, make_sync_group("lr", j1.gen_id, "c1", a1));
    decode_sync(recv_response(fd1).second);

    // C2 joins → triggers rebalance.
    send_request(fd2, api::kJoinGroup, 4, make_join_group("lr", "leave-reb", "c2"));
    auto j2 = decode_join(recv_response(fd2).second);

    // C1 re-joins.
    send_request(fd1, api::kHeartbeat, 5, make_heartbeat("lr", 1, "c1"));
    recv_response(fd1); // kRebalanceInProgress
    send_request(fd1, api::kJoinGroup, 6, make_join_group("lr", "leave-reb", "c1"));
    auto j1b = decode_join(recv_response(fd1).second);

    // Sync both.
    auto a2 = balanced_assignments(j1b.members, 4);
    send_request(fd1, api::kSyncGroup, 7, make_sync_group("lr", j1b.gen_id, "c1", a2));
    decode_sync(recv_response(fd1).second);
    send_request(fd2, api::kSyncGroup, 8, make_sync_group("lr", j1b.gen_id, "c2"));
    decode_sync(recv_response(fd2).second);

    // Now C2 leaves gracefully.
    send_request(fd2, api::kLeaveGroup, 9, make_leave_group("lr", "c2"));
    auto [_, lp] = recv_response(fd2);
    CHECK_EQ(read_error(lp), static_cast<int16_t>(0));

    // C1's next heartbeat should get kRebalanceInProgress.
    send_request(fd1, api::kHeartbeat, 10, make_heartbeat("lr", j1b.gen_id, "c1"));
    auto hb = read_error(recv_response(fd1).second);
    CHECK_EQ(hb, static_cast<int16_t>(27)); // err::kRebalanceInProgress

    close(fd1);
    close(fd2);
}

static void test_heartbeat_wrong_generation()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("hb-gen", 2));
    recv_response(fd);

    send_request(fd, api::kJoinGroup, 2, make_join_group("hg", "hb-gen", "m1"));
    auto j = decode_join(recv_response(fd).second);
    auto a = balanced_assignments(j.members, 2);
    send_request(fd, api::kSyncGroup, 3, make_sync_group("hg", j.gen_id, "m1", a));
    decode_sync(recv_response(fd).second);

    // Heartbeat with wrong generation (gen_id + 100).
    send_request(fd, api::kHeartbeat, 4, make_heartbeat("hg", j.gen_id + 100, "m1"));
    auto hb = read_error(recv_response(fd).second);
    CHECK_EQ(hb, static_cast<int16_t>(22)); // err::kIllegalGeneration

    close(fd);
}

static void test_heartbeat_unknown_member()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("hb-unk", 2));
    recv_response(fd);

    send_request(fd, api::kJoinGroup, 2, make_join_group("hu", "hb-unk", "m1"));
    auto j = decode_join(recv_response(fd).second);
    auto a = balanced_assignments(j.members, 2);
    send_request(fd, api::kSyncGroup, 3, make_sync_group("hu", j.gen_id, "m1", a));
    decode_sync(recv_response(fd).second);

    // Heartbeat from an unknown member.
    send_request(fd, api::kHeartbeat, 4, make_heartbeat("hu", j.gen_id, "ghost"));
    auto hb = read_error(recv_response(fd).second);
    CHECK_EQ(hb, static_cast<int16_t>(25)); // err::kUnknownMemberId

    close(fd);
}

static void test_heartbeat_unknown_group()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kHeartbeat, 1, make_heartbeat("nonexistent-group", 1, "m1"));
    auto hb = read_error(recv_response(fd).second);
    CHECK_EQ(hb, static_cast<int16_t>(16)); // err::kGroupNotFound

    close(fd);
}

static void test_multiple_consumer_groups_same_topic()
{
    // Two independent consumer groups on the same topic. Each group's offsets
    // and membership are isolated.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("shared-topic", 4));
    recv_response(fd);

    // Produce some data.
    for (int i = 0; i < 5; i++) {
        send_request(fd, api::kProduce, static_cast<uint32_t>(10 + i), make_produce("shared-topic", 0, "", "val-" + std::to_string(i)));
        recv_response(fd);
    }

    // Group A commits offset 3.
    send_request(fd, api::kOffsetCommit, 20, make_offset_commit("group-a", "shared-topic", 0, 3));
    auto [_, oc1] = recv_response(fd);
    CHECK_EQ(read_error(oc1), static_cast<int16_t>(0));

    // Group B commits offset 1.
    send_request(fd, api::kOffsetCommit, 21, make_offset_commit("group-b", "shared-topic", 0, 1));
    auto [_2, oc2] = recv_response(fd);
    CHECK_EQ(read_error(oc2), static_cast<int16_t>(0));

    // Verify group A's offset is 3.
    send_request(fd, api::kOffsetFetch, 22, make_offset_fetch("group-a", "shared-topic", 0));
    auto ofa = decode_offset_fetch(recv_response(fd).second);
    CHECK_EQ(ofa.offset, static_cast<uint64_t>(3));

    // Verify group B's offset is 1.
    send_request(fd, api::kOffsetFetch, 23, make_offset_fetch("group-b", "shared-topic", 0));
    auto ofb = decode_offset_fetch(recv_response(fd).second);
    CHECK_EQ(ofb.offset, static_cast<uint64_t>(1));

    close(fd);
}

static void test_produce_multiple_partitions()
{
    // Produce to specific partitions and verify data lands in the right place.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("multi-part", 3));
    recv_response(fd);

    // Write distinct data to each partition.
    for (int p = 0; p < 3; p++) {
        send_request(fd, api::kProduce, static_cast<uint32_t>(10 + p), make_produce("multi-part", p, "", "part-" + std::to_string(p)));
        auto [_, payload] = recv_response(fd);
        auto pr = decode_produce(payload);
        CHECK_EQ(pr.error, static_cast<int16_t>(0));
        CHECK_EQ(pr.partition_id, static_cast<int32_t>(p));
        CHECK_EQ(pr.offset, static_cast<uint64_t>(0)); // first record in each partition
    }

    // Fetch from each partition.
    for (int p = 0; p < 3; p++) {
        send_request(fd, api::kFetch, static_cast<uint32_t>(20 + p), make_fetch("multi-part", p, 0, 1024, 0));
        auto [_, payload] = recv_response(fd);
        auto fr = decode_fetch(payload);
        CHECK_EQ(fr.error, static_cast<int16_t>(0));
        CHECK_EQ(fr.records.size(), 1U);
        CHECK_EQ(fr.records[0].value, std::string("part-" + std::to_string(p)));
    }

    close(fd);
}

static void test_high_volume_produce_fetch()
{
    // Produce 1000 messages and verify all are fetchable.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("high-vol", 1));
    recv_response(fd);

    const int N = 1000;
    for (int i = 0; i < N; i++) {
        send_request(fd, api::kProduce, static_cast<uint32_t>(i), make_produce("high-vol", 0, "", "msg-" + std::to_string(i)));
        auto [_, payload] = recv_response(fd);
        auto pr = decode_produce(payload);
        CHECK_EQ(pr.error, static_cast<int16_t>(0));
        CHECK_EQ(pr.offset, static_cast<uint64_t>(i));
    }

    // Fetch all at once.
    send_request(fd, api::kFetch, 9999, make_fetch("high-vol", 0, 0, 10 * 1024 * 1024, 0));
    auto [_, payload] = recv_response(fd);
    auto fr = decode_fetch(payload);
    CHECK_EQ(fr.error, static_cast<int16_t>(0));
    CHECK_EQ(fr.records.size(), static_cast<size_t>(N));
    CHECK_EQ(fr.records[0].value, std::string("msg-0"));
    CHECK_EQ(fr.records[N - 1].value, std::string("msg-" + std::to_string(N - 1)));

    close(fd);
}

static void test_offset_commit_idempotent()
{
    // Committing the same offset twice should succeed both times.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("idem", 1));
    recv_response(fd);

    send_request(fd, api::kOffsetCommit, 2, make_offset_commit("grp", "idem", 0, 5));
    CHECK_EQ(read_error(recv_response(fd).second), static_cast<int16_t>(0));

    send_request(fd, api::kOffsetCommit, 3, make_offset_commit("grp", "idem", 0, 5));
    CHECK_EQ(read_error(recv_response(fd).second), static_cast<int16_t>(0));

    send_request(fd, api::kOffsetFetch, 4, make_offset_fetch("grp", "idem", 0));
    auto of = decode_offset_fetch(recv_response(fd).second);
    CHECK_EQ(of.offset, static_cast<uint64_t>(5));

    close(fd);
}

static void test_offset_overwrite()
{
    // Committing a higher offset overwrites the previous one.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("overwrite", 1));
    recv_response(fd);

    send_request(fd, api::kOffsetCommit, 2, make_offset_commit("og", "overwrite", 0, 2));
    recv_response(fd);
    send_request(fd, api::kOffsetCommit, 3, make_offset_commit("og", "overwrite", 0, 7));
    recv_response(fd);

    send_request(fd, api::kOffsetFetch, 4, make_offset_fetch("og", "overwrite", 0));
    auto of = decode_offset_fetch(recv_response(fd).second);
    CHECK_EQ(of.offset, static_cast<uint64_t>(7));

    close(fd);
}

static void test_metadata_multiple_topics()
{
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("meta-a", 2));
    recv_response(fd);
    send_request(fd, api::kCreateTopic, 2, make_create_topic("meta-b", 5));
    recv_response(fd);
    send_request(fd, api::kCreateTopic, 3, make_create_topic("meta-c", 1));
    recv_response(fd);

    send_request(fd, api::kMetadata, 4);
    auto [_, meta_payload] = recv_response(fd);
    BinaryReader mr(meta_payload);
    uint32_t num_topics = mr.ReadU32();
    CHECK_EQ(num_topics, 3U);

    // Read all topics (sorted by name).
    std::vector<std::pair<std::string, int32_t>> topics;
    for (uint32_t i = 0; i < num_topics; i++) {
        std::string name = mr.ReadString();
        int32_t np = mr.ReadI32();
        topics.emplace_back(name, np);
    }

    // Should be sorted.
    CHECK_EQ(topics[0].first, std::string("meta-a"));
    CHECK_EQ(topics[0].second, static_cast<int32_t>(2));
    CHECK_EQ(topics[1].first, std::string("meta-b"));
    CHECK_EQ(topics[1].second, static_cast<int32_t>(5));
    CHECK_EQ(topics[2].first, std::string("meta-c"));
    CHECK_EQ(topics[2].second, static_cast<int32_t>(1));

    close(fd);
}

static void test_concurrent_produce_different_partitions()
{
    // Two connections produce to different partitions concurrently.
    BrokerServer srv;
    int fd1 = connect_to(srv.port);
    int fd2 = connect_to(srv.port);

    send_request(fd1, api::kCreateTopic, 1, make_create_topic("conc-part", 2));
    recv_response(fd1);

    const int N = 50;
    // fd1 → partition 0, fd2 → partition 1
    for (int i = 0; i < N; i++) {
        send_request(fd1, api::kProduce, static_cast<uint32_t>(i), make_produce("conc-part", 0, "", "p0-" + std::to_string(i)));
        send_request(fd2, api::kProduce, static_cast<uint32_t>(i), make_produce("conc-part", 1, "", "p1-" + std::to_string(i)));
    }

    for (int i = 0; i < N; i++) {
        auto pr0 = decode_produce(recv_response(fd1).second);
        CHECK_EQ(pr0.error, static_cast<int16_t>(0));
        auto pr1 = decode_produce(recv_response(fd2).second);
        CHECK_EQ(pr1.error, static_cast<int16_t>(0));
    }

    // Fetch from each partition.
    send_request(fd1, api::kFetch, 9998, make_fetch("conc-part", 0, 0, 10 * 1024 * 1024, 0));
    auto fr0 = decode_fetch(recv_response(fd1).second);
    CHECK_EQ(fr0.records.size(), static_cast<size_t>(N));

    send_request(fd2, api::kFetch, 9999, make_fetch("conc-part", 1, 0, 10 * 1024 * 1024, 0));
    auto fr1 = decode_fetch(recv_response(fd2).second);
    CHECK_EQ(fr1.records.size(), static_cast<size_t>(N));

    close(fd1);
    close(fd2);
}

static void test_pipelined_produce_fetch()
{
    // Pipeline multiple produce requests, then pipeline multiple fetch requests.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("pipeline", 1));
    recv_response(fd);

    const int N = 20;
    // Pipeline all produces.
    for (int i = 0; i < N; i++) {
        send_request(fd, api::kProduce, static_cast<uint32_t>(10 + i), make_produce("pipeline", 0, "", "pipe-" + std::to_string(i)));
    }
    // Drain all produce responses.
    for (int i = 0; i < N; i++) {
        auto pr = decode_produce(recv_response(fd).second);
        CHECK_EQ(pr.error, static_cast<int16_t>(0));
    }

    // Fetch all.
    send_request(fd, api::kFetch, 999, make_fetch("pipeline", 0, 0, 10 * 1024 * 1024, 0));
    auto fr = decode_fetch(recv_response(fd).second);
    CHECK_EQ(fr.records.size(), static_cast<size_t>(N));

    close(fd);
}

// ─── Idempotent producer helpers ─────────────────────────────────────────────

static void send_request_versioned(int fd, uint16_t api_key, uint16_t api_version, uint32_t corr_id,
                                   const std::vector<uint8_t> &payload = {})
{
    RequestFrame req;
    req.api_key = api_key;
    req.api_version = api_version;
    req.correlation_id = corr_id;
    req.payload = payload;
    auto encoded = EncodeRequest(req);
    send_all(fd, encoded.data(), encoded.size());
}

struct InitPidResp
{
    int16_t error;
    uint64_t producer_id;
    uint16_t epoch;
};
static InitPidResp decode_init_pid(const std::vector<uint8_t> &payload)
{
    BinaryReader r(payload);
    return {r.ReadI16(), r.ReadU64(), r.ReadU16()};
}

static std::vector<uint8_t> make_produce_idempotent(const std::string &topic, int32_t part_id, const std::string &key,
                                                    const std::string &value, uint64_t pid, uint16_t epoch, int32_t seq)
{
    std::vector<uint8_t> body;
    BinaryWriter w(body);
    w.WriteString(topic);
    w.WriteI32(part_id);
    w.WriteU16(static_cast<uint16_t>(key.size()));
    w.WriteBytes(reinterpret_cast<const uint8_t *>(key.data()), key.size());
    w.WriteU32(static_cast<uint32_t>(value.size()));
    w.WriteBytes(reinterpret_cast<const uint8_t *>(value.data()), value.size());
    w.WriteU64(pid);
    w.WriteU16(epoch);
    w.WriteI32(seq);
    return body;
}

// ─── Idempotent producer tests ──────────────────────────────────────────────

void test_init_producer_id()
{
    BrokerServer bs;
    int fd = connect_to(bs.port);

    // Request a producer ID.
    send_request(fd, api::kInitProducerId, 1);
    auto resp = decode_init_pid(recv_response(fd).second);

    CHECK_EQ(resp.error, err::kOk);
    CHECK(resp.producer_id > 0);
    CHECK_EQ(resp.epoch, static_cast<uint16_t>(0));

    // Second call returns a different ID.
    send_request(fd, api::kInitProducerId, 2);
    auto resp2 = decode_init_pid(recv_response(fd).second);
    CHECK_EQ(resp2.error, err::kOk);
    CHECK(resp2.producer_id != resp.producer_id);

    close(fd);
}

void test_idempotent_produce_basic()
{
    BrokerServer bs;
    int fd = connect_to(bs.port);

    // Create topic.
    send_request(fd, api::kCreateTopic, 1, make_create_topic("idem-test", 1));
    CHECK_EQ(read_error(recv_response(fd).second), err::kOk);

    // Get PID.
    send_request(fd, api::kInitProducerId, 2);
    auto pid_resp = decode_init_pid(recv_response(fd).second);
    CHECK_EQ(pid_resp.error, err::kOk);

    // Produce with seq=0 (first message).
    send_request_versioned(fd, api::kProduce, 1, 3,
                           make_produce_idempotent("idem-test", 0, "", "msg-0", pid_resp.producer_id, pid_resp.epoch, 0));
    auto pr = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr.error, err::kOk);
    CHECK_EQ(pr.offset, static_cast<uint64_t>(0));

    // Produce with seq=1 (second message).
    send_request_versioned(fd, api::kProduce, 1, 4,
                           make_produce_idempotent("idem-test", 0, "", "msg-1", pid_resp.producer_id, pid_resp.epoch, 1));
    auto pr2 = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr2.error, err::kOk);
    CHECK_EQ(pr2.offset, static_cast<uint64_t>(1));

    close(fd);
}

void test_idempotent_duplicate_detection()
{
    BrokerServer bs;
    int fd = connect_to(bs.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("idem-dup", 1));
    CHECK_EQ(read_error(recv_response(fd).second), err::kOk);

    send_request(fd, api::kInitProducerId, 2);
    auto pid_resp = decode_init_pid(recv_response(fd).second);

    // First produce: seq=0 → offset 0.
    send_request_versioned(fd, api::kProduce, 1, 3,
                           make_produce_idempotent("idem-dup", 0, "", "hello", pid_resp.producer_id, pid_resp.epoch, 0));
    auto pr = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr.error, err::kOk);
    CHECK_EQ(pr.offset, static_cast<uint64_t>(0));

    // Duplicate: same seq=0 → should return same offset without re-appending.
    send_request_versioned(fd, api::kProduce, 1, 4,
                           make_produce_idempotent("idem-dup", 0, "", "hello", pid_resp.producer_id, pid_resp.epoch, 0));
    auto pr2 = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr2.error, err::kOk);
    CHECK_EQ(pr2.offset, static_cast<uint64_t>(0)); // same offset, not re-appended

    // Verify only one message was written by fetching.
    send_request(fd, api::kFetch, 5, make_fetch("idem-dup", 0, 0, 1024 * 1024, 0));
    auto fr = decode_fetch(recv_response(fd).second);
    CHECK_EQ(fr.records.size(), static_cast<size_t>(1));
    CHECK_EQ(fr.records[0].value, "hello");

    close(fd);
}

void test_idempotent_out_of_order()
{
    BrokerServer bs;
    int fd = connect_to(bs.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("idem-ooo", 1));
    CHECK_EQ(read_error(recv_response(fd).second), err::kOk);

    send_request(fd, api::kInitProducerId, 2);
    auto pid_resp = decode_init_pid(recv_response(fd).second);

    // Produce seq=0.
    send_request_versioned(fd, api::kProduce, 1, 3,
                           make_produce_idempotent("idem-ooo", 0, "", "msg-0", pid_resp.producer_id, pid_resp.epoch, 0));
    auto pr = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr.error, err::kOk);

    // Skip seq=1, send seq=2 → out of order error.
    send_request_versioned(fd, api::kProduce, 1, 4,
                           make_produce_idempotent("idem-ooo", 0, "", "msg-2", pid_resp.producer_id, pid_resp.epoch, 2));
    auto pr2 = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr2.error, err::kOutOfOrderSequence);

    close(fd);
}

void test_idempotent_unknown_producer_id()
{
    BrokerServer bs;
    int fd = connect_to(bs.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("idem-unknown", 1));
    CHECK_EQ(read_error(recv_response(fd).second), err::kOk);

    // Use a PID that was never allocated.
    send_request_versioned(fd, api::kProduce, 1, 2, make_produce_idempotent("idem-unknown", 0, "", "msg", 99999, 0, 0));
    auto pr = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr.error, err::kUnknownProducerId);

    close(fd);
}

void test_idempotent_multiple_partitions()
{
    BrokerServer bs;
    int fd = connect_to(bs.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("idem-multi", 3));
    CHECK_EQ(read_error(recv_response(fd).second), err::kOk);

    send_request(fd, api::kInitProducerId, 2);
    auto pid_resp = decode_init_pid(recv_response(fd).second);

    // Produce to partition 0 seq=0, partition 1 seq=0, partition 2 seq=0.
    // Sequence numbers are per (PID, partition).
    for (int32_t p = 0; p < 3; ++p) {
        send_request_versioned(
            fd, api::kProduce, 1, 10 + p,
            make_produce_idempotent("idem-multi", p, "", "msg-p" + std::to_string(p), pid_resp.producer_id, pid_resp.epoch, 0));
        auto pr = decode_produce(recv_response(fd).second);
        CHECK_EQ(pr.error, err::kOk);
        CHECK_EQ(pr.offset, static_cast<uint64_t>(0));
    }

    // Now produce seq=1 to each partition.
    for (int32_t p = 0; p < 3; ++p) {
        send_request_versioned(
            fd, api::kProduce, 1, 20 + p,
            make_produce_idempotent("idem-multi", p, "", "msg-p" + std::to_string(p) + "-1", pid_resp.producer_id, pid_resp.epoch, 1));
        auto pr = decode_produce(recv_response(fd).second);
        CHECK_EQ(pr.error, err::kOk);
        CHECK_EQ(pr.offset, static_cast<uint64_t>(1));
    }

    close(fd);
}

void test_idempotent_v0_still_works()
{
    // Non-idempotent (v0) produce should still work as before.
    BrokerServer bs;
    int fd = connect_to(bs.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("idem-v0", 1));
    CHECK_EQ(read_error(recv_response(fd).second), err::kOk);

    // v0 produce — no PID/seq fields.
    send_request(fd, api::kProduce, 2, make_produce("idem-v0", 0, "", "hello-v0"));
    auto pr = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr.error, err::kOk);
    CHECK_EQ(pr.offset, static_cast<uint64_t>(0));

    // Can produce again with same payload — no dedup since v0.
    send_request(fd, api::kProduce, 3, make_produce("idem-v0", 0, "", "hello-v0"));
    auto pr2 = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr2.error, err::kOk);
    CHECK_EQ(pr2.offset, static_cast<uint64_t>(1)); // new offset, not deduped

    close(fd);
}

void test_idempotent_two_producers_independent()
{
    BrokerServer bs;
    int fd = connect_to(bs.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("idem-2pid", 1));
    CHECK_EQ(read_error(recv_response(fd).second), err::kOk);

    // Allocate two independent producer IDs.
    send_request(fd, api::kInitProducerId, 2);
    auto pid1 = decode_init_pid(recv_response(fd).second);
    send_request(fd, api::kInitProducerId, 3);
    auto pid2 = decode_init_pid(recv_response(fd).second);
    CHECK(pid1.producer_id != pid2.producer_id);

    // Both can produce seq=0 independently.
    send_request_versioned(fd, api::kProduce, 1, 10,
                           make_produce_idempotent("idem-2pid", 0, "", "from-p1", pid1.producer_id, pid1.epoch, 0));
    auto pr1 = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr1.error, err::kOk);

    send_request_versioned(fd, api::kProduce, 1, 11,
                           make_produce_idempotent("idem-2pid", 0, "", "from-p2", pid2.producer_id, pid2.epoch, 0));
    auto pr2 = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr2.error, err::kOk);
    CHECK(pr2.offset != pr1.offset); // different offsets in the log

    close(fd);
}

// ─── main ─────────────────────────────────────────────────────────────────────

int main()
{
    printf("=== FluxMQ Broker Tests ===\n\n");

    // Core functionality
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

    // Error handling & edge cases
    RUN_TEST(test_produce_unknown_topic);
    RUN_TEST(test_produce_invalid_partition);
    RUN_TEST(test_fetch_invalid_partition);
    RUN_TEST(test_unknown_api_key);
    RUN_TEST(test_empty_key_produce_fetch);
    RUN_TEST(test_large_message_produce_fetch);
    RUN_TEST(test_fetch_beyond_log_end);

    // Topic isolation & metadata
    RUN_TEST(test_multiple_topics_isolation);
    RUN_TEST(test_produce_multiple_partitions);
    RUN_TEST(test_metadata_multiple_topics);

    // Consumer group edge cases
    RUN_TEST(test_leave_group_basic);
    RUN_TEST(test_leave_unknown_group);
    RUN_TEST(test_leave_unknown_member);
    RUN_TEST(test_leave_group_triggers_rebalance);
    RUN_TEST(test_heartbeat_wrong_generation);
    RUN_TEST(test_heartbeat_unknown_member);
    RUN_TEST(test_heartbeat_unknown_group);
    RUN_TEST(test_multiple_consumer_groups_same_topic);

    // Offset management
    RUN_TEST(test_offset_commit_idempotent);
    RUN_TEST(test_offset_overwrite);

    // Stress & concurrency
    RUN_TEST(test_high_volume_produce_fetch);
    RUN_TEST(test_concurrent_produce_different_partitions);
    RUN_TEST(test_pipelined_produce_fetch);

    // Idempotent producer
    RUN_TEST(test_init_producer_id);
    RUN_TEST(test_idempotent_produce_basic);
    RUN_TEST(test_idempotent_duplicate_detection);
    RUN_TEST(test_idempotent_out_of_order);
    RUN_TEST(test_idempotent_unknown_producer_id);
    RUN_TEST(test_idempotent_multiple_partitions);
    RUN_TEST(test_idempotent_v0_still_works);
    RUN_TEST(test_idempotent_two_producers_independent);

    printf("\n%d passed, %d failed\n", g_passed, g_failed);
    return g_failed > 0 ? 1 : 0;
}
