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
#include <vector>

// ─── Minimal test framework ───────────────────────────────────────────────────

static int g_passed = 0;
static int g_failed = 0;

#define RUN_TEST(fn)                                                                                                                       \
    do {                                                                                                                                   \
        printf("  %-60s", #fn " ...");                                                                                                     \
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

// ─── Helper: start/stop a full broker ─────────────────────────────────────────

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

struct BrokerServer
{
    std::string data_dir;
    bool owns_data_dir{true}; // if true, destructor removes data_dir
    std::unique_ptr<TopicManager> tm;
    std::unique_ptr<GroupCoordinator> gc;
    std::unique_ptr<Server> server;
    std::shared_ptr<BrokerHandler> broker;
    std::thread thread;
    uint16_t port{0};

    BrokerServer(std::string existing_dir = "", std::chrono::milliseconds session_timeout = std::chrono::milliseconds(10000))
    {
        if (existing_dir.empty()) {
            auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
            data_dir = (std::filesystem::temp_directory_path() / ("fluxmq_chaos_test_" + std::to_string(ns))).string();
            std::filesystem::create_directories(data_dir);
        }
        else {
            data_dir = existing_dir;
            owns_data_dir = false; // caller manages this directory
        }

        tm = std::make_unique<TopicManager>(data_dir);
        gc = std::make_unique<GroupCoordinator>(session_timeout);

        server = std::make_unique<Server>([](Connection &, RequestFrame) {});
        broker =
            std::make_shared<BrokerHandler>(*tm, *gc, [this](int fd, ResponseFrame resp) { server->PostResponse(fd, std::move(resp)); });
        server = std::make_unique<Server>([this](Connection &conn, RequestFrame frame) { broker->Handle(conn, frame); });

        thread = std::thread([this] { server->Run(0); });
        port = server->WaitReady();
    }

    // Stop the broker but preserve the data directory.
    void StopPreserveData()
    {
        server->Stop();
        thread.join();
        // Reset components to release file handles.
        broker.reset();
        server.reset();
        gc.reset();
        tm.reset();
    }

    ~BrokerServer()
    {
        if (server) {
            server->Stop();
            thread.join();
        }
        if (owns_data_dir) {
            std::filesystem::remove_all(data_dir);
        }
    }
};

// ─── Chaos / Fault Tolerance Tests ────────────────────────────────────────────

static void test_broker_restart_preserves_data()
{
    // Write data, stop the broker, restart from the same data directory,
    // and verify all messages survived.
    auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
    auto data_dir = (std::filesystem::temp_directory_path() / ("fluxmq_restart_" + std::to_string(ns))).string();
    std::filesystem::create_directories(data_dir);

    const int N = 50;

    // Phase 1: write data.
    {
        BrokerServer srv(data_dir);
        int fd = connect_to(srv.port);

        send_request(fd, api::kCreateTopic, 1, make_create_topic("persist", 2));
        recv_response(fd);

        for (int i = 0; i < N; i++) {
            send_request(fd, api::kProduce, static_cast<uint32_t>(10 + i), make_produce("persist", i % 2, "", "msg-" + std::to_string(i)));
            auto pr = decode_produce(recv_response(fd).second);
            CHECK_EQ(pr.error, static_cast<int16_t>(0));
        }

        close(fd);
        srv.StopPreserveData();
    }

    // Phase 2: restart and verify.
    {
        BrokerServer srv(data_dir);
        int fd = connect_to(srv.port);

        // Topic must still exist.
        send_request(fd, api::kMetadata, 1);
        auto [_, meta] = recv_response(fd);
        BinaryReader mr(meta);
        uint32_t num_topics = mr.ReadU32();
        CHECK(num_topics >= 1);
        std::string name = mr.ReadString();
        CHECK_EQ(name, std::string("persist"));

        // Fetch from both partitions.
        int total_records = 0;
        for (int p = 0; p < 2; p++) {
            send_request(fd, api::kFetch, static_cast<uint32_t>(10 + p), make_fetch("persist", p, 0, 10 * 1024 * 1024, 0));
            auto fr = decode_fetch(recv_response(fd).second);
            CHECK_EQ(fr.error, static_cast<int16_t>(0));
            total_records += static_cast<int>(fr.records.size());
        }
        CHECK_EQ(total_records, N);

        close(fd);
    }

    std::filesystem::remove_all(data_dir);
}

static void test_broker_restart_preserves_offsets()
{
    // Committed offsets must survive a broker restart.
    auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
    auto data_dir = (std::filesystem::temp_directory_path() / ("fluxmq_offsets_" + std::to_string(ns))).string();
    std::filesystem::create_directories(data_dir);

    // Phase 1: produce and commit offsets.
    {
        BrokerServer srv(data_dir);
        int fd = connect_to(srv.port);

        send_request(fd, api::kCreateTopic, 1, make_create_topic("off-persist", 1));
        recv_response(fd);

        for (int i = 0; i < 10; i++) {
            send_request(fd, api::kProduce, static_cast<uint32_t>(i), make_produce("off-persist", 0, "", "v" + std::to_string(i)));
            recv_response(fd);
        }

        send_request(fd, api::kOffsetCommit, 20, make_offset_commit("persist-grp", "off-persist", 0, 7));
        CHECK_EQ(read_error(recv_response(fd).second), static_cast<int16_t>(0));

        close(fd);
        srv.StopPreserveData();
    }

    // Phase 2: restart and verify offset.
    {
        BrokerServer srv(data_dir);
        int fd = connect_to(srv.port);

        send_request(fd, api::kOffsetFetch, 1, make_offset_fetch("persist-grp", "off-persist", 0));
        auto of = decode_offset_fetch(recv_response(fd).second);
        CHECK_EQ(of.error, static_cast<int16_t>(0));
        CHECK_EQ(of.offset, static_cast<uint64_t>(7));

        close(fd);
    }

    std::filesystem::remove_all(data_dir);
}

static void test_broker_restart_preserves_topics()
{
    // Topic metadata (name + partition count) must survive a restart.
    auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
    auto data_dir = (std::filesystem::temp_directory_path() / ("fluxmq_topics_" + std::to_string(ns))).string();
    std::filesystem::create_directories(data_dir);

    {
        BrokerServer srv(data_dir);
        int fd = connect_to(srv.port);

        send_request(fd, api::kCreateTopic, 1, make_create_topic("alpha", 3));
        recv_response(fd);
        send_request(fd, api::kCreateTopic, 2, make_create_topic("beta", 5));
        recv_response(fd);
        send_request(fd, api::kCreateTopic, 3, make_create_topic("gamma", 1));
        recv_response(fd);

        close(fd);
        srv.StopPreserveData();
    }

    {
        BrokerServer srv(data_dir);
        int fd = connect_to(srv.port);

        send_request(fd, api::kMetadata, 1);
        auto [_, meta] = recv_response(fd);
        BinaryReader mr(meta);
        uint32_t num_topics = mr.ReadU32();
        CHECK_EQ(num_topics, 3U);

        // Topics are sorted by name.
        std::string n1 = mr.ReadString();
        int32_t p1 = mr.ReadI32();
        CHECK_EQ(n1, std::string("alpha"));
        CHECK_EQ(p1, static_cast<int32_t>(3));

        std::string n2 = mr.ReadString();
        int32_t p2 = mr.ReadI32();
        CHECK_EQ(n2, std::string("beta"));
        CHECK_EQ(p2, static_cast<int32_t>(5));

        std::string n3 = mr.ReadString();
        int32_t p3 = mr.ReadI32();
        CHECK_EQ(n3, std::string("gamma"));
        CHECK_EQ(p3, static_cast<int32_t>(1));

        close(fd);
    }

    std::filesystem::remove_all(data_dir);
}

static void test_concurrent_produce_fetch_same_partition()
{
    // One thread produces while another fetches from the same partition.
    // No data loss or corruption should occur.
    BrokerServer srv;

    const int N = 200;
    std::atomic<bool> producer_done{false};

    // Producer thread.
    std::thread producer([&] {
        int fd = connect_to(srv.port);
        send_request(fd, api::kCreateTopic, 1, make_create_topic("conc-same", 1));
        recv_response(fd);

        for (int i = 0; i < N; i++) {
            send_request(fd, api::kProduce, static_cast<uint32_t>(i), make_produce("conc-same", 0, "", "m-" + std::to_string(i)));
            recv_response(fd);
        }
        producer_done = true;
        close(fd);
    });

    // Give producer a head start to create the topic.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Consumer thread: keep fetching until we've seen all N records.
    int total_fetched = 0;
    uint64_t fetch_offset = 0;
    int fd = connect_to(srv.port);

    while (total_fetched < N) {
        send_request(fd, api::kFetch, static_cast<uint32_t>(1000 + total_fetched),
                     make_fetch("conc-same", 0, fetch_offset, 1024 * 1024, 200));
        auto fr = decode_fetch(recv_response(fd).second);
        if (fr.error != 0) {
            // Topic may not exist yet; retry.
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }
        for (const auto &rec : fr.records) {
            CHECK_EQ(rec.offset, fetch_offset);
            fetch_offset++;
            total_fetched++;
        }
    }

    CHECK_EQ(total_fetched, N);
    close(fd);
    producer.join();
}

static void test_consumer_group_session_timeout_eviction()
{
    // A member that stops heartbeating should be evicted after the session
    // timeout, and the remaining member should be notified via kRebalanceInProgress.
    BrokerServer srv("", std::chrono::milliseconds(1000)); // 1s session timeout
    int fd1 = connect_to(srv.port);
    int fd2 = connect_to(srv.port);

    send_request(fd1, api::kCreateTopic, 1, make_create_topic("timeout-t", 4));
    recv_response(fd1);

    // C1 joins and stabilizes.
    send_request(fd1, api::kJoinGroup, 2, make_join_group("tg", "timeout-t", "c1"));
    auto j1 = decode_join(recv_response(fd1).second);
    auto a1 = balanced_assignments(j1.members, 4);
    send_request(fd1, api::kSyncGroup, 3, make_sync_group("tg", j1.gen_id, "c1", a1));
    decode_sync(recv_response(fd1).second);

    // C2 joins → rebalance.
    send_request(fd2, api::kJoinGroup, 4, make_join_group("tg", "timeout-t", "c2"));
    decode_join(recv_response(fd2).second);

    // C1 re-joins.
    send_request(fd1, api::kHeartbeat, 5, make_heartbeat("tg", 1, "c1"));
    recv_response(fd1);
    send_request(fd1, api::kJoinGroup, 6, make_join_group("tg", "timeout-t", "c1"));
    auto j1b = decode_join(recv_response(fd1).second);

    // Both sync.
    auto a2 = balanced_assignments(j1b.members, 4);
    send_request(fd1, api::kSyncGroup, 7, make_sync_group("tg", j1b.gen_id, "c1", a2));
    decode_sync(recv_response(fd1).second);
    send_request(fd2, api::kSyncGroup, 8, make_sync_group("tg", j1b.gen_id, "c2"));
    decode_sync(recv_response(fd2).second);

    // C2 stops heartbeating (simulates crash). C1 keeps heartbeating.
    // After session timeout (1s), C2 should be evicted.
    close(fd2); // abrupt disconnect

    // C1 heartbeats for a while. Eventually the reaper evicts C2 and C1
    // should get kRebalanceInProgress.
    bool got_rebalance = false;
    for (int attempt = 0; attempt < 30; attempt++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        send_request(fd1, api::kHeartbeat, static_cast<uint32_t>(100 + attempt), make_heartbeat("tg", j1b.gen_id, "c1"));
        auto hb = read_error(recv_response(fd1).second);
        if (hb == 27) { // kRebalanceInProgress
            got_rebalance = true;
            break;
        }
        CHECK_EQ(hb, static_cast<int16_t>(0)); // must be OK or rebalance
    }
    CHECK(got_rebalance);

    close(fd1);
}

static void test_produce_during_consumer_rebalance()
{
    // Producing messages while a consumer group is rebalancing should not
    // lose any data. The messages must be fetchable after rebalance completes.
    BrokerServer srv;
    int prod_fd = connect_to(srv.port);
    int fd1 = connect_to(srv.port);
    int fd2 = connect_to(srv.port);

    send_request(prod_fd, api::kCreateTopic, 1, make_create_topic("reb-prod", 2));
    recv_response(prod_fd);

    // Produce some initial data.
    for (int i = 0; i < 10; i++) {
        send_request(prod_fd, api::kProduce, static_cast<uint32_t>(i), make_produce("reb-prod", i % 2, "", "pre-" + std::to_string(i)));
        recv_response(prod_fd);
    }

    // C1 joins.
    send_request(fd1, api::kJoinGroup, 100, make_join_group("rp", "reb-prod", "c1"));
    auto j1 = decode_join(recv_response(fd1).second);
    auto a1 = balanced_assignments(j1.members, 2);
    send_request(fd1, api::kSyncGroup, 101, make_sync_group("rp", j1.gen_id, "c1", a1));
    decode_sync(recv_response(fd1).second);

    // C2 joins → triggers rebalance. While rebalancing, produce more.
    send_request(fd2, api::kJoinGroup, 102, make_join_group("rp", "reb-prod", "c2"));
    decode_join(recv_response(fd2).second);

    // Produce during rebalance.
    for (int i = 10; i < 20; i++) {
        send_request(prod_fd, api::kProduce, static_cast<uint32_t>(i), make_produce("reb-prod", i % 2, "", "during-" + std::to_string(i)));
        auto pr = decode_produce(recv_response(prod_fd).second);
        CHECK_EQ(pr.error, static_cast<int16_t>(0));
    }

    // Complete rebalance.
    send_request(fd1, api::kHeartbeat, 103, make_heartbeat("rp", 1, "c1"));
    recv_response(fd1);
    send_request(fd1, api::kJoinGroup, 104, make_join_group("rp", "reb-prod", "c1"));
    auto j1b = decode_join(recv_response(fd1).second);

    // Produce after rebalance.
    for (int i = 20; i < 30; i++) {
        send_request(prod_fd, api::kProduce, static_cast<uint32_t>(i), make_produce("reb-prod", i % 2, "", "post-" + std::to_string(i)));
        auto pr = decode_produce(recv_response(prod_fd).second);
        CHECK_EQ(pr.error, static_cast<int16_t>(0));
    }

    // Verify all 30 messages across both partitions.
    int total = 0;
    for (int p = 0; p < 2; p++) {
        send_request(prod_fd, api::kFetch, static_cast<uint32_t>(200 + p), make_fetch("reb-prod", p, 0, 10 * 1024 * 1024, 0));
        auto fr = decode_fetch(recv_response(prod_fd).second);
        CHECK_EQ(fr.error, static_cast<int16_t>(0));
        total += static_cast<int>(fr.records.size());
    }
    CHECK_EQ(total, 30);

    close(prod_fd);
    close(fd1);
    close(fd2);
}

static void test_rapid_topic_creation()
{
    // Create many topics rapidly and verify all appear in metadata.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    const int N = 20;
    for (int i = 0; i < N; i++) {
        send_request(fd, api::kCreateTopic, static_cast<uint32_t>(i), make_create_topic("rapid-" + std::to_string(i), (i % 4) + 1));
        auto [_, payload] = recv_response(fd);
        CHECK_EQ(read_error(payload), static_cast<int16_t>(0));
    }

    send_request(fd, api::kMetadata, 999);
    auto [_, meta] = recv_response(fd);
    BinaryReader mr(meta);
    uint32_t num_topics = mr.ReadU32();
    CHECK_EQ(num_topics, static_cast<uint32_t>(N));

    close(fd);
}

static void test_produce_fetch_across_segment_rotation()
{
    // Produce enough data to trigger segment rotation in the underlying log,
    // then verify all data is still fetchable.
    BrokerServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kCreateTopic, 1, make_create_topic("seg-rotate", 1));
    recv_response(fd);

    // Produce 500 messages with ~200B each to generate enough data.
    const int N = 500;
    std::string payload(200, 'R');
    for (int i = 0; i < N; i++) {
        send_request(fd, api::kProduce, static_cast<uint32_t>(i), make_produce("seg-rotate", 0, "", payload + std::to_string(i)));
        auto pr = decode_produce(recv_response(fd).second);
        CHECK_EQ(pr.error, static_cast<int16_t>(0));
        CHECK_EQ(pr.offset, static_cast<uint64_t>(i));
    }

    // Fetch in batches to avoid a huge single response.
    int total_fetched = 0;
    uint64_t offset = 0;
    while (total_fetched < N) {
        send_request(fd, api::kFetch, static_cast<uint32_t>(1000 + total_fetched), make_fetch("seg-rotate", 0, offset, 64 * 1024, 0));
        auto fr = decode_fetch(recv_response(fd).second);
        CHECK_EQ(fr.error, static_cast<int16_t>(0));
        CHECK(!fr.records.empty());
        for (const auto &rec : fr.records) {
            CHECK_EQ(rec.offset, offset);
            offset++;
            total_fetched++;
        }
    }
    CHECK_EQ(total_fetched, N);

    close(fd);
}

static void test_multiple_long_poll_consumers()
{
    // Multiple consumers long-polling on the same partition.
    // All should receive data when it arrives.
    BrokerServer srv;

    int prod_fd = connect_to(srv.port);
    send_request(prod_fd, api::kCreateTopic, 1, make_create_topic("multi-lp", 1));
    recv_response(prod_fd);

    const int kConsumers = 4;
    int consumer_fds[kConsumers];
    for (int i = 0; i < kConsumers; i++) {
        consumer_fds[i] = connect_to(srv.port);
        // Each consumer long-polls from offset 0.
        send_request(consumer_fds[i], api::kFetch, static_cast<uint32_t>(100 + i), make_fetch("multi-lp", 0, 0, 65536, 2000));
    }

    // Give the server time to register all pending fetches.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Produce one message.
    send_request(prod_fd, api::kProduce, 1, make_produce("multi-lp", 0, "", "broadcast"));
    recv_response(prod_fd);

    // All consumers should receive the message.
    for (int i = 0; i < kConsumers; i++) {
        auto fr = decode_fetch(recv_response(consumer_fds[i]).second);
        CHECK_EQ(fr.error, static_cast<int16_t>(0));
        CHECK_EQ(fr.records.size(), 1U);
        CHECK_EQ(fr.records[0].value, std::string("broadcast"));
        close(consumer_fds[i]);
    }

    close(prod_fd);
}

static void test_restart_produce_continues_offset()
{
    // After a restart, producing more messages should continue from the
    // correct offset without gaps or duplicates.
    auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
    auto data_dir = (std::filesystem::temp_directory_path() / ("fluxmq_cont_" + std::to_string(ns))).string();
    std::filesystem::create_directories(data_dir);

    // Phase 1: produce 10 messages.
    {
        BrokerServer srv(data_dir);
        int fd = connect_to(srv.port);

        send_request(fd, api::kCreateTopic, 1, make_create_topic("cont", 1));
        recv_response(fd);

        for (int i = 0; i < 10; i++) {
            send_request(fd, api::kProduce, static_cast<uint32_t>(i), make_produce("cont", 0, "", "v" + std::to_string(i)));
            auto pr = decode_produce(recv_response(fd).second);
            CHECK_EQ(pr.offset, static_cast<uint64_t>(i));
        }

        close(fd);
        srv.StopPreserveData();
    }

    // Phase 2: restart and produce 10 more.
    {
        BrokerServer srv(data_dir);
        int fd = connect_to(srv.port);

        for (int i = 10; i < 20; i++) {
            send_request(fd, api::kProduce, static_cast<uint32_t>(i), make_produce("cont", 0, "", "v" + std::to_string(i)));
            auto pr = decode_produce(recv_response(fd).second);
            CHECK_EQ(pr.error, static_cast<int16_t>(0));
            CHECK_EQ(pr.offset, static_cast<uint64_t>(i)); // must continue from 10
        }

        // Fetch all 20.
        send_request(fd, api::kFetch, 999, make_fetch("cont", 0, 0, 10 * 1024 * 1024, 0));
        auto fr = decode_fetch(recv_response(fd).second);
        CHECK_EQ(fr.records.size(), 20U);
        for (int i = 0; i < 20; i++) {
            CHECK_EQ(fr.records[i].offset, static_cast<uint64_t>(i));
            CHECK_EQ(fr.records[i].value, std::string("v" + std::to_string(i)));
        }

        close(fd);
    }

    std::filesystem::remove_all(data_dir);
}

static void test_client_crash_mid_produce()
{
    // A client that disconnects in the middle of sending a produce request
    // should not corrupt the broker. Other clients must still work.
    BrokerServer srv;

    // Bad client: connect, send partial frame, disconnect.
    {
        int bad = connect_to(srv.port);
        // Send the first few bytes of a produce request but not the full frame.
        RequestFrame req;
        req.api_key = api::kProduce;
        req.correlation_id = 1;
        req.payload = {0x01, 0x02, 0x03};
        auto encoded = EncodeRequest(req);
        // Send only half the frame.
        send_all(bad, encoded.data(), encoded.size() / 2);
        close(bad);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Good client: should work fine.
    int fd = connect_to(srv.port);
    send_request(fd, api::kCreateTopic, 1, make_create_topic("crash-mid", 1));
    auto [_, payload] = recv_response(fd);
    CHECK_EQ(read_error(payload), static_cast<int16_t>(0));

    send_request(fd, api::kProduce, 2, make_produce("crash-mid", 0, "", "still-works"));
    auto pr = decode_produce(recv_response(fd).second);
    CHECK_EQ(pr.error, static_cast<int16_t>(0));
    CHECK_EQ(pr.offset, static_cast<uint64_t>(0));

    close(fd);
}

static void test_many_clients_connect_produce_disconnect()
{
    // Simulate many short-lived clients connecting, producing a message,
    // and disconnecting. The broker must handle this churn gracefully.
    BrokerServer srv;

    // Create topic with a persistent connection first.
    int setup_fd = connect_to(srv.port);
    send_request(setup_fd, api::kCreateTopic, 1, make_create_topic("churn", 1));
    recv_response(setup_fd);
    close(setup_fd);

    const int N = 30;
    for (int i = 0; i < N; i++) {
        int fd = connect_to(srv.port);
        send_request(fd, api::kProduce, static_cast<uint32_t>(i), make_produce("churn", 0, "", "churn-" + std::to_string(i)));
        auto pr = decode_produce(recv_response(fd).second);
        CHECK_EQ(pr.error, static_cast<int16_t>(0));
        close(fd);
    }

    // Verify all N messages are present.
    int fd = connect_to(srv.port);
    send_request(fd, api::kFetch, 999, make_fetch("churn", 0, 0, 10 * 1024 * 1024, 0));
    auto fr = decode_fetch(recv_response(fd).second);
    CHECK_EQ(fr.records.size(), static_cast<size_t>(N));
    close(fd);
}

// ─── main ─────────────────────────────────────────────────────────────────────

int main()
{
    printf("=== FluxMQ Chaos & Fault Tolerance Tests ===\n\n");

    // Persistence / restart tests
    RUN_TEST(test_broker_restart_preserves_data);
    RUN_TEST(test_broker_restart_preserves_offsets);
    RUN_TEST(test_broker_restart_preserves_topics);
    RUN_TEST(test_restart_produce_continues_offset);

    // Concurrent access
    RUN_TEST(test_concurrent_produce_fetch_same_partition);
    RUN_TEST(test_multiple_long_poll_consumers);

    // Client failure
    RUN_TEST(test_client_crash_mid_produce);
    RUN_TEST(test_many_clients_connect_produce_disconnect);

    // Consumer group fault tolerance
    RUN_TEST(test_consumer_group_session_timeout_eviction);
    RUN_TEST(test_produce_during_consumer_rebalance);

    // Stress
    RUN_TEST(test_rapid_topic_creation);
    RUN_TEST(test_produce_fetch_across_segment_rotation);

    printf("\n%d passed, %d failed\n", g_passed, g_failed);
    return g_failed > 0 ? 1 : 0;
}
