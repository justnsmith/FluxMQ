#include "connection.h"
#include "protocol.h"
#include "server.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstdio>
#include <cstring>
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
        printf("  %-50s", #fn " ...");                                                                                                     \
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

// ─── Test client helpers ──────────────────────────────────────────────────────

// Create a blocking TCP socket connected to localhost:port.
static int connect_to(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        throw std::runtime_error("socket failed");
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        close(fd);
        throw std::runtime_error("connect failed");
    }
    return fd;
}

// Write all bytes to a blocking fd.
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

// Read exactly n bytes from a blocking fd.
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

// Send an encoded request frame.
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

// Read one response frame from a blocking fd.
static ResponseFrame recv_response(int fd)
{
    uint8_t hdr[4];
    recv_all(fd, hdr, 4);
    uint32_t total_len = DecBe32(hdr);

    std::vector<uint8_t> body(total_len);
    recv_all(fd, body.data(), total_len);

    if (total_len < kRespBodyHdrSize)
        throw std::runtime_error("response too short");

    ResponseFrame resp;
    resp.correlation_id = DecBe32(body.data());
    resp.payload.assign(body.begin() + kRespBodyHdrSize, body.end());
    return resp;
}

// ─── Shared echo-server fixture ───────────────────────────────────────────────

struct EchoServer
{
    RequestHandler echo_handler = [](Connection &conn, RequestFrame frame) {
        ResponseFrame resp;
        resp.correlation_id = frame.correlation_id;
        resp.payload = std::move(frame.payload);
        conn.SendResponse(resp);
    };

    Server server{echo_handler};
    std::thread thread;
    uint16_t port{0};

    EchoServer()
    {
        thread = std::thread([this] { server.Run(0); });
        port = server.WaitReady();
    }

    ~EchoServer()
    {
        server.Stop();
        thread.join();
    }
};

// ─── Protocol unit tests ──────────────────────────────────────────────────────

static void test_encode_decode_request()
{
    RequestFrame req;
    req.api_key = api::kProduce;
    req.api_version = 1;
    req.correlation_id = 0xDEADBEEF;
    req.payload = {0x01, 0x02, 0x03};

    auto wire = EncodeRequest(req);

    // Check total length: 4 (len field) + 8 (fixed header) + 3 (payload) = 15
    CHECK_EQ(wire.size(), 15U);

    // Check total_len field (big-endian): 8 + 3 = 11
    uint32_t total_len = DecBe32(wire.data());
    CHECK_EQ(total_len, 11U);

    CHECK_EQ(DecBe16(wire.data() + 4), api::kProduce);
    CHECK_EQ(DecBe16(wire.data() + 6), static_cast<uint16_t>(1));
    CHECK_EQ(DecBe32(wire.data() + 8), static_cast<uint32_t>(0xDEADBEEF));
    CHECK_EQ(wire[12], static_cast<uint8_t>(0x01));
    CHECK_EQ(wire[13], static_cast<uint8_t>(0x02));
    CHECK_EQ(wire[14], static_cast<uint8_t>(0x03));
}

static void test_encode_decode_response()
{
    ResponseFrame resp;
    resp.correlation_id = 42;
    resp.payload = {0xAA, 0xBB};

    auto wire = EncodeResponse(resp);

    // 4 (len) + 4 (corr_id) + 2 (payload) = 10
    CHECK_EQ(wire.size(), 10U);
    uint32_t total_len = DecBe32(wire.data());
    CHECK_EQ(total_len, 6U); // 4 + 2
    CHECK_EQ(DecBe32(wire.data() + 4), static_cast<uint32_t>(42));
    CHECK_EQ(wire[8], static_cast<uint8_t>(0xAA));
    CHECK_EQ(wire[9], static_cast<uint8_t>(0xBB));
}

// ─── Server integration tests ─────────────────────────────────────────────────

static void test_echo_basic()
{
    EchoServer srv;
    int fd = connect_to(srv.port);

    send_request(fd, api::kProduce, 99, {1, 2, 3, 4, 5});
    auto resp = recv_response(fd);

    CHECK_EQ(resp.correlation_id, static_cast<uint32_t>(99));
    CHECK_EQ(resp.payload.size(), 5U);
    CHECK_EQ(resp.payload[0], static_cast<uint8_t>(1));
    CHECK_EQ(resp.payload[4], static_cast<uint8_t>(5));

    close(fd);
}

static void test_pipelined_requests()
{
    // Send N requests without waiting for their responses, then read all N
    // responses.  Verifies correlation IDs are matched correctly.
    EchoServer srv;
    int fd = connect_to(srv.port);

    const int kN = 10;
    for (int i = 0; i < kN; i++) {
        std::vector<uint8_t> payload(static_cast<size_t>(i + 1), static_cast<uint8_t>(i));
        send_request(fd, api::kFetch, static_cast<uint32_t>(i), payload);
    }
    for (int i = 0; i < kN; i++) {
        auto resp = recv_response(fd);
        CHECK_EQ(resp.correlation_id, static_cast<uint32_t>(i));
        CHECK_EQ(resp.payload.size(), static_cast<size_t>(i + 1));
    }

    close(fd);
}

static void test_partial_reads()
{
    // Send the frame one byte at a time with brief pauses.
    // The server must reassemble the frame correctly before responding.
    EchoServer srv;
    int fd = connect_to(srv.port);

    std::vector<uint8_t> payload(10, 0xAB);
    auto wire = EncodeRequest(RequestFrame{api::kProduce, 0, 77, payload});

    for (uint8_t b : wire) {
        send_all(fd, &b, 1);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    auto resp = recv_response(fd);
    CHECK_EQ(resp.correlation_id, static_cast<uint32_t>(77));
    CHECK_EQ(resp.payload.size(), 10U);

    close(fd);
}

static void test_large_payload()
{
    EchoServer srv;
    int fd = connect_to(srv.port);

    std::vector<uint8_t> big(128 * 1024, 0xFF);
    send_request(fd, api::kProduce, 1000, big);
    auto resp = recv_response(fd);

    CHECK_EQ(resp.correlation_id, static_cast<uint32_t>(1000));
    CHECK_EQ(resp.payload.size(), big.size());
    CHECK_EQ(resp.payload.front(), static_cast<uint8_t>(0xFF));

    close(fd);
}

static void test_multiple_connections()
{
    EchoServer srv;

    const int kConns = 8;
    int fds[kConns];
    for (int i = 0; i < kConns; i++)
        fds[i] = connect_to(srv.port);

    // Send one request from each connection.
    for (int i = 0; i < kConns; i++) {
        send_request(fds[i], api::kMetadata, static_cast<uint32_t>(i));
    }
    // Read responses from each.
    for (int i = 0; i < kConns; i++) {
        auto resp = recv_response(fds[i]);
        CHECK_EQ(resp.correlation_id, static_cast<uint32_t>(i));
    }

    for (int i = 0; i < kConns; i++)
        close(fds[i]);
}

static void test_client_disconnect_mid_frame()
{
    // Client disconnects after sending only part of a frame.
    // Server must not crash and must continue serving other connections.
    EchoServer srv;

    int bad = connect_to(srv.port);
    // Send only 2 bytes (less than the 4-byte header).
    uint8_t partial[2] = {0x00, 0x0A};
    send_all(bad, partial, 2);
    close(bad); // abrupt close

    // Give the server a moment to process the disconnect.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // A subsequent connection must still work.
    int good = connect_to(srv.port);
    send_request(good, api::kHeartbeat, 555);
    auto resp = recv_response(good);
    CHECK_EQ(resp.correlation_id, static_cast<uint32_t>(555));
    close(good);
}

// ─── main ─────────────────────────────────────────────────────────────────────

int main()
{
    printf("=== FluxMQ Network Layer Tests ===\n\n");

    RUN_TEST(test_encode_decode_request);
    RUN_TEST(test_encode_decode_response);
    RUN_TEST(test_echo_basic);
    RUN_TEST(test_pipelined_requests);
    RUN_TEST(test_partial_reads);
    RUN_TEST(test_large_payload);
    RUN_TEST(test_multiple_connections);
    RUN_TEST(test_client_disconnect_mid_frame);

    printf("\n%d passed, %d failed\n", g_passed, g_failed);
    return g_failed > 0 ? 1 : 0;
}
