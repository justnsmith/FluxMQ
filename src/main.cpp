#include "log.h"
#include "server.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

// ─── Timing helper ────────────────────────────────────────────────────────────

static double elapsed_ms(std::chrono::steady_clock::time_point t0)
{
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
}

// ─── Storage benchmark ────────────────────────────────────────────────────────

static void bench_storage_append(const std::string &dir, int num_msgs, int msg_size)
{
    std::filesystem::remove_all(dir);
    Log log(dir);
    std::string payload(static_cast<size_t>(msg_size), 'x');
    auto data = std::span<const std::byte>(reinterpret_cast<const std::byte *>(payload.data()), payload.size());

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < num_msgs; i++)
        log.Append(data);
    double ms = elapsed_ms(t0);

    printf("storage  append  %7d x %4d-byte msgs  %7.0f ms  =>  %8.0f msg/s  %6.1f MB/s\n", num_msgs, msg_size, ms,
           num_msgs / (ms / 1000.0), (static_cast<double>(num_msgs) * msg_size) / (ms / 1000.0) / (1024.0 * 1024.0));
    std::filesystem::remove_all(dir);
}

// ─── Network benchmark ────────────────────────────────────────────────────────

// Synchronous helper: write all bytes to a blocking fd.
static bool write_all(int fd, const void *data, size_t n)
{
    const auto *p = static_cast<const uint8_t *>(data);
    while (n > 0) {
        ssize_t sent = send(fd, p, n, MSG_NOSIGNAL);
        if (sent <= 0)
            return false;
        p += sent;
        n -= static_cast<size_t>(sent);
    }
    return true;
}

// Synchronous helper: read exactly n bytes from a blocking fd.
static bool read_all(int fd, void *dst, size_t n)
{
    auto *p = static_cast<uint8_t *>(dst);
    while (n > 0) {
        ssize_t r = recv(fd, p, n, 0);
        if (r <= 0)
            return false;
        p += r;
        n -= static_cast<size_t>(r);
    }
    return true;
}

static void bench_network(uint16_t port, int num_requests, int payload_size)
{
    // Connect a blocking TCP client.
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));

    // Pre-encode a single request frame (reused for all requests).
    RequestFrame req;
    req.api_key = api::kProduce;
    req.api_version = 0;
    req.correlation_id = 0;
    req.payload.assign(static_cast<size_t>(payload_size), static_cast<uint8_t>('p'));
    auto encoded = EncodeRequest(req);

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < num_requests; i++) {
        // Set a fresh correlation_id per request.
        EncBe32(encoded.data() + 8, static_cast<uint32_t>(i));
        write_all(fd, encoded.data(), encoded.size());

        // Read the response header (4-byte total_len).
        uint8_t hdr[4];
        read_all(fd, hdr, 4);
        uint32_t resp_body_len = DecBe32(hdr);
        // Drain response body.
        std::vector<uint8_t> body(resp_body_len);
        read_all(fd, body.data(), resp_body_len);
    }
    double ms = elapsed_ms(t0);

    printf("network  echo    %7d x %4d-byte msgs  %7.0f ms  =>  %8.0f req/s  %6.1f MB/s\n", num_requests, payload_size, ms,
           num_requests / (ms / 1000.0), (static_cast<double>(num_requests) * payload_size) / (ms / 1000.0) / (1024.0 * 1024.0));

    close(fd);
}

// ─── main ─────────────────────────────────────────────────────────────────────

int main()
{
    printf("=== FluxMQ Benchmark ===\n\n");

    // ── Storage ──────────────────────────────────────────────────────────────
    const std::string bench_dir = "/tmp/fluxmq_bench";
    bench_storage_append(bench_dir, 1'000'000, 100);
    bench_storage_append(bench_dir, 100'000, 1024);

    // ── Network ──────────────────────────────────────────────────────────────
    // Echo handler: bounce the payload back with the same correlation_id.
    auto echo_handler = [](Connection &conn, RequestFrame frame) {
        ResponseFrame resp;
        resp.correlation_id = frame.correlation_id;
        resp.payload = std::move(frame.payload);
        conn.SendResponse(resp);
    };

    Server server(echo_handler);
    std::thread server_thread([&] { server.Run(0); });
    uint16_t port = server.WaitReady();

    bench_network(port, 200'000, 100);
    bench_network(port, 50'000, 1024);

    server.Stop();
    server_thread.join();

    printf("\nDone.\n");
    return 0;
}
