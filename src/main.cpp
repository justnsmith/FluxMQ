#include "log.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

static double elapsed_ms(std::chrono::steady_clock::time_point start)
{
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(now - start).count();
}

// ---------------------------------------------------------------------------
// Benchmark: sustained append throughput
// ---------------------------------------------------------------------------

static void bench_append(const std::string &dir, int num_msgs, int msg_size)
{
    std::filesystem::remove_all(dir);

    Log log(dir);

    std::string payload(static_cast<size_t>(msg_size), 'x');
    auto data = std::span<const std::byte>(reinterpret_cast<const std::byte *>(payload.data()), payload.size());

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < num_msgs; i++) {
        log.Append(data);
    }
    double ms = elapsed_ms(t0);

    double msgs_per_sec = static_cast<double>(num_msgs) / (ms / 1000.0);
    double mb_per_sec = (static_cast<double>(num_msgs) * msg_size) / (ms / 1000.0) / (1024.0 * 1024.0);

    printf("append %d x %d-byte msgs in %.1f ms  =>  %.0f msg/s  %.1f MB/s\n", num_msgs, msg_size, ms, msgs_per_sec, mb_per_sec);

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Benchmark: random read latency
// ---------------------------------------------------------------------------

static void bench_read(const std::string &dir, int num_msgs, int msg_size)
{
    std::filesystem::remove_all(dir);

    Log log(dir);

    std::string payload(static_cast<size_t>(msg_size), 'y');
    auto data = std::span<const std::byte>(reinterpret_cast<const std::byte *>(payload.data()), payload.size());
    for (int i = 0; i < num_msgs; i++) {
        log.Append(data);
    }

    // Read every 1000th record to sample across the full offset space.
    int reads = 0;
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < num_msgs; i += 1000) {
        auto result = log.Read(static_cast<uint64_t>(i));
        if (result.empty()) {
            printf("ERROR: missing record at offset %d\n", i);
        }
        reads++;
    }
    double ms = elapsed_ms(t0);

    printf("read  %d sampled records (1-in-1000 of %d)  in %.1f ms\n", reads, num_msgs, ms);

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main()
{
    const std::string bench_dir = "/tmp/fluxmq_bench";

    printf("=== FluxMQ Storage Engine Benchmark ===\n\n");

    bench_append(bench_dir, 1'000'000, 100);
    bench_append(bench_dir, 100'000, 1024);
    bench_read(bench_dir, 1'000'000, 100);

    printf("\nDone.\n");
    return 0;
}
