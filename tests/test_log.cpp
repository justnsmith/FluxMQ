#include "log.h"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <span>
#include <stdexcept>
#include <string>

// ---------------------------------------------------------------------------
// Minimal test framework
// ---------------------------------------------------------------------------

static int g_passed = 0;
static int g_failed = 0;

#define RUN_TEST(fn)                                                                                                                       \
    do {                                                                                                                                   \
        printf("  %-45s", #fn " ...");                                                                                                     \
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

#define CHECK_STR_EQ(a, b)                                                                                                                 \
    do {                                                                                                                                   \
        std::string _sa = (a);                                                                                                             \
        std::string _sb = (b);                                                                                                             \
        if (_sa != _sb) {                                                                                                                  \
            throw std::runtime_error("CHECK_STR_EQ failed at line " + std::to_string(__LINE__) + ": got \"" + _sa + "\" expected \"" +     \
                                     _sb + "\"");                                                                                          \
        }                                                                                                                                  \
    } while (0)

// ---------------------------------------------------------------------------
// Temp directory helpers
// ---------------------------------------------------------------------------

static std::string make_temp_dir()
{
    // Use a nanosecond timestamp to get a unique directory name without mkdtemp.
    auto ns = std::chrono::steady_clock::now().time_since_epoch().count();
    auto path = std::filesystem::temp_directory_path() / ("fluxmq_test_" + std::to_string(ns));
    std::filesystem::create_directories(path);
    return path.string();
}

static void remove_temp_dir(const std::string &path)
{
    std::filesystem::remove_all(path);
}

// Helpers for converting between string and span<const byte>
static std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

static std::string as_string(const std::vector<std::byte> &v)
{
    return {reinterpret_cast<const char *>(v.data()), v.size()};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

static void test_basic_append_and_read()
{
    auto dir = make_temp_dir();

    {
        Log log(dir);
        uint64_t o0 = log.Append(as_bytes("hello"));
        uint64_t o1 = log.Append(as_bytes("world"));
        uint64_t o2 = log.Append(as_bytes("fluxmq"));

        CHECK_EQ(o0, 0U);
        CHECK_EQ(o1, 1U);
        CHECK_EQ(o2, 2U);
        CHECK_EQ(log.NextOffset(), 3U);

        CHECK_STR_EQ(as_string(log.Read(0)), "hello");
        CHECK_STR_EQ(as_string(log.Read(1)), "world");
        CHECK_STR_EQ(as_string(log.Read(2)), "fluxmq");

        // Out-of-range reads return empty.
        CHECK(log.Read(3).empty());
        CHECK(log.Read(UINT64_MAX).empty());
    }

    remove_temp_dir(dir);
}

static void test_persistence_across_restart()
{
    auto dir = make_temp_dir();

    // Write some records.
    {
        Log log(dir);
        log.Append(as_bytes("persistent-0"));
        log.Append(as_bytes("persistent-1"));
        log.Append(as_bytes("persistent-2"));
    }

    // Reopen: state must be fully recovered.
    {
        Log log(dir);
        CHECK_EQ(log.NextOffset(), 3U);
        CHECK_STR_EQ(as_string(log.Read(0)), "persistent-0");
        CHECK_STR_EQ(as_string(log.Read(1)), "persistent-1");
        CHECK_STR_EQ(as_string(log.Read(2)), "persistent-2");

        // Append after recovery continues from the correct offset.
        uint64_t o3 = log.Append(as_bytes("persistent-3"));
        CHECK_EQ(o3, 3U);
        CHECK_STR_EQ(as_string(log.Read(3)), "persistent-3");
    }

    remove_temp_dir(dir);
}

static void test_segment_rotation()
{
    auto dir = make_temp_dir();

    // Small segment size so rotation happens quickly.
    // Each record is kRecordHeaderSize(8) + 100 = 108 bytes.
    // 5 records = 540 bytes.  Set max to 400 so rotation at record 4.
    const uint64_t kSmallSeg = 400;

    {
        Log log(dir, kSmallSeg);

        std::string payload(100, 'z');
        for (int i = 0; i < 10; i++) {
            log.Append(as_bytes(payload));
        }

        // With 108-byte records and 400-byte segments, we expect > 1 segment.
        CHECK(log.NumSegments() > 1);
    }

    // Recovery should restore all 10 records regardless of segment count.
    {
        Log log(dir, kSmallSeg);
        CHECK_EQ(log.NextOffset(), 10U);
        for (int i = 0; i < 10; i++) {
            auto result = log.Read(static_cast<uint64_t>(i));
            CHECK(!result.empty());
            CHECK_EQ(result.size(), 100U);
        }
    }

    remove_temp_dir(dir);
}

static void test_sparse_index_lookups()
{
    // Write enough records to span several index entries (kIndexIntervalBytes = 4096).
    // Each record is 8 + 200 = 208 bytes.  ~20 records per index interval.
    auto dir = make_temp_dir();

    const int kRecords = 200;
    std::string payload(200, 'i');

    {
        Log log(dir);
        for (int i = 0; i < kRecords; i++) {
            log.Append(as_bytes(payload));
        }
    }

    {
        Log log(dir);
        CHECK_EQ(log.NextOffset(), static_cast<uint64_t>(kRecords));

        // Read every record including those that require a linear scan past an index entry.
        for (int i = 0; i < kRecords; i++) {
            auto result = log.Read(static_cast<uint64_t>(i));
            CHECK(!result.empty());
            CHECK_EQ(result.size(), 200U);
        }
    }

    remove_temp_dir(dir);
}

static void test_crc_corruption_detection()
{
    auto dir = make_temp_dir();

    {
        Log log(dir);
        log.Append(as_bytes("uncorrupted"));
        log.Append(as_bytes("corruptme"));
        log.Append(as_bytes("also-fine"));
    }

    // Corrupt the payload of record 1 by flipping a byte in the .log file.
    // Record 0: [4B len=11][4B crc][11B payload] = 19 bytes at offset 0.
    // Record 1 starts at byte 19.  Payload starts at byte 19+8=27.
    {
        auto log_path = std::filesystem::path(dir) / "00000000000000000000.log";
        FILE *f = fopen(log_path.c_str(), "r+b");
        if (!f)
            throw std::runtime_error("cannot open log file for corruption");
        fseek(f, 27, SEEK_SET);
        char bad = 0xFF;
        fwrite(&bad, 1, 1, f);
        fclose(f);
    }

    // Re-read: corrupted record returns empty, clean records are intact.
    {
        Log log(dir);
        CHECK_STR_EQ(as_string(log.Read(0)), "uncorrupted");
        CHECK(log.Read(1).empty()); // CRC mismatch
        CHECK_STR_EQ(as_string(log.Read(2)), "also-fine");
    }

    remove_temp_dir(dir);
}

static void test_large_append_correctness()
{
    // Append 1M small records and spot-check reads.
    auto dir = make_temp_dir();

    const int kN = 1'000'000;
    std::string payload(100, 'a');

    {
        Log log(dir);
        for (int i = 0; i < kN; i++) {
            uint64_t off = log.Append(as_bytes(payload));
            CHECK_EQ(off, static_cast<uint64_t>(i));
        }
        CHECK_EQ(log.NextOffset(), static_cast<uint64_t>(kN));
    }

    // Verify correctness after recovery.
    {
        Log log(dir);
        CHECK_EQ(log.NextOffset(), static_cast<uint64_t>(kN));

        // Spot-check: first, last, and every 50,000th record.
        for (int i = 0; i < kN; i += 50'000) {
            auto result = log.Read(static_cast<uint64_t>(i));
            CHECK(!result.empty());
            CHECK_EQ(result.size(), 100U);
        }
        // Last record
        auto last = log.Read(static_cast<uint64_t>(kN - 1));
        CHECK(!last.empty());
        CHECK_EQ(last.size(), 100U);
    }

    remove_temp_dir(dir);
}

static void test_delete_before()
{
    auto dir = make_temp_dir();

    // Force multiple segments with a small limit.
    const uint64_t kSmallSeg = 200; // fits ~2 records per segment (8+90=98 bytes each)

    {
        Log log(dir, kSmallSeg);
        std::string payload(90, 'd');
        for (int i = 0; i < 8; i++) {
            log.Append(as_bytes(payload));
        }
        CHECK(log.NumSegments() > 2);

        // Keep only the last two segments worth of data.
        size_t before = log.NumSegments();
        log.DeleteBefore(4); // delete segments entirely before offset 4
        CHECK(log.NumSegments() < before);
    }

    remove_temp_dir(dir);
}

static void test_empty_log_read()
{
    // Reading from a brand-new, empty log should return empty results.
    auto dir = make_temp_dir();

    {
        Log log(dir);
        CHECK_EQ(log.NextOffset(), 0U);
        CHECK(log.Read(0).empty());
        CHECK(log.Read(1).empty());
        CHECK(log.Read(UINT64_MAX).empty());
    }

    remove_temp_dir(dir);
}

static void test_zero_length_record()
{
    // Appending an empty payload should work and be recoverable.
    auto dir = make_temp_dir();

    {
        Log log(dir);
        uint64_t o0 = log.Append(as_bytes(""));
        uint64_t o1 = log.Append(as_bytes("nonempty"));
        uint64_t o2 = log.Append(as_bytes(""));

        CHECK_EQ(o0, 0U);
        CHECK_EQ(o1, 1U);
        CHECK_EQ(o2, 2U);

        auto r0 = log.Read(0);
        CHECK(r0.empty() || r0.size() == 0U); // zero-length payload
        CHECK_STR_EQ(as_string(log.Read(1)), "nonempty");
    }

    // Verify recovery handles zero-length records.
    {
        Log log(dir);
        CHECK_EQ(log.NextOffset(), 3U);
        CHECK_STR_EQ(as_string(log.Read(1)), "nonempty");
    }

    remove_temp_dir(dir);
}

static void test_exact_segment_boundary()
{
    // Records that fill a segment exactly to max_seg_bytes should rotate
    // cleanly without data loss.
    auto dir = make_temp_dir();

    // Each record: 4B len + 4B CRC + payload = 8 + payload_size.
    // Set segment size so that exactly 2 records fit per segment.
    const size_t kPayloadSize = 96;
    const uint64_t kSegSize = 2 * (8 + kPayloadSize); // exactly 2 records

    {
        Log log(dir, kSegSize);
        std::string payload(kPayloadSize, 'b');

        // Write 6 records → should produce exactly 3 segments.
        for (int i = 0; i < 6; i++) {
            log.Append(as_bytes(payload));
        }

        CHECK_EQ(log.NextOffset(), 6U);
        CHECK(log.NumSegments() >= 3);

        // All records readable.
        for (int i = 0; i < 6; i++) {
            auto r = log.Read(static_cast<uint64_t>(i));
            CHECK(!r.empty());
            CHECK_EQ(r.size(), kPayloadSize);
        }
    }

    // Recovery must restore all.
    {
        Log log(dir, kSegSize);
        CHECK_EQ(log.NextOffset(), 6U);
    }

    remove_temp_dir(dir);
}

static void test_append_after_delete_before()
{
    // After deleting old segments, new appends must continue with correct offsets.
    auto dir = make_temp_dir();
    const uint64_t kSmallSeg = 200;

    {
        Log log(dir, kSmallSeg);
        std::string payload(90, 'x');

        for (int i = 0; i < 10; i++) {
            log.Append(as_bytes(payload));
        }
        CHECK_EQ(log.NextOffset(), 10U);

        log.DeleteBefore(6);

        // Append more after deletion.
        uint64_t o10 = log.Append(as_bytes("after-delete"));
        CHECK_EQ(o10, 10U);
        CHECK_STR_EQ(as_string(log.Read(10)), "after-delete");
    }

    remove_temp_dir(dir);
}

static void test_multiple_segment_recovery()
{
    // Write records across many segments, close, reopen, verify all data intact.
    auto dir = make_temp_dir();
    const uint64_t kSmallSeg = 200;
    const int kRecords = 50;

    {
        Log log(dir, kSmallSeg);
        for (int i = 0; i < kRecords; i++) {
            std::string payload = "record-" + std::to_string(i);
            log.Append(as_bytes(payload));
        }
        CHECK(log.NumSegments() > 2);
    }

    // Reopen and verify every record.
    {
        Log log(dir, kSmallSeg);
        CHECK_EQ(log.NextOffset(), static_cast<uint64_t>(kRecords));
        for (int i = 0; i < kRecords; i++) {
            std::string expected = "record-" + std::to_string(i);
            CHECK_STR_EQ(as_string(log.Read(static_cast<uint64_t>(i))), expected);
        }
    }

    remove_temp_dir(dir);
}

static void test_single_byte_records()
{
    // Verify correctness with the smallest possible non-empty payload.
    auto dir = make_temp_dir();

    {
        Log log(dir);
        for (int i = 0; i < 256; i++) {
            std::string s(1, static_cast<char>(i));
            uint64_t off = log.Append(as_bytes(s));
            CHECK_EQ(off, static_cast<uint64_t>(i));
        }

        for (int i = 0; i < 256; i++) {
            auto r = log.Read(static_cast<uint64_t>(i));
            CHECK_EQ(r.size(), 1U);
            CHECK_EQ(static_cast<unsigned char>(r[0]), static_cast<unsigned char>(i));
        }
    }

    remove_temp_dir(dir);
}

static void test_large_record()
{
    // Single record larger than a typical segment index interval.
    auto dir = make_temp_dir();

    {
        Log log(dir);
        std::string big(1024 * 1024, 'L'); // 1 MB record
        uint64_t off = log.Append(as_bytes(big));
        CHECK_EQ(off, 0U);

        auto result = log.Read(0);
        CHECK_EQ(result.size(), big.size());
        // Spot-check first and last bytes.
        CHECK_EQ(static_cast<char>(result.front()), 'L');
        CHECK_EQ(static_cast<char>(result.back()), 'L');
    }

    remove_temp_dir(dir);
}

static void test_interleaved_append_read()
{
    // Interleave appends and reads to verify read-after-write consistency.
    auto dir = make_temp_dir();

    {
        Log log(dir);
        for (int i = 0; i < 100; i++) {
            std::string payload = "interleaved-" + std::to_string(i);
            uint64_t off = log.Append(as_bytes(payload));
            CHECK_EQ(off, static_cast<uint64_t>(i));

            // Immediately read back.
            CHECK_STR_EQ(as_string(log.Read(off)), payload);

            // Also re-read a previous record if available.
            if (i > 0) {
                std::string prev = "interleaved-" + std::to_string(i - 1);
                CHECK_STR_EQ(as_string(log.Read(static_cast<uint64_t>(i - 1))), prev);
            }
        }
    }

    remove_temp_dir(dir);
}

static void test_crc_corruption_first_record()
{
    // Corrupt the very first record; subsequent records must still be readable.
    auto dir = make_temp_dir();

    {
        Log log(dir);
        log.Append(as_bytes("first"));
        log.Append(as_bytes("second"));
        log.Append(as_bytes("third"));
    }

    // Corrupt byte in first record's payload (offset 8 in file = past header).
    {
        auto log_path = std::filesystem::path(dir) / "00000000000000000000.log";
        FILE *f = fopen(log_path.c_str(), "r+b");
        if (!f)
            throw std::runtime_error("cannot open log file");
        fseek(f, 8, SEEK_SET); // first byte of first record's payload
        char bad = 0xFF;
        fwrite(&bad, 1, 1, f);
        fclose(f);
    }

    {
        Log log(dir);
        CHECK(log.Read(0).empty()); // corrupted
        CHECK_STR_EQ(as_string(log.Read(1)), "second");
        CHECK_STR_EQ(as_string(log.Read(2)), "third");
    }

    remove_temp_dir(dir);
}

static void test_crc_corruption_last_record()
{
    // Corrupt the last record; earlier records must be intact.
    auto dir = make_temp_dir();

    {
        Log log(dir);
        log.Append(as_bytes("aaa"));
        log.Append(as_bytes("bbb"));
        log.Append(as_bytes("ccc")); // offset 2
    }

    // Record layout: each is 8 + 3 = 11 bytes. Record 2 starts at byte 22.
    // Corrupt payload at byte 22 + 8 = 30.
    {
        auto log_path = std::filesystem::path(dir) / "00000000000000000000.log";
        FILE *f = fopen(log_path.c_str(), "r+b");
        if (!f)
            throw std::runtime_error("cannot open log file");
        fseek(f, 30, SEEK_SET);
        char bad = 0x00;
        fwrite(&bad, 1, 1, f);
        fclose(f);
    }

    {
        Log log(dir);
        CHECK_STR_EQ(as_string(log.Read(0)), "aaa");
        CHECK_STR_EQ(as_string(log.Read(1)), "bbb");
        CHECK(log.Read(2).empty()); // corrupted
    }

    remove_temp_dir(dir);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main()
{
    printf("=== FluxMQ Storage Engine Tests ===\n\n");

    RUN_TEST(test_basic_append_and_read);
    RUN_TEST(test_persistence_across_restart);
    RUN_TEST(test_segment_rotation);
    RUN_TEST(test_sparse_index_lookups);
    RUN_TEST(test_crc_corruption_detection);
    RUN_TEST(test_large_append_correctness);
    RUN_TEST(test_delete_before);
    RUN_TEST(test_empty_log_read);
    RUN_TEST(test_zero_length_record);
    RUN_TEST(test_exact_segment_boundary);
    RUN_TEST(test_append_after_delete_before);
    RUN_TEST(test_multiple_segment_recovery);
    RUN_TEST(test_single_byte_records);
    RUN_TEST(test_large_record);
    RUN_TEST(test_interleaved_append_read);
    RUN_TEST(test_crc_corruption_first_record);
    RUN_TEST(test_crc_corruption_last_record);

    printf("\n%d passed, %d failed\n", g_passed, g_failed);
    return g_failed > 0 ? 1 : 0;
}
