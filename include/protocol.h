#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

// ─── API keys ─────────────────────────────────────────────────────────────────
// The API key identifies the request type — identical to Kafka's numbering so
// the design is recognisable in interviews.
namespace api
{
inline constexpr uint16_t kProduce = 0;
inline constexpr uint16_t kFetch = 1;
inline constexpr uint16_t kCreateTopic = 2;
inline constexpr uint16_t kMetadata = 3;
inline constexpr uint16_t kJoinGroup = 4;
inline constexpr uint16_t kSyncGroup = 5;
inline constexpr uint16_t kHeartbeat = 6;
inline constexpr uint16_t kOffsetCommit = 7;
inline constexpr uint16_t kOffsetFetch = 8;
inline constexpr uint16_t kLeaveGroup = 9;
} // namespace api

// ─── Frame types ──────────────────────────────────────────────────────────────

// Decoded request frame (all fields in host byte order).
struct RequestFrame
{
    uint16_t api_key{0};
    uint16_t api_version{0};
    uint32_t correlation_id{0};
    std::vector<uint8_t> payload;
};

// Response frame to be encoded and sent back.
struct ResponseFrame
{
    uint32_t correlation_id{0};
    std::vector<uint8_t> payload;
};

// ─── Wire layout (all integers big-endian) ────────────────────────────────────
//
// Request:
//   [4B total_len][2B api_key][2B api_version][4B correlation_id][payload...]
//   total_len = 8 + payload.size()   (does NOT include the 4-byte total_len itself)
//
// Response:
//   [4B total_len][4B correlation_id][payload...]
//   total_len = 4 + payload.size()

inline constexpr size_t kFrameLenSize = 4;    // leading Total Len field
inline constexpr size_t kReqBodyHdrSize = 8;  // api_key + api_ver + corr_id
inline constexpr size_t kRespBodyHdrSize = 4; // corr_id

// ─── Big-endian encode / decode helpers ──────────────────────────────────────

inline uint32_t DecBe32(const uint8_t *p)
{
    return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) | (static_cast<uint32_t>(p[2]) << 8) |
           static_cast<uint32_t>(p[3]);
}

inline uint16_t DecBe16(const uint8_t *p)
{
    return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
}

inline void EncBe32(uint8_t *p, uint32_t v)
{
    p[0] = static_cast<uint8_t>(v >> 24);
    p[1] = static_cast<uint8_t>(v >> 16);
    p[2] = static_cast<uint8_t>(v >> 8);
    p[3] = static_cast<uint8_t>(v);
}

inline void EncBe16(uint8_t *p, uint16_t v)
{
    p[0] = static_cast<uint8_t>(v >> 8);
    p[1] = static_cast<uint8_t>(v);
}

// Encode a RequestFrame into wire bytes.
inline std::vector<uint8_t> EncodeRequest(const RequestFrame &req)
{
    size_t payload_size = req.payload.size();
    uint32_t total_len = static_cast<uint32_t>(kReqBodyHdrSize + payload_size);
    std::vector<uint8_t> out(kFrameLenSize + kReqBodyHdrSize + payload_size);
    EncBe32(out.data(), total_len);
    EncBe16(out.data() + 4, req.api_key);
    EncBe16(out.data() + 6, req.api_version);
    EncBe32(out.data() + 8, req.correlation_id);
    if (!req.payload.empty()) {
        std::copy(req.payload.begin(), req.payload.end(), out.data() + 12);
    }
    return out;
}

// Encode a ResponseFrame into wire bytes.
inline std::vector<uint8_t> EncodeResponse(const ResponseFrame &resp)
{
    size_t payload_size = resp.payload.size();
    uint32_t total_len = static_cast<uint32_t>(kRespBodyHdrSize + payload_size);
    std::vector<uint8_t> out(kFrameLenSize + kRespBodyHdrSize + payload_size);
    EncBe32(out.data(), total_len);
    EncBe32(out.data() + 4, resp.correlation_id);
    if (!resp.payload.empty()) {
        std::copy(resp.payload.begin(), resp.payload.end(), out.data() + 8);
    }
    return out;
}
