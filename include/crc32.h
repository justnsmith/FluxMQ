#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

// CRC32 (ISO 3309 / ITU-T V.42) with a compile-time lookup table.
// Used to detect on-disk record corruption.
namespace crc32
{

namespace detail
{

constexpr uint32_t kPoly = 0xEDB88320U;

constexpr uint32_t make_entry(uint32_t i)
{
    uint32_t crc = i;
    for (int j = 0; j < 8; j++) {
        crc = (crc & 1U) ? ((crc >> 1) ^ kPoly) : (crc >> 1);
    }
    return crc;
}

constexpr std::array<uint32_t, 256> make_table()
{
    std::array<uint32_t, 256> t{};
    for (std::size_t i = 0; i < 256; i++) {
        t[i] = make_entry(static_cast<uint32_t>(i));
    }
    return t;
}

// Pre-computed at compile time; avoids any runtime table-init cost.
inline constexpr std::array<uint32_t, 256> kTable = make_table();

} // namespace detail

inline uint32_t compute(const uint8_t *data, std::size_t length)
{
    uint32_t crc = 0xFFFFFFFFU;
    for (std::size_t i = 0; i < length; i++) {
        crc = (crc >> 8) ^ detail::kTable[(crc ^ data[i]) & 0xFFU];
    }
    return crc ^ 0xFFFFFFFFU;
}

inline uint32_t compute(const std::byte *data, std::size_t length)
{
    return compute(reinterpret_cast<const uint8_t *>(data), length);
}

} // namespace crc32
