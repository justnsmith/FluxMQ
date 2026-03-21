#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

// BinaryWriter — append big-endian integers and length-prefixed strings to a byte buffer.
class BinaryWriter
{
  public:
    explicit BinaryWriter(std::vector<uint8_t> &buf) : buf_(buf)
    {
    }

    void WriteI16(int16_t v)
    {
        WriteU16(static_cast<uint16_t>(v));
    }
    void WriteU16(uint16_t v)
    {
        buf_.push_back(static_cast<uint8_t>(v >> 8));
        buf_.push_back(static_cast<uint8_t>(v));
    }

    void WriteI32(int32_t v)
    {
        WriteU32(static_cast<uint32_t>(v));
    }
    void WriteU32(uint32_t v)
    {
        buf_.push_back(static_cast<uint8_t>(v >> 24));
        buf_.push_back(static_cast<uint8_t>(v >> 16));
        buf_.push_back(static_cast<uint8_t>(v >> 8));
        buf_.push_back(static_cast<uint8_t>(v));
    }

    void WriteU64(uint64_t v)
    {
        WriteU32(static_cast<uint32_t>(v >> 32));
        WriteU32(static_cast<uint32_t>(v));
    }

    // 2B length-prefixed string.
    void WriteString(std::string_view s)
    {
        WriteU16(static_cast<uint16_t>(s.size()));
        buf_.insert(buf_.end(), s.begin(), s.end());
    }

    void WriteBytes(const uint8_t *data, size_t n)
    {
        buf_.insert(buf_.end(), data, data + n);
    }

  private:
    std::vector<uint8_t> &buf_;
};

// BinaryReader — read big-endian integers and length-prefixed strings from a byte vector.
class BinaryReader
{
  public:
    explicit BinaryReader(const std::vector<uint8_t> &v) : data_(v.data()), len_(v.size())
    {
    }

    int16_t ReadI16()
    {
        return static_cast<int16_t>(ReadU16());
    }
    uint16_t ReadU16()
    {
        Check(2);
        uint16_t v = (static_cast<uint16_t>(data_[pos_]) << 8) | data_[pos_ + 1];
        pos_ += 2;
        return v;
    }

    int32_t ReadI32()
    {
        return static_cast<int32_t>(ReadU32());
    }
    uint32_t ReadU32()
    {
        Check(4);
        uint32_t v = (static_cast<uint32_t>(data_[pos_]) << 24) | (static_cast<uint32_t>(data_[pos_ + 1]) << 16) |
                     (static_cast<uint32_t>(data_[pos_ + 2]) << 8) | static_cast<uint32_t>(data_[pos_ + 3]);
        pos_ += 4;
        return v;
    }

    uint64_t ReadU64()
    {
        uint64_t hi = ReadU32();
        uint64_t lo = ReadU32();
        return (hi << 32) | lo;
    }

    // Read 2B length-prefixed string.
    std::string ReadString()
    {
        uint16_t len = ReadU16();
        Check(len);
        std::string s(reinterpret_cast<const char *>(data_ + pos_), len);
        pos_ += len;
        return s;
    }

    // Read n raw bytes.
    std::vector<uint8_t> ReadBytes(uint32_t n)
    {
        Check(n);
        std::vector<uint8_t> v(data_ + pos_, data_ + pos_ + n);
        pos_ += n;
        return v;
    }

    bool HasRemaining() const
    {
        return pos_ < len_;
    }
    size_t Remaining() const
    {
        return pos_ < len_ ? len_ - pos_ : 0;
    }
    bool Ok() const
    {
        return ok_;
    }

  private:
    void Check(size_t n)
    {
        if (pos_ + n > len_) {
            ok_ = false;
            throw std::runtime_error("BinaryReader: buffer underflow");
        }
    }

    const uint8_t *data_;
    size_t len_;
    size_t pos_{0};
    bool ok_{true};
};
