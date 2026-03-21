#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

// Byte buffer backed by a dynamically-growing vector with a separate read cursor.
// Used for both receive (accumulating partial socket reads) and send (queuing
// pending writes that didn't fit in the kernel socket buffer).
//
// Compact() is called automatically when the consumed prefix exceeds half the
// allocated space, so amortized memory usage is proportional to the live data.
class Buffer
{
  public:
    // Append n bytes from src to the write end.
    void Append(const void *src, size_t n);

    // Number of bytes available to read.
    size_t Readable() const
    {
        return data_.size() - rpos_;
    }

    // Pointer to the first unconsumed byte.
    const uint8_t *Data() const
    {
        return data_.data() + rpos_;
    }

    // Advance the read cursor by n bytes (mark as consumed).
    void Consume(size_t n);

    bool Empty() const
    {
        return Readable() == 0;
    }

    void Clear()
    {
        data_.clear();
        rpos_ = 0;
    }

  private:
    void MaybeCompact();

    std::vector<uint8_t> data_;
    size_t rpos_{0};
};
