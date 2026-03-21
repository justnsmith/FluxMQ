#include "buffer.h"

#include <cstring>
#include <stdexcept>

void Buffer::Append(const void *src, size_t n)
{
    const auto *p = static_cast<const uint8_t *>(src);
    data_.insert(data_.end(), p, p + n);
}

void Buffer::Consume(size_t n)
{
    if (n > Readable()) {
        throw std::logic_error("Buffer::Consume: n exceeds available bytes");
    }
    rpos_ += n;
    MaybeCompact();
}

void Buffer::MaybeCompact()
{
    // Compact when the consumed prefix is at least half the allocation.
    // Amortises the O(n) erase cost to O(1) per Consume call on average.
    if (rpos_ > 0 && rpos_ >= data_.size() / 2) {
        data_.erase(data_.begin(), data_.begin() + static_cast<std::ptrdiff_t>(rpos_));
        rpos_ = 0;
    }
}
