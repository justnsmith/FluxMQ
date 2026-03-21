#pragma once

#include "buffer.h"
#include "protocol.h"

#include <functional>

// Handler called for each complete, decoded request frame.
using RequestHandler = std::function<void(class Connection &, RequestFrame)>;

// Manages a single non-blocking TCP connection.
//
// OnReadable / OnWritable are called by the Server's reactor loop whenever the
// kernel reports the socket is readable / writable.  They return false when
// the connection should be closed (peer EOF or unrecoverable error), and true
// to continue.
//
// SendResponse() encodes a response and attempts an immediate flush.  Any bytes
// not delivered immediately are held in send_buf_; the caller should register
// EPOLLOUT / EVFILT_WRITE until WantWrite() returns false.
class Connection
{
  public:
    Connection(int fd, RequestHandler handler);
    ~Connection();

    Connection(const Connection &) = delete;
    Connection &operator=(const Connection &) = delete;

    // Called when the socket is readable.  Returns false → close this connection.
    bool OnReadable();

    // Called when the socket is writable (flush pending send buffer).
    // Returns false → close this connection.
    bool OnWritable();

    // Encode resp and queue it for sending (attempts an immediate flush first).
    void SendResponse(const ResponseFrame &resp);

    // True if there are bytes waiting to be sent (caller should register EPOLLOUT).
    bool WantWrite() const
    {
        return !send_buf_.Empty();
    }

    int Fd() const
    {
        return fd_;
    }

  private:
    // Parse as many complete frames from recv_buf_ as possible.
    // Returns true if at least one frame was consumed.
    bool TryParseFrames();

    // Try to flush send_buf_ to the socket.  Does not block.
    void TryFlush();

    int fd_;
    RequestHandler handler_;

    Buffer recv_buf_;
    Buffer send_buf_;

    // Frame parser state machine.
    enum class ParseState
    {
        kHeader, // waiting for the 4-byte Total Len field
        kBody,   // waiting for total_len body bytes
    };
    ParseState parse_state_{ParseState::kHeader};
    uint32_t pending_body_len_{0};
};
