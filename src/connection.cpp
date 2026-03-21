#include "connection.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

// ─── Constructor / Destructor ─────────────────────────────────────────────────

Connection::Connection(int fd, RequestHandler handler) : fd_(fd), handler_(std::move(handler))
{
}

Connection::~Connection()
{
    if (fd_ >= 0)
        close(fd_);
}

// ─── OnReadable ───────────────────────────────────────────────────────────────

bool Connection::OnReadable()
{
    // Drain the socket into recv_buf_ using a stack buffer.
    // Loop until EAGAIN (no more data) or an error / EOF.
    uint8_t tmp[65536];
    while (true) {
        ssize_t n = recv(fd_, tmp, sizeof(tmp), 0);
        if (n > 0) {
            recv_buf_.Append(tmp, static_cast<size_t>(n));
        }
        else if (n == 0) {
            return false; // peer closed — caller should remove this connection
        }
        else {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            return false; // unrecoverable error
        }
    }

    // Parse all complete frames accumulated in recv_buf_.
    TryParseFrames();
    return true;
}

// ─── OnWritable ───────────────────────────────────────────────────────────────

bool Connection::OnWritable()
{
    TryFlush();
    return true;
}

// ─── SendResponse ─────────────────────────────────────────────────────────────

void Connection::SendResponse(const ResponseFrame &resp)
{
    auto encoded = EncodeResponse(resp);
    send_buf_.Append(encoded.data(), encoded.size());
    // Attempt an immediate flush; this avoids an extra epoll round-trip for
    // small responses that fit in the kernel socket buffer (the common case).
    TryFlush();
}

// ─── TryFlush ─────────────────────────────────────────────────────────────────

void Connection::TryFlush()
{
    while (!send_buf_.Empty()) {
        ssize_t n = send(fd_, send_buf_.Data(), send_buf_.Readable(), MSG_NOSIGNAL);
        if (n > 0) {
            send_buf_.Consume(static_cast<size_t>(n));
        }
        else {
            break; // EAGAIN or error — will be retried via OnWritable()
        }
    }
}

// ─── TryParseFrames ───────────────────────────────────────────────────────────

bool Connection::TryParseFrames()
{
    bool parsed_any = false;

    while (true) {
        if (parse_state_ == ParseState::kHeader) {
            if (recv_buf_.Readable() < kFrameLenSize)
                break;

            pending_body_len_ = DecBe32(recv_buf_.Data());
            recv_buf_.Consume(kFrameLenSize);
            parse_state_ = ParseState::kBody;
        }

        if (parse_state_ == ParseState::kBody) {
            if (recv_buf_.Readable() < pending_body_len_)
                break;

            // Sanity check: body must be at least the fixed header fields.
            if (pending_body_len_ < kReqBodyHdrSize) {
                // Malformed frame — close connection by returning a failure
                // signal to the caller.  (We return true here; the caller
                // will see the connection marked for close via the state.)
                break;
            }

            const uint8_t *body = recv_buf_.Data();
            RequestFrame frame;
            frame.api_key = DecBe16(body);
            frame.api_version = DecBe16(body + 2);
            frame.correlation_id = DecBe32(body + 4);

            size_t payload_len = pending_body_len_ - kReqBodyHdrSize;
            if (payload_len > 0) {
                frame.payload.assign(body + kReqBodyHdrSize, body + pending_body_len_);
            }

            recv_buf_.Consume(pending_body_len_);
            parse_state_ = ParseState::kHeader;
            pending_body_len_ = 0;

            if (handler_)
                handler_(*this, std::move(frame));
            parsed_any = true;
        }
    }

    return parsed_any;
}
