#pragma once

#include "connection.h"
#include "reactor.h"

#include <atomic>
#include <cstdint>
#include <future>
#include <unordered_map>

// Single-threaded TCP server backed by an epoll (Linux) / kqueue (macOS) reactor.
//
// Request handling is synchronous on the I/O thread for Phase 2.  Phase 3 will
// dispatch heavy work (produce / fetch) to a worker thread pool.
//
// Typical usage:
//   Server srv(echo_handler);
//   std::thread t([&]{ srv.Run(9092); });
//   uint16_t port = srv.WaitReady();  // blocks until bound and listening
//   // ... send requests from another thread ...
//   srv.Stop();
//   t.join();
class Server
{
  public:
    explicit Server(RequestHandler handler);
    ~Server();

    Server(const Server &) = delete;
    Server &operator=(const Server &) = delete;

    // Bind to port (0 = OS-assigned) and enter the reactor loop.
    // Blocks until Stop() is called.
    void Run(uint16_t port = 9092);

    // Thread-safe.  Wakes the reactor loop and causes Run() to return.
    void Stop();

    // Block until the server has bound its listen socket and entered the loop.
    // Returns the actual listening port (useful when port = 0 in tests).
    uint16_t WaitReady();

  private:
    void AcceptNew();
    void HandleEvent(const PollEvent &ev);
    void CloseConn(int fd);

    RequestHandler handler_;
    Reactor reactor_;
    int listen_fd_{-1};

    // Self-pipe used by Stop() to wake epoll_wait / kevent.
    int stop_pipe_[2]{-1, -1};

    std::unordered_map<int, Connection *> connections_;

    std::atomic<bool> running_{false};

    // Promise/future pair so WaitReady() can block until Run() has bound.
    std::promise<uint16_t> ready_promise_;
    std::future<uint16_t> ready_future_{ready_promise_.get_future()};
};
