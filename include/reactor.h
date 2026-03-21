#pragma once

#include <unordered_map>
#include <utility>
#include <vector>

// Event fired by the reactor for a single file descriptor.
struct PollEvent
{
    int fd{-1};
    bool readable{false};
    bool writable{false};
    bool error{false}; // includes EOF / HUP
};

// Thin portability wrapper over epoll (Linux) and kqueue (macOS/BSD).
//
// Usage:
//   Reactor r;
//   r.Add(fd, /*read=*/true, /*write=*/false);
//   while (running) {
//       for (auto& ev : r.Poll(100)) { /* handle ev */ }
//   }
//
// EPOLLOUT / EVFILT_WRITE should only be registered when there is actually
// data to send — otherwise the reactor busy-loops on writable events.
class Reactor
{
  public:
    Reactor();
    ~Reactor();

    Reactor(const Reactor &) = delete;
    Reactor &operator=(const Reactor &) = delete;

    // Register a new fd.  Must be called before Modify/Remove.
    void Add(int fd, bool watch_read, bool watch_write);

    // Update the watched events for an already-registered fd.
    void Modify(int fd, bool watch_read, bool watch_write);

    // Deregister fd.  Safe to call even if fd was never registered.
    void Remove(int fd);

    // Block up to timeout_ms for events (-1 = infinite).
    std::vector<PollEvent> Poll(int timeout_ms);

  private:
    int poll_fd_{-1};

#if !defined(__linux__)
    // kqueue tracks read and write filters separately; we need per-fd state to
    // issue the correct EV_ADD / EV_DELETE commands on Modify.
    std::unordered_map<int, std::pair<bool, bool>> state_; // fd → {read, write}
#endif
};
