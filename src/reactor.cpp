#include "reactor.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <unistd.h>

#ifdef __linux__
#include <sys/epoll.h>
#else
#include <sys/event.h>
#include <sys/time.h>
#endif

// ─── Constructor / Destructor ─────────────────────────────────────────────────

Reactor::Reactor()
{
#ifdef __linux__
    poll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (poll_fd_ < 0) {
        throw std::runtime_error(std::string("epoll_create1: ") + strerror(errno));
    }
#else
    poll_fd_ = kqueue();
    if (poll_fd_ < 0) {
        throw std::runtime_error(std::string("kqueue: ") + strerror(errno));
    }
#endif
}

Reactor::~Reactor()
{
    if (poll_fd_ >= 0) {
        close(poll_fd_);
    }
}

// ─── Add ──────────────────────────────────────────────────────────────────────

void Reactor::Add(int fd, bool watch_read, bool watch_write)
{
#ifdef __linux__
    epoll_event ev{};
    ev.data.fd = fd;
    ev.events = EPOLLERR | EPOLLHUP;
    if (watch_read)
        ev.events |= EPOLLIN;
    if (watch_write)
        ev.events |= EPOLLOUT;
    epoll_ctl(poll_fd_, EPOLL_CTL_ADD, fd, &ev);
#else
    state_[fd] = {watch_read, watch_write};
    struct kevent changes[2];
    int n = 0;
    if (watch_read)
        EV_SET(&changes[n++], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
    if (watch_write)
        EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, nullptr);
    if (n > 0)
        kevent(poll_fd_, changes, n, nullptr, 0, nullptr);
#endif
}

// ─── Modify ───────────────────────────────────────────────────────────────────

void Reactor::Modify(int fd, bool watch_read, bool watch_write)
{
#ifdef __linux__
    epoll_event ev{};
    ev.data.fd = fd;
    ev.events = EPOLLERR | EPOLLHUP;
    if (watch_read)
        ev.events |= EPOLLIN;
    if (watch_write)
        ev.events |= EPOLLOUT;
    epoll_ctl(poll_fd_, EPOLL_CTL_MOD, fd, &ev);
#else
    auto it = state_.find(fd);
    if (it == state_.end())
        return;
    auto &[cur_read, cur_write] = it->second;

    struct kevent changes[2];
    int n = 0;
    if (watch_read != cur_read) {
        EV_SET(&changes[n++], fd, EVFILT_READ, watch_read ? (EV_ADD | EV_ENABLE) : EV_DELETE, 0, 0, nullptr);
        cur_read = watch_read;
    }
    if (watch_write != cur_write) {
        EV_SET(&changes[n++], fd, EVFILT_WRITE, watch_write ? (EV_ADD | EV_ENABLE) : EV_DELETE, 0, 0, nullptr);
        cur_write = watch_write;
    }
    if (n > 0)
        kevent(poll_fd_, changes, n, nullptr, 0, nullptr);
#endif
}

// ─── Remove ───────────────────────────────────────────────────────────────────

void Reactor::Remove(int fd)
{
#ifdef __linux__
    epoll_ctl(poll_fd_, EPOLL_CTL_DEL, fd, nullptr);
#else
    auto it = state_.find(fd);
    if (it == state_.end())
        return;
    auto [had_read, had_write] = it->second;
    state_.erase(it);

    struct kevent changes[2];
    int n = 0;
    if (had_read)
        EV_SET(&changes[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    if (had_write)
        EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    if (n > 0)
        kevent(poll_fd_, changes, n, nullptr, 0, nullptr);
#endif
}

// ─── Poll ─────────────────────────────────────────────────────────────────────

std::vector<PollEvent> Reactor::Poll(int timeout_ms)
{
    std::vector<PollEvent> result;

#ifdef __linux__
    static constexpr int kMaxEvents = 64;
    epoll_event events[kMaxEvents];
    int n = epoll_wait(poll_fd_, events, kMaxEvents, timeout_ms);
    if (n < 0) {
        if (errno != EINTR)
            std::cerr << "epoll_wait: " << strerror(errno) << "\n";
        return result;
    }
    result.reserve(static_cast<size_t>(n));
    for (int i = 0; i < n; i++) {
        PollEvent pe;
        pe.fd = events[i].data.fd;
        pe.readable = (events[i].events & EPOLLIN) != 0;
        pe.writable = (events[i].events & EPOLLOUT) != 0;
        pe.error = (events[i].events & (EPOLLERR | EPOLLHUP)) != 0;
        result.push_back(pe);
    }
#else
    static constexpr int kMaxEvents = 64;
    struct kevent events[kMaxEvents];
    struct timespec ts{};
    struct timespec *tsp = nullptr;
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = static_cast<long>((timeout_ms % 1000) * 1000000L);
        tsp = &ts;
    }
    int n = kevent(poll_fd_, nullptr, 0, events, kMaxEvents, tsp);
    if (n < 0) {
        if (errno != EINTR)
            std::cerr << "kevent: " << strerror(errno) << "\n";
        return result;
    }
    result.reserve(static_cast<size_t>(n));
    for (int i = 0; i < n; i++) {
        PollEvent pe;
        pe.fd = static_cast<int>(events[i].ident);

        if (events[i].filter == EVFILT_READ) {
            pe.readable = true;
            // EV_EOF means the peer closed their write end.  Mark it readable
            // so OnReadable() can drain remaining data before we close.
            if (events[i].flags & EV_EOF)
                pe.error = true;
        }
        else if (events[i].filter == EVFILT_WRITE) {
            pe.writable = true;
        }
        if (events[i].flags & EV_ERROR)
            pe.error = true;

        result.push_back(pe);
    }
#endif
    return result;
}
