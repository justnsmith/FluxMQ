#include "server.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

// ─── Helpers ──────────────────────────────────────────────────────────────────

static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
        return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static uint16_t get_bound_port(int fd)
{
    struct sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) < 0)
        return 0;
    return ntohs(addr.sin_port);
}

// ─── Constructor / Destructor ─────────────────────────────────────────────────

Server::Server(RequestHandler handler) : handler_(std::move(handler))
{
    if (pipe(stop_pipe_) < 0) {
        throw std::runtime_error(std::string("pipe: ") + strerror(errno));
    }
    set_nonblocking(stop_pipe_[0]);
    set_nonblocking(stop_pipe_[1]);
}

Server::~Server()
{
    for (auto &[fd, conn] : connections_) {
        delete conn;
    }
    if (listen_fd_ >= 0)
        close(listen_fd_);
    if (stop_pipe_[0] >= 0)
        close(stop_pipe_[0]);
    if (stop_pipe_[1] >= 0)
        close(stop_pipe_[1]);
}

// ─── Run ──────────────────────────────────────────────────────────────────────

void Server::Run(uint16_t port)
{
    // Create and configure the listen socket.
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error(std::string("socket: ") + strerror(errno));
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(listen_fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    set_nonblocking(listen_fd_);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error(std::string("bind: ") + strerror(errno));
    }
    if (listen(listen_fd_, SOMAXCONN) < 0) {
        throw std::runtime_error(std::string("listen: ") + strerror(errno));
    }

    reactor_.Add(listen_fd_, /*read=*/true, /*write=*/false);
    reactor_.Add(stop_pipe_[0], /*read=*/true, /*write=*/false);

    // Signal WaitReady() callers — the server is now accepting connections.
    ready_promise_.set_value(get_bound_port(listen_fd_));

    running_ = true;
    while (running_) {
        for (const auto &ev : reactor_.Poll(50)) {
            if (ev.fd == stop_pipe_[0]) {
                running_ = false;
                break;
            }
            if (ev.fd == listen_fd_) {
                AcceptNew();
            }
            else {
                HandleEvent(ev);
            }
        }
    }
}

// ─── Stop ─────────────────────────────────────────────────────────────────────

void Server::Stop()
{
    running_ = false;
    char b = 0;
    (void)write(stop_pipe_[1], &b, sizeof(b));
}

// ─── WaitReady ────────────────────────────────────────────────────────────────

uint16_t Server::WaitReady()
{
    return ready_future_.get();
}

// ─── AcceptNew ────────────────────────────────────────────────────────────────

void Server::AcceptNew()
{
    while (true) {
        int client_fd = accept(listen_fd_, nullptr, nullptr);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            std::cerr << "accept: " << strerror(errno) << "\n";
            break;
        }
        set_nonblocking(client_fd);
        int opt = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
        reactor_.Add(client_fd, /*read=*/true, /*write=*/false);
        connections_[client_fd] = new Connection(client_fd, handler_);
    }
}

// ─── HandleEvent ──────────────────────────────────────────────────────────────

void Server::HandleEvent(const PollEvent &ev)
{
    auto it = connections_.find(ev.fd);
    if (it == connections_.end())
        return;
    Connection *conn = it->second;

    bool close_it = ev.error;

    if (!close_it && ev.readable) {
        close_it = !conn->OnReadable();
    }
    if (!close_it && ev.writable) {
        close_it = !conn->OnWritable();
    }

    if (close_it) {
        CloseConn(ev.fd);
    }
    else {
        // Keep EPOLLOUT registered only when there is pending data to send.
        reactor_.Modify(ev.fd, /*read=*/true, /*write=*/conn->WantWrite());
    }
}

// ─── CloseConn ────────────────────────────────────────────────────────────────

void Server::CloseConn(int fd)
{
    auto it = connections_.find(fd);
    if (it == connections_.end())
        return;
    reactor_.Remove(fd);
    delete it->second;
    connections_.erase(it);
}
