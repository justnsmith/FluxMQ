#include "metrics_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <string>

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------

MetricsServer::MetricsServer(MetricsRegistry &registry, uint16_t port) : registry_(registry), port_(port)
{
}

MetricsServer::~MetricsServer()
{
    running_ = false;
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (thread_.joinable())
        thread_.join();
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

void MetricsServer::Start()
{
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0)
        return;

    int yes = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0 || ::listen(listen_fd_, 16) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    // Discover the actual bound port (needed when port_ == 0).
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len);
    bound_port_ = ntohs(addr.sin_port);

    running_ = true;
    thread_ = std::thread([this] { Serve(); });
}

// ---------------------------------------------------------------------------
// Serve  (runs on a dedicated thread)
// ---------------------------------------------------------------------------

void MetricsServer::Serve()
{
    while (running_) {
        int client = ::accept(listen_fd_, nullptr, nullptr);
        if (client < 0)
            break; // listen_fd_ was closed by the destructor

        // Read the request line — we only need the first line.
        char buf[512];
        ssize_t n = ::recv(client, buf, sizeof(buf) - 1, 0);

        std::string body;
        int status = 404;
        if (n > 0) {
            buf[n] = '\0';
            if (std::strncmp(buf, "GET /metrics", 12) == 0) {
                body = registry_.Render();
                status = 200;
            }
        }

        std::string response;
        if (status == 200) {
            response = "HTTP/1.1 200 OK\r\n"
                       "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                       "Content-Length: " +
                       std::to_string(body.size()) +
                       "\r\n"
                       "Connection: close\r\n"
                       "\r\n" +
                       body;
        }
        else {
            response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        }

        ::send(client, response.data(), response.size(), 0);
        ::close(client);
    }
}
