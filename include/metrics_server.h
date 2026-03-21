#pragma once

#include "metrics.h"

#include <atomic>
#include <cstdint>
#include <thread>

// MetricsServer exposes a Prometheus /metrics endpoint on a dedicated port.
//
// It runs a minimal blocking HTTP/1.1 server on its own thread — completely
// independent of the main reactor.  Only GET /metrics is handled; all other
// requests receive a 404.  No external HTTP library is required.
class MetricsServer
{
  public:
    MetricsServer(MetricsRegistry &registry, uint16_t port);
    ~MetricsServer();

    MetricsServer(const MetricsServer &) = delete;
    MetricsServer &operator=(const MetricsServer &) = delete;

    // Bind the listen socket and start the server thread.
    // No-op (and silent) if the port is already in use or bind fails.
    void Start();

    // Returns the actual bound port (useful when port was 0 = OS-assigned).
    uint16_t Port() const
    {
        return bound_port_;
    }

  private:
    void Serve();

    MetricsRegistry &registry_;
    uint16_t port_;
    uint16_t bound_port_{0};
    int listen_fd_{-1};
    std::atomic<bool> running_{false};
    std::thread thread_;
};
