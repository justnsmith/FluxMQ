#include "cluster_store.h"
#include "group_coordinator.h"
#include "handler.h"
#include "leader_elector.h"
#include "replication_manager.h"
#include "server.h"
#include "topic_manager.h"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

// ─── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char *argv[])
{
    uint16_t port = 9092;
    std::string data_dir = "/tmp/fluxmq_data";
    std::string cluster_dir;
    std::string broker_host = "127.0.0.1";
    int broker_id = 0; // 0 = standalone (no replication)
    int session_timeout_ms = 10000;
    int replication_factor = 1;
    int replica_lag_ms = 10000;
    int broker_timeout_ms = 15000;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--port=", 0) == 0)
            port = static_cast<uint16_t>(std::stoi(arg.substr(7)));
        else if (arg.rfind("--data-dir=", 0) == 0)
            data_dir = arg.substr(11);
        else if (arg.rfind("--cluster-dir=", 0) == 0)
            cluster_dir = arg.substr(14);
        else if (arg.rfind("--broker-host=", 0) == 0)
            broker_host = arg.substr(14);
        else if (arg.rfind("--broker-id=", 0) == 0)
            broker_id = std::stoi(arg.substr(12));
        else if (arg.rfind("--session-timeout-ms=", 0) == 0)
            session_timeout_ms = std::stoi(arg.substr(21));
        else if (arg.rfind("--replication-factor=", 0) == 0)
            replication_factor = std::stoi(arg.substr(21));
        else if (arg.rfind("--replica-lag-ms=", 0) == 0)
            replica_lag_ms = std::stoi(arg.substr(17));
        else if (arg.rfind("--broker-timeout-ms=", 0) == 0)
            broker_timeout_ms = std::stoi(arg.substr(20));
    }

    std::filesystem::create_directories(data_dir);

    TopicManager tm(data_dir);
    auto gc_timeout = std::chrono::milliseconds(session_timeout_ms);
    GroupCoordinator gc(gc_timeout);

    // ── Cluster mode (broker_id > 0 and cluster_dir set) ──────────────────────
    std::unique_ptr<ClusterStore> cs;
    std::unique_ptr<ReplicationManager> rm;
    std::unique_ptr<LeaderElector> le;

    if (broker_id > 0 && !cluster_dir.empty()) {
        std::filesystem::create_directories(cluster_dir);
        cs = std::make_unique<ClusterStore>(cluster_dir, broker_id);
        cs->RegisterSelf(broker_host, port);

        rm = std::make_unique<ReplicationManager>(*cs, tm, std::chrono::milliseconds(replica_lag_ms));

        le = std::make_unique<LeaderElector>(*cs, tm, *rm, std::chrono::milliseconds(broker_timeout_ms));
    }

    // Break the circular dependency (BrokerHandler ↔ Server) using a deferred
    // pointer: broker captures &srv by reference, but srv is only dereferenced
    // from the background thread after the server has been fully started.
    std::unique_ptr<Server> srv;

    auto broker = std::make_shared<BrokerHandler>(
        tm, gc, [&srv](int fd, ResponseFrame resp) { srv->PostResponse(fd, std::move(resp)); }, cs.get(), rm.get(), replication_factor,
        std::chrono::milliseconds(broker_timeout_ms));

    srv = std::make_unique<Server>([broker](Connection &conn, RequestFrame frame) { broker->Handle(conn, frame); });

    std::thread t([&] { srv->Run(port); });
    uint16_t bound = srv->WaitReady();
    printf("FluxMQ broker listening on port %u  (data: %s)\n", bound, data_dir.c_str());

    // Start replication after the server is listening.
    if (rm)
        rm->Start();
    if (le)
        le->Start();

    t.join();
    return 0;
}
