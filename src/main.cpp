#include "group_coordinator.h"
#include "handler.h"
#include "server.h"
#include "topic_manager.h"

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

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--port=", 0) == 0)
            port = static_cast<uint16_t>(std::stoi(arg.substr(7)));
        else if (arg.rfind("--data-dir=", 0) == 0)
            data_dir = arg.substr(11);
    }

    std::filesystem::create_directories(data_dir);

    TopicManager tm(data_dir);
    GroupCoordinator gc;

    // Break the circular dependency (BrokerHandler ↔ Server) using a deferred
    // pointer: broker captures &srv by reference, but srv is only dereferenced
    // from the background thread after the server has been fully started.
    std::unique_ptr<Server> srv;

    auto broker = std::make_shared<BrokerHandler>(tm, gc, [&srv](int fd, ResponseFrame resp) { srv->PostResponse(fd, std::move(resp)); });

    srv = std::make_unique<Server>([broker](Connection &conn, RequestFrame frame) { broker->Handle(conn, frame); });

    std::thread t([&] { srv->Run(port); });
    uint16_t bound = srv->WaitReady();
    printf("FluxMQ broker listening on port %u  (data: %s)\n", bound, data_dir.c_str());

    t.join();
    return 0;
}
