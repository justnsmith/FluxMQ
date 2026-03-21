#pragma once

#include "partition.h"
#include "protocol.h"

#include <cstdint>
#include <string>
#include <vector>

// ReplicaClient is a simple synchronous TCP client used by a follower broker
// to pull records from the partition leader via the ReplicaFetch API (key 10).
//
// Unlike the server-side Connection, this is a plain blocking socket client
// that is safe to use from a single dedicated background thread.
class ReplicaClient
{
  public:
    ReplicaClient() = default;
    ~ReplicaClient();

    ReplicaClient(const ReplicaClient &) = delete;
    ReplicaClient &operator=(const ReplicaClient &) = delete;

    // Connect to a broker.  Returns false on error.
    bool Connect(const std::string &host, uint16_t port);
    void Disconnect();
    bool IsConnected() const
    {
        return fd_ >= 0;
    }

    // Result of a single ReplicaFetch call.
    struct FetchResult
    {
        int16_t error{0};
        int32_t leader_epoch{0};
        uint64_t high_watermark{0};
        std::vector<Record> records;
    };

    // Send a ReplicaFetch request and block until the response arrives.
    // request wire: [4B follower_id][2B topic][4B partition][8B offset][4B max_bytes][4B epoch]
    // response wire: [2B error][4B epoch][8B hwm][4B num_records] + records
    FetchResult Fetch(int32_t self_broker_id, const std::string &topic, int32_t partition, uint64_t fetch_offset, int32_t leader_epoch,
                      uint32_t max_bytes = 512 * 1024);

  private:
    bool WriteAll(const void *data, size_t len);
    bool ReadAll(void *data, size_t len);

    int fd_{-1};
    uint32_t corr_id_{0};
};
