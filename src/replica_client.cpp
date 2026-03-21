#include "replica_client.h"

#include "codec.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// ---------------------------------------------------------------------------
// Destructor
// ---------------------------------------------------------------------------

ReplicaClient::~ReplicaClient()
{
    Disconnect();
}

// ---------------------------------------------------------------------------
// Connect / Disconnect
// ---------------------------------------------------------------------------

bool ReplicaClient::Connect(const std::string &host, uint16_t port)
{
    Disconnect();

    struct addrinfo hints
    {
    };
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *res = nullptr;
    std::string port_str = std::to_string(port);
    if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0)
        return false;

    int sock = -1;
    for (auto *p = res; p != nullptr; p = p->ai_next) {
        sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sock < 0)
            continue;
        if (connect(sock, p->ai_addr, p->ai_addrlen) == 0)
            break;
        close(sock);
        sock = -1;
    }
    freeaddrinfo(res);

    if (sock < 0)
        return false;

    fd_ = sock;
    return true;
}

void ReplicaClient::Disconnect()
{
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

// ---------------------------------------------------------------------------
// I/O helpers
// ---------------------------------------------------------------------------

bool ReplicaClient::WriteAll(const void *data, size_t len)
{
    const auto *p = static_cast<const uint8_t *>(data);
    while (len > 0) {
        ssize_t n = write(fd_, p, len);
        if (n <= 0)
            return false;
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

bool ReplicaClient::ReadAll(void *data, size_t len)
{
    auto *p = static_cast<uint8_t *>(data);
    while (len > 0) {
        ssize_t n = read(fd_, p, len);
        if (n <= 0)
            return false;
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

ReplicaClient::FetchResult ReplicaClient::Fetch(int32_t self_broker_id, const std::string &topic, int32_t partition, uint64_t fetch_offset,
                                                int32_t leader_epoch, uint32_t max_bytes)
{
    FetchResult bad;
    bad.error = -1;

    if (fd_ < 0)
        return bad;

    // Build payload: [4B self_id][2B topic_len][topic][4B partition][8B offset][4B max_bytes][4B epoch]
    std::vector<uint8_t> payload;
    BinaryWriter w(payload);
    w.WriteI32(self_broker_id);
    w.WriteString(topic);
    w.WriteI32(partition);
    w.WriteU64(fetch_offset);
    w.WriteU32(max_bytes);
    w.WriteI32(leader_epoch);

    // Encode full request frame.
    uint32_t corr_id = ++corr_id_;
    RequestFrame req;
    req.api_key = api::kReplicaFetch;
    req.api_version = 0;
    req.correlation_id = corr_id;
    req.payload = std::move(payload);

    auto wire = EncodeRequest(req);
    if (!WriteAll(wire.data(), wire.size()))
        return bad;

    // Read response: [4B total_len][4B corr_id][payload...]
    uint8_t len_buf[4];
    if (!ReadAll(len_buf, 4))
        return bad;

    uint32_t total_len = DecBe32(len_buf);
    if (total_len < 4)
        return bad;

    std::vector<uint8_t> resp_body(total_len);
    if (!ReadAll(resp_body.data(), total_len))
        return bad;

    // Skip the 4-byte corr_id.
    BinaryReader r(resp_body);
    r.ReadI32(); // corr_id (ignored)

    FetchResult result;
    result.error = r.ReadI16();
    result.leader_epoch = r.ReadI32();
    result.high_watermark = r.ReadU64();

    if (result.error != 0)
        return result;

    uint32_t num_records = r.ReadU32();
    result.records.reserve(num_records);
    for (uint32_t i = 0; i < num_records; ++i) {
        Record rec;
        rec.offset = r.ReadU64();
        uint32_t val_len = r.ReadU32();
        auto val = r.ReadBytes(val_len);
        rec.value.assign(val.begin(), val.end());
        result.records.push_back(std::move(rec));
    }
    return result;
}
