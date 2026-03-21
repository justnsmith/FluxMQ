#include "cluster_store.h"

#include <algorithm>
#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <sys/file.h>
#include <unistd.h>

using Clock = std::chrono::system_clock;

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

ClusterStore::ClusterStore(std::filesystem::path cluster_dir, int32_t self_id) : cluster_dir_(std::move(cluster_dir)), self_id_(self_id)
{
    std::filesystem::create_directories(cluster_dir_);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

int64_t ClusterStore::NowMs()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::vector<std::string> ClusterStore::Split(const std::string &s, char delim)
{
    std::vector<std::string> parts;
    std::stringstream ss(s);
    std::string token;
    while (std::getline(ss, token, delim))
        if (!token.empty())
            parts.push_back(token);
    return parts;
}

std::vector<int32_t> ClusterStore::ParseIntList(const std::string &s)
{
    std::vector<int32_t> result;
    if (s.empty() || s == "-")
        return result;
    for (const auto &part : Split(s, ','))
        result.push_back(static_cast<int32_t>(std::stoi(part)));
    return result;
}

std::string ClusterStore::JoinIntList(const std::vector<int32_t> &v)
{
    if (v.empty())
        return "-";
    std::string result;
    for (size_t i = 0; i < v.size(); ++i) {
        if (i > 0)
            result += ',';
        result += std::to_string(v[i]);
    }
    return result;
}

// ---------------------------------------------------------------------------
// Lock
// ---------------------------------------------------------------------------

int ClusterStore::AcquireLock() const
{
    auto lock_path = cluster_dir_ / "state.lock";
    int fd = open(lock_path.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0)
        return -1;
    flock(fd, LOCK_EX);
    return fd;
}

void ClusterStore::ReleaseLock(int fd)
{
    if (fd >= 0) {
        flock(fd, LOCK_UN);
        close(fd);
    }
}

// ---------------------------------------------------------------------------
// LoadLocked  (must be called with flock held)
// ---------------------------------------------------------------------------

void ClusterStore::LoadLocked()
{
    brokers_.clear();
    assignments_.clear();

    // brokers.txt
    auto brokers_path = cluster_dir_ / "brokers.txt";
    if (std::filesystem::exists(brokers_path)) {
        std::ifstream f(brokers_path);
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty() || line[0] == '#')
                continue;
            auto parts = Split(line, ' ');
            if (parts.size() < 4)
                continue;
            BrokerInfo b;
            b.id = std::stoi(parts[0]);
            b.host = parts[1];
            b.port = static_cast<uint16_t>(std::stoi(parts[2]));
            b.last_heartbeat_ms = std::stoll(parts[3]);
            brokers_.push_back(b);
        }
    }

    // assignments.txt
    auto asgn_path = cluster_dir_ / "assignments.txt";
    if (std::filesystem::exists(asgn_path)) {
        std::ifstream f(asgn_path);
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty() || line[0] == '#')
                continue;
            auto parts = Split(line, ' ');
            if (parts.size() < 6)
                continue;
            PartitionAssignment a;
            a.topic = parts[0];
            a.partition = std::stoi(parts[1]);
            a.leader_id = std::stoi(parts[2]);
            a.leader_epoch = std::stoi(parts[3]);
            a.replicas = ParseIntList(parts[4]);
            a.isr = ParseIntList(parts[5]);
            assignments_.push_back(std::move(a));
        }
    }
}

// ---------------------------------------------------------------------------
// SaveLocked  (must be called with flock held)
// ---------------------------------------------------------------------------

void ClusterStore::SaveLocked() const
{
    // Write brokers.txt
    {
        auto tmp = cluster_dir_ / "brokers.txt.tmp";
        {
            std::ofstream f(tmp);
            f << "# id host port last_heartbeat_ms\n";
            for (const auto &b : brokers_)
                f << b.id << ' ' << b.host << ' ' << b.port << ' ' << b.last_heartbeat_ms << '\n';
        }
        std::filesystem::rename(tmp, cluster_dir_ / "brokers.txt");
    }

    // Write assignments.txt
    {
        auto tmp = cluster_dir_ / "assignments.txt.tmp";
        {
            std::ofstream f(tmp);
            f << "# topic partition leader_id leader_epoch replicas isr\n";
            for (const auto &a : assignments_)
                f << a.topic << ' ' << a.partition << ' ' << a.leader_id << ' ' << a.leader_epoch << ' ' << JoinIntList(a.replicas) << ' '
                  << JoinIntList(a.isr) << '\n';
        }
        std::filesystem::rename(tmp, cluster_dir_ / "assignments.txt");
    }
}

// ---------------------------------------------------------------------------
// RegisterSelf
// ---------------------------------------------------------------------------

void ClusterStore::RegisterSelf(const std::string &host, uint16_t port)
{
    int lock_fd = AcquireLock();
    LoadLocked();

    bool found = false;
    for (auto &b : brokers_) {
        if (b.id == self_id_) {
            b.host = host;
            b.port = port;
            b.last_heartbeat_ms = NowMs();
            found = true;
            break;
        }
    }
    if (!found) {
        BrokerInfo b;
        b.id = self_id_;
        b.host = host;
        b.port = port;
        b.last_heartbeat_ms = NowMs();
        brokers_.push_back(b);
    }

    SaveLocked();
    ReleaseLock(lock_fd);

    std::unique_lock w(mu_);
    // Reflect in the in-process cache by reloading.
    LoadLocked();
}

// ---------------------------------------------------------------------------
// SendHeartbeat
// ---------------------------------------------------------------------------

void ClusterStore::SendHeartbeat()
{
    int lock_fd = AcquireLock();
    LoadLocked();

    for (auto &b : brokers_) {
        if (b.id == self_id_) {
            b.last_heartbeat_ms = NowMs();
            break;
        }
    }

    SaveLocked();
    ReleaseLock(lock_fd);

    std::unique_lock w(mu_);
    LoadLocked();
}

// ---------------------------------------------------------------------------
// Reload
// ---------------------------------------------------------------------------

void ClusterStore::Reload()
{
    int lock_fd = AcquireLock();
    LoadLocked();
    ReleaseLock(lock_fd);

    std::unique_lock w(mu_);
    LoadLocked();
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

std::optional<PartitionAssignment> ClusterStore::GetAssignment(std::string_view topic, int32_t partition) const
{
    std::shared_lock r(mu_);
    for (const auto &a : assignments_) {
        if (a.topic == topic && a.partition == partition)
            return a;
    }
    return std::nullopt;
}

std::vector<PartitionAssignment> ClusterStore::LeaderAssignments() const
{
    std::shared_lock r(mu_);
    std::vector<PartitionAssignment> result;
    for (const auto &a : assignments_)
        if (a.leader_id == self_id_)
            result.push_back(a);
    return result;
}

std::vector<PartitionAssignment> ClusterStore::FollowerAssignments() const
{
    std::shared_lock r(mu_);
    std::vector<PartitionAssignment> result;
    for (const auto &a : assignments_) {
        if (a.leader_id != self_id_) {
            for (int32_t r_id : a.replicas) {
                if (r_id == self_id_) {
                    result.push_back(a);
                    break;
                }
            }
        }
    }
    return result;
}

bool ClusterStore::IsLeader(std::string_view topic, int32_t partition) const
{
    std::shared_lock r(mu_);
    for (const auto &a : assignments_)
        if (a.topic == topic && a.partition == partition)
            return a.leader_id == self_id_;
    // No assignment: assume leader (standalone mode).
    return true;
}

std::vector<BrokerInfo> ClusterStore::GetBrokers() const
{
    std::shared_lock r(mu_);
    return brokers_;
}

// ---------------------------------------------------------------------------
// CommitAssignment
// ---------------------------------------------------------------------------

void ClusterStore::CommitAssignment(const PartitionAssignment &asgn)
{
    int lock_fd = AcquireLock();
    LoadLocked();

    bool found = false;
    for (auto &a : assignments_) {
        if (a.topic == asgn.topic && a.partition == asgn.partition) {
            a = asgn;
            found = true;
            break;
        }
    }
    if (!found)
        assignments_.push_back(asgn);

    SaveLocked();
    ReleaseLock(lock_fd);

    std::unique_lock w(mu_);
    LoadLocked();
}

// ---------------------------------------------------------------------------
// UpdateISR
// ---------------------------------------------------------------------------

void ClusterStore::UpdateISR(std::string_view topic, int32_t partition, std::vector<int32_t> new_isr)
{
    int lock_fd = AcquireLock();
    LoadLocked();

    for (auto &a : assignments_) {
        if (a.topic == topic && a.partition == partition) {
            a.isr = std::move(new_isr);
            break;
        }
    }

    SaveLocked();
    ReleaseLock(lock_fd);

    std::unique_lock w(mu_);
    LoadLocked();
}

// ---------------------------------------------------------------------------
// AssignNewTopic
// ---------------------------------------------------------------------------

void ClusterStore::AssignNewTopic(std::string_view topic_name, int num_partitions, int replication_factor,
                                  std::chrono::milliseconds broker_timeout)
{
    int lock_fd = AcquireLock();
    LoadLocked();

    // Collect live broker ids sorted by id.
    int64_t now = NowMs();
    std::vector<int32_t> live;
    for (const auto &b : brokers_) {
        if (now - b.last_heartbeat_ms <= broker_timeout.count())
            live.push_back(b.id);
    }
    std::sort(live.begin(), live.end());

    // Always include self even if heartbeat is slightly stale.
    if (std::find(live.begin(), live.end(), self_id_) == live.end())
        live.push_back(self_id_);

    if (static_cast<int>(live.size()) < replication_factor)
        replication_factor = static_cast<int>(live.size());

    for (int p = 0; p < num_partitions; ++p) {
        // Check if assignment already exists.
        bool exists = false;
        for (const auto &a : assignments_)
            if (a.topic == topic_name && a.partition == p) {
                exists = true;
                break;
            }
        if (exists)
            continue;

        PartitionAssignment a;
        a.topic = std::string(topic_name);
        a.partition = p;
        a.leader_epoch = 1;

        // Distribute replicas round-robin across live brokers.
        for (int r = 0; r < replication_factor; ++r)
            a.replicas.push_back(live[(p + r) % static_cast<int>(live.size())]);

        a.leader_id = a.replicas[0];
        a.isr = a.replicas; // Start with full ISR.

        assignments_.push_back(std::move(a));
    }

    SaveLocked();
    ReleaseLock(lock_fd);

    std::unique_lock w(mu_);
    LoadLocked();
}
