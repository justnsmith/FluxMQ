#include "metrics.h"

#include <sstream>
#include <string>

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

static std::string PartKey(const std::string &topic, int32_t partition)
{
    return topic + ":" + std::to_string(partition);
}

static std::string LagKey(const std::string &group, const std::string &topic, int32_t partition)
{
    return group + '\0' + topic + ":" + std::to_string(partition);
}

// Split "topic:partition" at the last ':'.
static void SplitPartKey(const std::string &key, std::string &topic_out, std::string &part_out)
{
    auto pos = key.rfind(':');
    if (pos == std::string::npos) {
        topic_out = key;
        part_out = "0";
    }
    else {
        topic_out = key.substr(0, pos);
        part_out = key.substr(pos + 1);
    }
}

// ---------------------------------------------------------------------------
// ObserveFetchLatency
// ---------------------------------------------------------------------------

void MetricsRegistry::ObserveFetchLatency(uint64_t ms)
{
    // Increment all cumulative buckets where ms <= bucket upper bound.
    bool placed = false;
    for (size_t i = 0; i < kBuckets.size(); ++i) {
        if (!placed && ms <= kBuckets[i])
            placed = true;
        if (placed)
            fetch_latency_buckets_[i].fetch_add(1, std::memory_order_relaxed);
    }
    // +Inf bucket always gets incremented.
    fetch_latency_buckets_[10].fetch_add(1, std::memory_order_relaxed);
    fetch_latency_sum_ms_.fetch_add(ms, std::memory_order_relaxed);
    fetch_latency_count_.fetch_add(1, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// EnsureXxx
// ---------------------------------------------------------------------------

void MetricsRegistry::EnsurePartition(const std::string &topic, int32_t partition)
{
    std::string key = PartKey(topic, partition);
    std::unique_lock lock(part_mu_);
    partitions_.try_emplace(key, std::make_unique<PartitionGauges>());
}

void MetricsRegistry::EnsureConsumerLag(const std::string &group, const std::string &topic, int32_t partition)
{
    std::string key = LagKey(group, topic, partition);
    std::unique_lock lock(lag_mu_);
    consumer_lags_.try_emplace(key, std::make_unique<std::atomic<uint64_t>>(0));
}

void MetricsRegistry::EnsureReplicationLag(const std::string &topic, int32_t partition)
{
    std::string key = PartKey(topic, partition);
    std::unique_lock lock(repl_mu_);
    replication_lags_.try_emplace(key, std::make_unique<std::atomic<uint64_t>>(0));
}

// ---------------------------------------------------------------------------
// SetXxx  (shared lock on map + atomic store on the entry)
// ---------------------------------------------------------------------------

void MetricsRegistry::SetPartitionLEO(const std::string &topic, int32_t partition, uint64_t leo)
{
    std::string key = PartKey(topic, partition);
    {
        std::shared_lock lock(part_mu_);
        auto it = partitions_.find(key);
        if (it != partitions_.end()) {
            it->second->log_end_offset.store(leo, std::memory_order_relaxed);
            return;
        }
    }
    EnsurePartition(topic, partition);
    std::shared_lock lock(part_mu_);
    auto it = partitions_.find(key);
    if (it != partitions_.end())
        it->second->log_end_offset.store(leo, std::memory_order_relaxed);
}

void MetricsRegistry::SetPartitionHWM(const std::string &topic, int32_t partition, uint64_t hwm)
{
    std::string key = PartKey(topic, partition);
    {
        std::shared_lock lock(part_mu_);
        auto it = partitions_.find(key);
        if (it != partitions_.end()) {
            it->second->high_watermark.store(hwm, std::memory_order_relaxed);
            return;
        }
    }
    EnsurePartition(topic, partition);
    std::shared_lock lock(part_mu_);
    auto it = partitions_.find(key);
    if (it != partitions_.end())
        it->second->high_watermark.store(hwm, std::memory_order_relaxed);
}

void MetricsRegistry::SetConsumerLag(const std::string &group, const std::string &topic, int32_t partition, uint64_t lag)
{
    std::string key = LagKey(group, topic, partition);
    {
        std::shared_lock lock(lag_mu_);
        auto it = consumer_lags_.find(key);
        if (it != consumer_lags_.end()) {
            it->second->store(lag, std::memory_order_relaxed);
            return;
        }
    }
    EnsureConsumerLag(group, topic, partition);
    std::shared_lock lock(lag_mu_);
    auto it = consumer_lags_.find(key);
    if (it != consumer_lags_.end())
        it->second->store(lag, std::memory_order_relaxed);
}

void MetricsRegistry::SetReplicationLag(const std::string &topic, int32_t partition, uint64_t lag_ms)
{
    std::string key = PartKey(topic, partition);
    {
        std::shared_lock lock(repl_mu_);
        auto it = replication_lags_.find(key);
        if (it != replication_lags_.end()) {
            it->second->store(lag_ms, std::memory_order_relaxed);
            return;
        }
    }
    EnsureReplicationLag(topic, partition);
    std::shared_lock lock(repl_mu_);
    auto it = replication_lags_.find(key);
    if (it != replication_lags_.end())
        it->second->store(lag_ms, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

std::string MetricsRegistry::Render() const
{
    std::ostringstream ss;

    // ── Global counters ──────────────────────────────────────────────────────
    ss << "# HELP fluxmq_bytes_in_total Total bytes received by the broker\n"
       << "# TYPE fluxmq_bytes_in_total counter\n"
       << "fluxmq_bytes_in_total " << bytes_in_total.load(std::memory_order_relaxed) << "\n\n";

    ss << "# HELP fluxmq_bytes_out_total Total bytes sent by the broker\n"
       << "# TYPE fluxmq_bytes_out_total counter\n"
       << "fluxmq_bytes_out_total " << bytes_out_total.load(std::memory_order_relaxed) << "\n\n";

    ss << "# HELP fluxmq_messages_in_total Total messages received by the broker\n"
       << "# TYPE fluxmq_messages_in_total counter\n"
       << "fluxmq_messages_in_total " << messages_in_total.load(std::memory_order_relaxed) << "\n\n";

    ss << "# HELP fluxmq_messages_out_total Total messages sent by the broker\n"
       << "# TYPE fluxmq_messages_out_total counter\n"
       << "fluxmq_messages_out_total " << messages_out_total.load(std::memory_order_relaxed) << "\n\n";

    // ── Per-partition gauges ─────────────────────────────────────────────────
    ss << "# HELP fluxmq_partition_log_end_offset Log end offset per partition\n"
       << "# TYPE fluxmq_partition_log_end_offset gauge\n";
    {
        std::shared_lock lock(part_mu_);
        for (const auto &[key, g] : partitions_) {
            std::string topic, part;
            SplitPartKey(key, topic, part);
            ss << "fluxmq_partition_log_end_offset{topic=\"" << topic << "\",partition=\"" << part << "\"} "
               << g->log_end_offset.load(std::memory_order_relaxed) << "\n";
        }
    }
    ss << "\n";

    ss << "# HELP fluxmq_partition_high_watermark High watermark offset per partition\n"
       << "# TYPE fluxmq_partition_high_watermark gauge\n";
    {
        std::shared_lock lock(part_mu_);
        for (const auto &[key, g] : partitions_) {
            std::string topic, part;
            SplitPartKey(key, topic, part);
            ss << "fluxmq_partition_high_watermark{topic=\"" << topic << "\",partition=\"" << part << "\"} "
               << g->high_watermark.load(std::memory_order_relaxed) << "\n";
        }
    }
    ss << "\n";

    // ── Consumer group lag ───────────────────────────────────────────────────
    ss << "# HELP fluxmq_consumer_group_lag Consumer group lag (HWM - committed offset)\n"
       << "# TYPE fluxmq_consumer_group_lag gauge\n";
    {
        std::shared_lock lock(lag_mu_);
        for (const auto &[key, v] : consumer_lags_) {
            auto sep = key.find('\0');
            if (sep == std::string::npos)
                continue;
            std::string group = key.substr(0, sep);
            std::string remainder = key.substr(sep + 1);
            std::string topic, part;
            SplitPartKey(remainder, topic, part);
            ss << "fluxmq_consumer_group_lag{group=\"" << group << "\",topic=\"" << topic << "\",partition=\"" << part << "\"} "
               << v->load(std::memory_order_relaxed) << "\n";
        }
    }
    ss << "\n";

    // ── Replication lag ──────────────────────────────────────────────────────
    ss << "# HELP fluxmq_replication_lag_ms Time since last follower fetch in milliseconds\n"
       << "# TYPE fluxmq_replication_lag_ms gauge\n";
    {
        std::shared_lock lock(repl_mu_);
        for (const auto &[key, v] : replication_lags_) {
            std::string topic, part;
            SplitPartKey(key, topic, part);
            ss << "fluxmq_replication_lag_ms{topic=\"" << topic << "\",partition=\"" << part << "\"} " << v->load(std::memory_order_relaxed)
               << "\n";
        }
    }
    ss << "\n";

    // ── Fetch latency histogram ──────────────────────────────────────────────
    ss << "# HELP fluxmq_fetch_latency_ms Fetch request latency in milliseconds\n"
       << "# TYPE fluxmq_fetch_latency_ms histogram\n";
    for (size_t i = 0; i < kBuckets.size(); ++i) {
        ss << "fluxmq_fetch_latency_ms_bucket{le=\"" << kBuckets[i] << "\"} " << fetch_latency_buckets_[i].load(std::memory_order_relaxed)
           << "\n";
    }
    ss << "fluxmq_fetch_latency_ms_bucket{le=\"+Inf\"} " << fetch_latency_buckets_[10].load(std::memory_order_relaxed) << "\n";
    ss << "fluxmq_fetch_latency_ms_sum " << fetch_latency_sum_ms_.load(std::memory_order_relaxed) << "\n";
    ss << "fluxmq_fetch_latency_ms_count " << fetch_latency_count_.load(std::memory_order_relaxed) << "\n";

    return ss.str();
}
