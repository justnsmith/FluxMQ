# FluxMQ

FluxMQ is a distributed message queue built from scratch in C++20 and Go, inspired by Kafka's architecture. The broker implements an append-only segmented log, a custom binary protocol over TCP, an epoll-based reactor, leader-follower replication with ISR tracking, and consumer group coordination. The Go side provides a client library with a batching producer and channel-based consumer, a CLI for managing topics and streaming messages, and a benchmarking tool. The whole thing runs as a single broker binary with no external runtime dependencies.

## Table of Contents
- [Features](#features)
- [Architecture](#architecture)
- [Build](#build)
- [Running](#running)
- [CLI](#cli)
- [Benchmarking](#benchmarking)
- [Metrics](#metrics)
- [Docker](#docker)
- [Design Notes](#design-notes)

## Features

- Append-only segmented log with sparse offset index
- Custom binary TCP protocol with request pipelining
- epoll/kqueue reactor (non-blocking I/O)
- Partitioned topics with leader-follower replication and ISR tracking
- Automatic leader failover on heartbeat timeout
- Consumer groups with cooperative rebalancing
- Prometheus `/metrics` endpoint
- Go client SDK with batching producer, channel-based consumer, and cluster-aware routing
- CLI (`fluxmq`) and benchmark tool (`fluxmq-bench`)
- Docker Compose 3-broker dev cluster

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  FluxMQ Cluster                  │
│                                                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐    │
│  │ Broker 1 │◄──│ Broker 2 │◄──│ Broker 3 │    │
│  │ (leader) │   │(follower)│   │(follower)│    │
│  └──────────┘   └──────────┘   └──────────┘    │
│         Shared ClusterStore (file-backed)        │
└──────────────────────────────────────────────────┘
        ▲ Produce                    │ Fetch
┌───────┴──────┐            ┌────────▼───────┐
│ Go Producer  │            │  Go Consumer   │
│ batch+linger │            │ group rebalance│
└──────────────┘            └────────────────┘
```

| Component | File | What it does |
|---|---|---|
| Segment | `segment.cpp` | `.log` + sparse `.index` file pair |
| Log | `log.cpp` | Ordered segment chain, rotation |
| Reactor | `reactor.cpp` | epoll/kqueue event loop |
| BrokerHandler | `handler.cpp` | Dispatches all API keys, long-poll fetch |
| GroupCoordinator | `group_coordinator.cpp` | JoinGroup/SyncGroup state machine |
| ClusterStore | `cluster_store.cpp` | Broker registry + assignments via `flock` |
| ReplicationManager | `replication_manager.cpp` | Follower loops, ISR, HWM |
| LeaderElector | `leader_elector.cpp` | Detects dead brokers, claims leadership |
| MetricsRegistry | `metrics.cpp` | Atomic counters/gauges/histogram |

## Build

Requires g++ (C++20) and Go 1.22+.

```bash
make            # broker binary → build/fluxmq
make sdk        # CLI tool      → build/fluxmq-cli
make bench      # bench tool    → build/fluxmq-bench
make test       # C++ unit tests
make sdk-test   # Go integration tests
make format-check && make lint
```

## Running

### Standalone

```bash
./build/fluxmq                    # :9092 (broker), :9093 (metrics)
./build/fluxmq --port=9094 --data-dir=/var/lib/fluxmq
```

Key flags:

| Flag | Default | Description |
|---|---|---|
| `--port` | `9092` | Listen port |
| `--metrics-port` | `port+1` | Prometheus HTTP port |
| `--data-dir` | `/tmp/fluxmq_data` | Log storage |
| `--broker-id` | `0` | Set > 0 to enable cluster mode |
| `--broker-host` | `127.0.0.1` | Advertised address |
| `--cluster-dir` | | Shared coordination directory |
| `--replication-factor` | `1` | Default replication factor |
| `--broker-timeout-ms` | `15000` | Heartbeat timeout |

### 3-broker cluster (local)

```bash
./build/fluxmq --broker-id=1 --port=9092 --cluster-dir=/tmp/cluster --data-dir=/tmp/b1 &
./build/fluxmq --broker-id=2 --port=9093 --cluster-dir=/tmp/cluster --data-dir=/tmp/b2 &
./build/fluxmq --broker-id=3 --port=9094 --cluster-dir=/tmp/cluster --data-dir=/tmp/b3 --replication-factor=3 &
```

## CLI

```bash
BROKER=localhost:9092

./build/fluxmq-cli --broker=$BROKER topic create --name orders --partitions 6
./build/fluxmq-cli --broker=$BROKER topic list
./build/fluxmq-cli --broker=$BROKER produce --topic orders --key "u:1" --value '{"id":1}'
./build/fluxmq-cli --broker=$BROKER consume --topic orders --group my-group --from-beginning
./build/fluxmq-cli --broker=$BROKER cluster info
```

## Benchmarks

Single standalone broker on Apple M3 Pro, 256-byte messages, 4 producer threads:

| Workload | Throughput | Latency (p50) | Latency (p99) |
|---|---|---|---|
| **Produce** (1M msgs) | 45,986 msg/sec · 11.2 MB/sec | 0.07 ms | 0.19 ms |
| **Consume** (905K msgs) | 59,463 msg/sec · 14.5 MB/sec | — | — |
| **End-to-end** (produce → consume) | 17,330 msg/sec · 4.4 MB/sec | 0.09 ms | 11.7 ms |

Run your own:

```bash
./build/fluxmq-bench produce --topic bench --msg-size 256 --num-msgs 1000000 --threads 4
./build/fluxmq-bench consume --topic bench --group g --threads 4 --duration 15s
./build/fluxmq-bench e2e    --topic bench --msg-size 256 --duration 15s
```

## Metrics

```bash
curl http://localhost:9093/metrics
```

| Metric | Type |
|---|---|
| `fluxmq_bytes_in_total` / `fluxmq_bytes_out_total` | counter |
| `fluxmq_messages_in_total` / `fluxmq_messages_out_total` | counter |
| `fluxmq_partition_log_end_offset{topic,partition}` | gauge |
| `fluxmq_partition_high_watermark{topic,partition}` | gauge |
| `fluxmq_consumer_group_lag{group,topic,partition}` | gauge |
| `fluxmq_replication_lag_ms{topic,partition}` | gauge |
| `fluxmq_fetch_latency_ms` | histogram |

## Docker

```bash
docker compose up --build -d    # build + start 3-broker cluster
docker compose logs -f
docker compose down -v
```

| Broker | Protocol | Metrics |
|---|---|---|
| broker1 | `localhost:9092` | `localhost:9093` |
| broker2 | `localhost:9094` | `localhost:9095` |
| broker3 | `localhost:9096` | `localhost:9097` |

Brokers communicate via Docker service names. External clients connect through the mapped host ports.

## Design Notes

**Custom protocol over gRPC.** Custom framing means the broker reads directly into stack buffers without deserializing into protobuf objects. Correlation IDs let multiple requests share one TCP connection without head-of-line blocking.

**ISR instead of Raft.** Raft needs a majority quorum on every write. ISR only requires acknowledgement from in-sync replicas, which can shrink to just the leader. Better write throughput for queue workloads, tunable per-produce via `acks`.

**Pull-based replication.** Followers fetch from the leader using the same Fetch API consumers use. Slow followers fall behind and get evicted from the ISR rather than blocking the leader.

**File-based coordination.** `ClusterStore` uses `flock(2)` on shared files instead of ZooKeeper/etcd. Good enough for a dev cluster, trivially reproducible with a shared Docker volume.

## License

MIT
