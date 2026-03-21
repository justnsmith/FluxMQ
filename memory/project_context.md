---
name: FluxMQ project context
description: FluxMQ project overview, current phase, architecture decisions, and benchmark numbers
type: project
---

FluxMQ is a Kafka-inspired distributed message queue built in C++20 (broker) + Go (client SDK + CLI).
It is a resume project demonstrating different primitives from an existing KV store (which uses LSM-tree, Raft, gRPC).

## Current State

**Phase 1 (Storage Engine) — COMPLETE as of 2026-03-20**

Implemented files:
- `include/crc32.h` — compile-time CRC32 lookup table (header-only)
- `include/segment.h` / `src/segment.cpp` — Segment class
- `include/log.h` / `src/log.cpp` — Log class (multi-segment)
- `src/main.cpp` — benchmark/demo binary
- `tests/test_log.cpp` — 7 unit tests (all passing)

Key design decisions locked in:
- On-disk record: `[4B length][4B crc32][payload]`
- On-disk index entry: `[8B logical_offset][4B file_position]` (sparse, 12 bytes each)
- Sparse index interval: 4096 bytes (`kIndexIntervalBytes`)
- First record of every segment is always indexed
- Index loaded via mmap on Open() (zero-copy bulk read into std::vector)
- Log data read via pread() (not mmap, avoids TLB thrashing on large files)
- Segment filenames: 20-digit zero-padded base offset (e.g., `00000000000000000000.log`)
- Segment rotation when log file >= `max_segment_bytes` (default 1 GiB)
- C++20, `-Wall -Wextra`, clang-format (LLVM style, indent=4)

## Benchmark Numbers (macOS, NVMe, single-threaded, no fsync)

- 1M × 100-byte records: **~245K msg/s, ~23 MB/s**
- 100K × 1024-byte records: **~115K msg/s, ~112 MB/s**
- These are limited by 3 pwrite() syscalls per record; Phase 3 batching will improve this significantly.

## Roadmap

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Storage Engine (Segment + Log) | ✅ Done |
| 2 | Network Layer (epoll reactor, binary protocol) | Not started |
| 3 | Broker Core (produce/fetch paths) | Not started |
| 4 | Go Client SDK + CLI | Not started |
| 5 | Consumer Groups & Rebalancing | Not started |
| 6 | Replication & Fault Tolerance (ISR) | Not started |
| 7 | Observability & Benchmarking | Not started |
| 8 | Docker + Deployment | Not started |

## Resume Target (fill X's after benchmarking)

```
FluxMQ — Distributed Message Queue | C++20, Go, Docker, AWS          Mar 2026 – Present
• Engineered a Kafka-inspired distributed message queue in C++20 with append-only segmented
  logs, a custom binary protocol over TCP, and an epoll-based event loop sustaining X msg/sec
  on a single broker.
• Implemented partitioned topics with leader-follower replication and in-sync replica (ISR)
  tracking, achieving automatic failover in <Xms with zero acknowledged message loss.
• Built a Go client SDK with batched async producers, consumer group rebalancing via a
  cooperative protocol, and a CLI tool for topic management and real-time tail.
• Reduced consumer read latency by X% using zero-copy sendfile() and memory-mapped segment
  indices, benchmarked at X MB/sec sustained throughput per partition.
```
