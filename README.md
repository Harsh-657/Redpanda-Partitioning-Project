# Redpanda — Partitioning Deep Dive

> A systems engineering project that traces how Redpanda distributes, replicates, and stores data across partitions — from source code to live experimentation.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Repository Structure](#repository-structure)
- [Environment Setup](#environment-setup)
- [Running Redpanda Locally](#running-redpanda-locally)
- [Source Code Trace](#source-code-trace)
- [Design Decisions](#design-decisions)
- [Experiments](#experiments)
- [Failure Analysis](#failure-analysis)
- [Key Findings](#key-findings)
- [References](#references)

---

## Project Overview

This project investigates **how Redpanda implements partitioning** at the source-code level. Rather than relying on documentation alone, the goal is to trace actual C++ code paths — from the moment a producer sends a message, through the Raft consensus layer, all the way to physical disk storage.

**Core questions driving this project:**
- How does a message get assigned to a partition?
- How does Redpanda use Raft to replicate partitions across nodes?
- What happens to partition data when a broker fails?
- What are the performance trade-offs of Redpanda's thread-per-core architecture?

---

## Repository Structure

```
redpanda-partitioning/
├── README.md                  ← this file
├── report.md                  ← full written report (added after analysis)
├── experiments/
│   ├── benchmark.sh           ← throughput benchmarks across partition counts
│   ├── failure_test.sh        ← leader failover simulation
│   ├── skew_test.sh           ← partition skew experiment
│   └── results/
│       ├── benchmark.csv
│       └── screenshots/
└── slides/
    └── presentation.pdf       ← final presentation deck
```

---

## Environment Setup

**System:** Ubuntu 22.04 LTS  
**Dependencies:**

```bash
sudo apt update && sudo apt upgrade -y

sudo apt install -y \
  git curl wget unzip \
  docker.io docker-compose \
  kafkacat python3 python3-pip \
  ripgrep tree

# Allow Docker without sudo
sudo usermod -aG docker $USER
newgrp docker
```

**Install `rpk` (Redpanda CLI):**

```bash
curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip
unzip rpk-linux-amd64.zip
sudo mv rpk /usr/local/bin/
rpk version
```

**Clone Redpanda source** (for code tracing — no need to compile):

```bash
git clone https://github.com/redpanda-data/redpanda.git
```

---

## Running Redpanda Locally

Single-node setup via Docker:

```bash
docker run -d --name redpanda \
  -p 9092:9092 \
  -p 9644:9644 \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start \
  --overprovisioned \
  --smp 1 \
  --memory 1G \
  --reserve-memory 0M \
  --node-id 0 \
  --check=false
```

Verify the cluster is up:

```bash
rpk cluster info --brokers localhost:9092
```

Create a test topic and produce messages:

```bash
rpk topic create my-topic --partitions 3 --replicas 1 --brokers localhost:9092
echo "hello redpanda" | rpk topic produce my-topic --brokers localhost:9092
rpk topic consume my-topic --brokers localhost:9092
rpk topic describe my-topic --brokers localhost:9092
```

---

## Source Code Trace

The write path flows through four distinct layers. Each layer hands off to the next. Below is the trace with the key files involved.

### Layer 1 — Kafka API Entry Point

The producer request enters here:

```
src/v/kafka/server/replicated_partition.h
src/v/kafka/server/replicated_partition.cc
```

Search the produce handler:
```bash
rg "produce_request" src/v/kafka/server/
```

### Layer 2 — Cluster / Partition Management

The message is assigned to a partition and handed to the cluster layer:

```
src/v/cluster/partition.h
src/v/cluster/partition.cc
```

Key function to locate:
```bash
rg "replicate" src/v/cluster/partition.cc
```

### Layer 3 — Raft Consensus

The partition leader uses Raft to replicate the entry across nodes before acknowledging the write:

```
src/v/raft/consensus.h
src/v/raft/consensus.cc
```

Key functions:
```bash
rg "append_entries" src/v/raft/
rg "do_append" src/v/raft/
```

### Layer 4 — Physical Storage

Once consensus is reached, the entry is appended to the on-disk log:

```
src/v/storage/log.h
src/v/storage/log.cc
src/v/storage/segment.cc
```

Key functions:
```bash
rg "do_write" src/v/storage/
rg "append" src/v/storage/log.cc
```

> **Full trace with exact line numbers will be added here after code analysis is complete.**

---

## Design Decisions

Three key design decisions identified in Redpanda's partitioning implementation:

| # | Decision | Location in Code | Problem Solved | Trade-off |
|---|---|---|---|---|
| 1 | Thread-per-core architecture (Seastar) | `src/v/application.cc` | Eliminates context switching overhead | Cross-core partition communication is more complex |
| 2 | Raft-based replication (no ZooKeeper) | `src/v/raft/consensus.cc` | Removes external dependency, faster failover | Raft adds implementation complexity |
| 3 | Append-only segment storage | `src/v/storage/segment.cc` | Sequential writes maximize disk throughput | Compaction/cleanup required for old data |

> **Detailed analysis with code references will be filled in after the deep dive.**

---

## Experiments

### Experiment 1 — Partition Count vs. Throughput

Vary the number of partitions (1, 3, 6, 12) and measure producer throughput for a fixed message count.

```bash
# Example: produce 100k messages and time it
time seq 1 100000 | kafkacat -P -b localhost:9092 -t test-topic
```

**Expected outcome:** Throughput should increase with partition count up to the number of available cores, then plateau or degrade. *(Results to be added.)*

---

### Experiment 2 — Leader Failover

Run a 3-node cluster, kill the partition leader, and observe how long re-election takes.

```bash
# Bring up 3-node cluster
docker compose up -d

# Kill the leader
docker stop redpanda-1

# Watch recovery
rpk cluster info
```

**Expected outcome:** Raft should elect a new leader within seconds. Consumer lag during this window will be measured. *(Results to be added.)*

---

### Experiment 3 — Partition Skew

Route 90% of traffic to a single partition and observe throughput and latency compared to a balanced distribution.

*(Script and results to be added.)*

---

## Failure Analysis

The following failure scenarios will be analyzed based on experiment results:

1. **What happens when data size increases significantly across partitions?**
   - Expected: Segment rollover triggers, compaction load increases, storage I/O becomes the bottleneck.

2. **What happens under partition skew (one partition gets 90% of traffic)?**
   - Expected: The core handling that partition saturates while others sit idle — the thread-per-core model has no load balancing within a node.

3. **What happens if a Raft leader crashes mid-write?**
   - Expected: Raft ensures uncommitted entries are rolled back; no data loss for acknowledged writes.

4. **What assumptions does Redpanda's partitioning rely on?**
   - Sequential disk I/O being fast, network latency between replicas being low, and producer keys being well-distributed.

> **Findings will be updated after experiments are run.**

---

## Key Findings

*(To be completed after code tracing and experiments.)*

Preliminary expectations:
- Redpanda's tight coupling of Raft and storage gives it a latency advantage over Kafka for replication acknowledgment.
- The thread-per-core model means partition count should be tuned to match CPU core count for optimal performance.
- Leader failover is fast but not instantaneous — there is a measurable unavailability window during election.

---

## References

- [Redpanda GitHub Source](https://github.com/redpanda-data/redpanda)
- [Redpanda Architecture Docs](https://docs.redpanda.com/current/reference/architecture/)
- [Raft Consensus Algorithm](https://raft.github.io/)
- [Seastar Framework](https://seastar.io/)
- [Apache Kafka Partitioning (for comparison)](https://kafka.apache.org/documentation/#design_partitionsreplication)
