# Redpanda — Partitioning Deep Dive

> A systems engineering project that traces how Redpanda distributes,
> replicates, and stores data across partitions — from source code
> to live experimentation.

---

## Table of Contents
- [Project Overview](#project-overview)
- [Repository Structure](#repository-structure)
- [Environment Setup](#environment-setup)
- [Running Redpanda](#running-redpanda)
- [Source Code Trace](#source-code-trace)
- [Design Decisions](#design-decisions)
- [Experiments](#experiments)
- [Key Findings](#key-findings)
- [References](#references)

---

## Project Overview

This project investigates how Redpanda implements partitioning at the
source-code level. Rather than relying on documentation alone, the goal
is to trace actual C++ code paths — from the moment a producer sends a
message, through the Raft consensus layer, all the way to physical disk
storage via DMA writes.

**Core questions driving this project:**
- How does a message get assigned to a partition?
- How does Redpanda use Raft to replicate partitions across nodes?
- What happens to partition data when a broker fails?
- What are the performance trade-offs of Redpanda's thread-per-core architecture?

---

## Repository Structure

```
redpanda-partitioning/
├── README.md
├── week1-notes.md
├── report/
│   ├── 1_introduction.md
│   ├── 2_system_design.md
│   ├── 3_observation.md
│   └── 4_failure_analysis.md
└── experiments/
    ├── docker-compose.yml
    ├── exp1_partition_throughput.py
    ├── exp2_leader_failover.py
    ├── exp3_partition_skew.py
    └── results/
        ├── exp1_output.txt
        ├── exp2_output.txt
        └── exp3_output.txt
```

---

## Environment Setup

**System:** Ubuntu 26.04 LTS on WSL2 (Windows 11)  
**Redpanda version:** 26.1.7-1  
**RPK version:** 26.1.7-1  
**CPU cores:** 8  

```bash
# Install dependencies
sudo apt install -y git curl wget unzip python3 python3-pip ripgrep

# Install rpk
curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip
unzip rpk-linux-amd64.zip
sudo mv rpk /usr/local/bin/

# Install Redpanda
curl -1sLf \
  'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' \
  | sudo -E bash
sudo apt-get install -y redpanda

# Install Python client
python3 -m pip install --break-system-packages kafka-python
```

---

## Running Redpanda

Single node (for Experiments 1 and 3):

```bash
sudo rpk redpanda start \
  --overprovisioned \
  --smp 1 \
  --memory 1G \
  --reserve-memory 0M \
  --node-id 0 \
  --check=false \
  --install-dir /opt/redpanda &

rpk cluster info --brokers localhost:9092
```

3-node cluster (for Experiment 2):

```bash
cd experiments/
docker-compose up -d
sleep 15
rpk cluster info --brokers localhost:9092
```

---

## Source Code Trace

The complete write path through 4 layers:

```
produce_handler::handle()         [produce.cc:639]
        ↓
partition_append()                [produce.cc:135]
        ↓
partition.replicate()             [produce.cc:152]
        ↓
_raft->replicate()                [partition.cc:348]
        ↓
append_entries_request            [consensus.cc:721]
        ↓
.append_entries() RPC             [consensus.cc:738]
        ↓
disk_log_impl::make_appender()    [disk_log_impl.cc:2093]
        ↓
disk_log_appender::operator()     [disk_log_appender.cc:81]
        ↓
segment_appender::append()        [segment_appender.cc:111]
        ↓
dma_write() to disk               [segment_appender.cc:648]
```

---

## Design Decisions

| Decision | Code Reference | Problem Solved | Trade-off |
|---|---|---|---|
| Thread-per-core (Seastar) | produce.cc:259, shard_table.h:47 | No locks, no context switching | No automatic load rebalancing |
| Raft before ack | partition.cc:348, consensus.cc:721 | Zero data loss for acked writes | Network round-trip per produce |
| Append-only + DMA | segment_appender.cc:648 | Predictable low latency writes | Compaction needed for cleanup |
| murmur2 key hashing | hashing/murmur.h | Kafka-compatible partition routing | Skewed keys = hot partition |

---

## Experiments

### Experiment 1: Partition Count vs Throughput

| Partitions | Time (s) | Msgs/sec | MB/sec |
|------------|----------|----------|--------|
| 1          | 1.19     | 4196     | 4.10   |
| 3          | 0.95     | 5263     | 5.14   |
| 6          | 1.36     | 3679     | 3.59   |
| 12         | 1.36     | 3685     | 3.60   |

Peak throughput at 3 partitions. Plateau confirmed at 6 and 12.
Proves thread-per-core scaling behaviour.
Python client became bottleneck before broker cores were saturated.

---

### Experiment 2: Partition Leader Failover

| Metric | Value |
|---|---|
| Total messages | 150 |
| Errors | 0 |
| Failover spike | 7230.2ms (message 33) |
| Normal latency | ~2-4ms |
| Data loss | Zero |

Raft elected new leader within ~7 seconds.
Zero errors. Zero data loss. Instant recovery after election.
Proves consensus.cc leader election code path experimentally.

---

### Experiment 3: Partition Skew

| Scenario | Msgs/sec | Hot Partition |
|---|---|---|
| Skewed (fixed key) | 3754 | P1: 100% of traffic |
| Even (varied keys) | 4107 | P0/P1/P2: ~33% each |

9.4% throughput gain from even key distribution.
Fixed key routed 100% to Partition 1 — proves murmur2 determinism.
Thread-per-core cannot rebalance skewed load automatically.

---

## Key Findings

1. **Partition count should match client capability not just core count**  
   Peak at 3 partitions despite 8 available cores — Python client
   became the bottleneck before the broker was saturated.

2. **Raft guarantees zero data loss with a measurable failover window**  
   7.2 second election window observed experimentally. Zero errors
   and zero data loss with acks=all and retries=10.

3. **Key design is the most critical operational decision**  
   Fixed keys cause 100% skew to one partition and 9.4% throughput
   loss. murmur2 with varied keys gives near-perfect distribution.

4. **DMA writes bypass page cache for predictable latency**  
   Every write is a direct DMA operation (segment_appender.cc:648)
   — no page cache eviction surprises, predictable tail latency.

---

## References

- [Redpanda GitHub Source](https://github.com/redpanda-data/redpanda)
- [Redpanda Architecture Docs](https://docs.redpanda.com/current/reference/architecture/)
- [Raft Consensus Algorithm](https://raft.github.io/)
- [Seastar Framework](https://seastar.io/)
- [MurmurHash](https://github.com/aappleby/smhasher)
- [Kafka Partitioning](https://kafka.apache.org/documentation/)
