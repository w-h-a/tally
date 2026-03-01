 # Tally                                                                                               

## Problem

Travis Jeffery's [proglog](https://github.com/travisjeffery/proglog) teaches the right patterns but is frozen at Go 1.13 with deprecated dependencies.

## Solution

Tally is the spiritual successor — a single-topic distributed commit log written for Go 1.23+ with observability designed in. Single binary. Zero cloud dependencies.

- **Log-structured storage** — append-only store with memory-mapped index, segmented for rotation
- **gRPC API** — Produce, Consume, and streaming RPCs
- **Raft consensus** — leader accepts writes, followers serve reads, automatic failover
- **Serf discovery** — gossip-based cluster membership
- **mTLS + ACL** — mutual TLS authentication with Casbin policy authorization
- **OpenTelemetry** — traces and metrics on every operation, structured logging with trace correlation

## Architecture

```mermaid
graph TB
    Client[Client]

    subgraph Node1["Node 1 (Leader)"]
        G1[gRPC Server]
        DL1[DistributedLog]
        R1[Raft]
        L1[Log]
        Seg1["Segments (Store + Index)"]
        G1 --> DL1
        DL1 --> R1
        DL1 -.->|CommitLog interface| L1
        L1 --> Seg1
    end

    subgraph Node2["Node 2 (Follower)"]
        G2[gRPC Server]
        DL2[DistributedLog]
        R2[Raft]
        L2[Log]
        Seg2["Segments (Store + Index)"]
        G2 --> DL2
        DL2 --> R2
        DL2 -.->|CommitLog interface| L2
        L2 --> Seg2
    end

    subgraph Node3["Node 3 (Follower)"]
        G3[gRPC Server]
        DL3[DistributedLog]
        R3[Raft]
        L3[Log]
        Seg3["Segments (Store + Index)"]
        G3 --> DL3
        DL3 --> R3
        DL3 -.->|CommitLog interface| L3
        L3 --> Seg3
    end

    Client -->|Produce| G1
    Client -->|Consume| G2
    Client -->|Consume| G3

    R1 -->|replicate| R2
    R1 -->|replicate| R3

    Node1 <-->|Serf gossip| Node2
    Node1 <-->|Serf gossip| Node3
    Node2 <-->|Serf gossip| Node3
```

Writes go to the Raft leader. Reads are served by any node. Serf gossip handles cluster membership. Each node's local log is an ordered set of segments, each pairing an append-only store file with a  memory-mapped index.

## Usage

Coming soon!