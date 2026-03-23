 # Tally        

<div align="center">
  <img src="./.github/assets/tally.png" alt="Tally Mascot" />
</div>

## Problem

Travis Jeffery's [proglog](https://github.com/travisjeffery/proglog) teaches the right patterns but is frozen at Go 1.13 with deprecated dependencies.

## Solution

Tally is the spiritual successor — a single-topic distributed commit log written for Go 1.23+ with observability designed in. Single binary. Zero cloud dependencies.

- **Log-structured storage** — append-only store with memory-mapped index, segmented for rotation and retention
- **gRPC API** — Produce, Consume, streaming RPCs, and consumer offset tracking 
- **Raft consensus** — leader accepts writes, followers serve reads, automatic failover
- **Serf discovery** — gossip-based cluster membership
- **mTLS + ACL** — mutual TLS authentication with Casbin policy authorization
- **OpenTelemetry** — traces and metrics on every operation, structured logging with trace correlation
- **Deterministic simulation testing** — fault-injecting transport and store wrappers with seed-based reproducibility
- **CLI** — produce, consume, and cluster inspection from the command line

## Architecture

```mermaid
graph TB
    Client[Client]

    subgraph Node1["Node 1 (Leader)"]
        G1[gRPC Server]
        DL1[DistributedLog]
        OFS1[OffsetService]
        R1[Raft]
        L1[Log]
        Seg1["Segments (Store + Index)"]
        FOS1[FileOffsetStore]
        G1 --> DL1
        G1 --> OFS1
        DL1 -.->|Consensus| R1
        DL1 -.->|CommitLog| L1
        OFS1 -.->|OffsetStore| FOS1
        L1 --> Seg1
    end

    subgraph Node2["Node 2 (Follower)"]
        G2[gRPC Server]
        DL2[DistributedLog]
        OFS2[OffsetService]
        R2[Raft]
        L2[Log]
        Seg2["Segments (Store + Index)"]
        FOS2[FileOffsetStore]
        G2 --> DL2
        G2 --> OFS2
        DL2 -.->|Consensus| R2
        DL2 -.->|CommitLog| L2
        OFS2 -.->|OffsetStore| FOS2
        L2 --> Seg2
    end

    subgraph Node3["Node 3 (Follower)"]
        G3[gRPC Server]
        DL3[DistributedLog]
        OFS3[OffsetService]
        R3[Raft]
        L3[Log]
        Seg3["Segments (Store + Index)"]
        FOS3[FileOffsetStore]
        G3 --> DL3
        G3 --> OFS3
        DL3 -.->|Consensus| R3
        DL3 -.->|CommitLog| L3
        OFS3 -.->|OffsetStore| FOS3
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

Writes go to the Raft leader. Reads are served by any node. Serf gossip handles cluster membership. Each node's local log is an ordered set of segments, each pairing an append-only store file with a memory-mapped index.

## Usage

Coming soon!
