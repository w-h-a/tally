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
- **OpenTelemetry** — traces and metrics on every operation, structured logging with trace correlation
- **Deterministic simulation testing** — fault-injecting transport and store wrappers with seed-based reproducibility
- **CLI** — produce, consume, and cluster inspection from the command line

## Architecture

```mermaid
graph TB
    Client[Client]

    subgraph Node2["Node 2 (Follower)"]
        G2[gRPC Server]
        MEM2[MembershipService]
        R2[Raft]
        S2[Serf]
        DL2[DistributedLog]
        L2[FileCommitLog]
        Seg2["Segments (Store + Index)"]
        FOS2[FileOffsetStore]
        G2 --> DL2
        MEM2 -.->|Discovery| S2
        MEM2 -.->|Consensus| R2
        DL2 -.->|Consensus| R2
        DL2 -.->|OffsetStore| FOS2
        DL2 -.->|CommitLog| L2
        L2 --> Seg2
    end

    subgraph Node1["Node 1 (Leader)"]
        G1[gRPC Server]
        DL1[DistributedLog]
        L1[FileCommitLog]
        Seg1["Segments (Store + Index)"]
        FOS1[FileOffsetStore]
        MEM1[MembershipService]
        R1[Raft]
        S1[Serf]
        G1 --> DL1
        DL1 -.->|CommitLog| L1
        DL1 -.->|OffsetStore| FOS1
        DL1 -.->|Consensus| R1
        MEM1 -.->|Consensus| R1
        MEM1 -.->|Discovery| S1
        L1 --> Seg1
    end

    Client -->|Produce| G1
    Client -->|Consume| G2

    R1 -->|replicate| R2

    S1 <-->|gossip| S2
```

Writes go to the Raft leader. Reads are served by any node. Serf gossip handles cluster membership. Each node's local log is an ordered set of segments, each pairing an append-only store file with a memory-mapped index. Consumer offsets are replicated through the same consensus path.

#### Consensus

```mermaid
sequenceDiagram
      participant C as Client
      participant H as gRPC Handler
      participant S as DistributedLog (Leader)
      participant RC as raftConsensus (Leader)
      participant LR as Raft (Leader)
      participant LF as fsm (Leader)
      participant LCL as CommitLog (Leader)
      participant FR as Raft (Follower)
      participant FF as fsm (Follower)
      participant FS as DistributedLog (Follower)
      participant FCL as CommitLog (Follower)

      C->>H: Produce(record)
      H->>S: Append(ctx, record)
      S->>S: marshal record with type prefix
      S->>RC: Apply(ctx, data)
      Note over RC: starts consensus.Apply span
      RC->>LR: raft.Apply(data, timeout)
      Note over LR: append to Raft log
      LR->>FR: AppendEntries over TCP (streamLayer)
      FR-->>LR: ack
      Note over LR: quorum reached (2 of 3)
      LR->>LF: fsm.Apply(log)
      LF->>S: applyFn(log.Data) — closure
      S->>LCL: commitlog.Append(ctx.Background(), rec)
      LCL-->>S: offset
      S-->>LF: fsmResponse{offset, nil}
      LF-->>LR: fsmResponse
      LR-->>RC: future.Response()
      RC-->>S: offset
      S-->>H: offset
      H-->>C: ProduceResponse{offset}

      Note over FR: committed, apply locally
      FR->>FF: fsm.Apply(log)
      FF->>FS: applyFn(log.Data) — closure
      FS->>FCL: commitlog.Append(ctx.Background(), rec)
      FCL-->>FS: offset
      FS-->>FF: fsmResponse{offset, nil}
```

#### Discovery

Coming soon!

## Usage

Coming soon!
