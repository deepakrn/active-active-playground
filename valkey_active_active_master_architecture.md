# Valkey Active-Active Replication: Master Architecture Specification
**Document Version**: 1.0.0-PROD  
**Target System**: Valkey Core (v8.0+ / v9.0-dev)  
**Status**: Authoritative Architectural Design  
**Classification**: Engineering Architecture Specification  

---

# Table of Contents
1. [Executive Summary & Architectural Foundations](#1-executive-summary--architectural-foundations)
2. [Domain 1: Product Use Cases & Workload Profiles](#2-domain-1-product-use-cases--workload-profiles)
3. [Domain 2: Topologies & Connection Meshes](#3-domain-2-topologies--connection-meshes)
4. [Domain 3: Primaryship, Writeability & Sharding Models](#4-domain-3-primaryship-writeability--sharding-models)
5. [Domain 4: Replication Engine, Protocols & Stream Offsets](#5-domain-4-replication-engine-protocols--stream-offsets)
6. [Domain 5: Clocks, Ordering & Metadata Primitives](#6-domain-5-clocks-ordering--metadata-primitives)
7. [Domain 6: Conflict Resolution Mechanisms & CRDT Models](#7-domain-6-conflict-resolution-mechanisms--crdt-models)
8. [Domain 7: Data Structure Command Matrix & Resolution Semantics](#8-domain-7-data-structure-command-matrix--resolution-semantics)
9. [Domain 8: Deletions, Tombstones & Anti-Entropy Garbage Collection](#9-domain-8-deletions-tombstones--anti-entropy-garbage-collection)
10. [Domain 9: Network Partitions & Outage Reconciliation](#10-domain-9-network-partitions--outage-reconciliation)
11. [Domain 10: Engine Architecture, Extensibility & Persistence Subsystems](#11-domain-10-engine-architecture-extensibility--persistence-subsystems)
12. [Domain 11: Observability, Telemetry & Diagnostics Subsystems](#12-domain-11-observability-telemetry--diagnostics-subsystems)
13. [Domain 12: Architectural Synthesis, Decision Framework & Implementation Roadmap](#13-domain-12-architectural-synthesis-decision-framework--implementation-roadmap)

---

# 1. Executive Summary & Architectural Foundations

## 1.1 The Active-Active Imperative
Valkey's baseline architecture relies on an asynchronous, single-primary, multi-replica replication model. Writes are serialized strictly on a single primary node per hash slot, generating an append-only replication stream identified by a 40-character hexadecimal Replication ID (`replid`) and a monotonically increasing 64-bit scalar offset (`master_repl_offset`). Replicas consume this stream unidirectionally.

Transitioning Valkey to a multi-region, Active-Active (multi-primary / multi-writable) model breaks four foundational invariants of the engine:
1. **Concurrency Invariant**: Writes occur concurrently at multiple geographical sites without synchronous distributed coordination.
2. **Replication Stream Linearity Invariant**: Multiple primaries emit distinct write streams concurrently, invalidating the scalar replication offset and single `replid`.
3. **Non-Destructive Synchronization Invariant**: Traditional full synchronization (`PSYNC`) flushes the replica's keyspace (`emptyDb()`) before loading an RDB snapshot, which permanently destroys un-replicated local concurrent writes in an Active-Active topology.
4. **Causality & Partial Ordering Invariant**: Network latencies across Wide Area Networks (WANs) introduce out-of-order delivery, packet loss, and asymmetric link partitions.

## 1.2 The PACELC Governing Spectrum
Active-Active data storage across WANs is governed by the PACELC theorem:
$$\text{If Partition } (P) \implies \text{Trade off Availability } (A) \text{ vs Consistency } (C); \quad \text{Else } (E) \implies \text{Trade off Latency } (L) \text{ vs Consistency } (C)$$

In-memory key-value stores are fundamentally deployed for sub-millisecond read/write latencies ($\le 1\text{ ms}$). Introducing synchronous multi-datacenter consensus (e.g., Multi-Paxos, Raft, 2-Phase Commit) introduces WAN round-trip times ($\text{RTT} \approx 30\text{--}150\text{ ms}$), violating the core value proposition. Therefore, Active-Active Valkey requires:
- **Local-Latency Writes**: Executed in RAM with $O(1)$ time complexity and zero WAN round-trips.
- **Asynchronous Cross-Datacenter Replication**: Streamed non-blockingly over persistent inter-region channels.
- **Strong Eventual Consistency (SEC)**: Replicas that have received the same set of updates reach identical states regardless of update arrival order.

```
+---------------------------------------------------------------------------------------------------+
|                                  PACELC Spectrum for Valkey Systems                                |
|                                                                                                   |
|   Synchronous Distributed Consensus (Raft/Paxos)          Asynchronous CRDTs / HLC-LWW             |
|   - Write Latency: 30ms - 150ms (WAN RTT)                 - Write Latency: < 1ms (Local RAM)       |
|   - Consistency: Linearizable / External Consistency      - Consistency: Strong Eventual (SEC)     |
|   - Partition: Minority partition unavailable (CP)        - Partition: 100% Writable (AP)          |
|                                                                                                   |
|   ========================================================> TARGET ARCHITECTURAL FOCUS            |
+---------------------------------------------------------------------------------------------------+
```

---

# 2. Domain 1: Product Use Cases & Workload Profiles

## 2.1 Technical Problem Statement
Different enterprise workloads exhibit orthogonal consistency, latency, and conflict requirements. A monolithic Active-Active replication engine cannot treat all data structures and access patterns identically without suffering severe memory bloat or silent data loss.

---

## 2.2 Workload Profile Deep Dives

### 2.2.1 Active-Active Disaster Recovery (Zero-RPO / Low-RTO Failover)
- **Problem Statement**: Production traffic is split across DC-East and DC-West. If DC-East suffers an abrupt power outage, fiber cut, or region failure, client traffic is dynamically rerouted to DC-West without administrative intervention, promotion lag, or split-brain data corruption.
- **Invariants**:
  - Recovery Point Objective (RPO): $\text{RPO} \le \Delta t_{\text{WAN}}$ (replication lag window, typically $5\text{--}100\text{ ms}$).
  - Recovery Time Objective (RTO): $\text{RTO} \approx 0$ (no manual cluster failover, no cross-WAN leader election).
  - Post-Partition Re-convergence: When DC-East recovers, divergent branches merge without data truncation.
- **Technical Options**:
  - **Option 1.1A: Asynchronous Hybrid Logical Clock Last-Writer-Wins (HLC-LWW)**. Every write is tagged with an HLC. During cross-DC replication, the higher HLC overwrites the lower.
  - **Option 1.1B: State-Based Replicated CRDTs with Continuous Anti-Entropy**. Complete CRDT state trees are maintained, guaranteeing mathematical join-semilattice convergence.
  - **Option 1.1C: Active-Passive 2-Way Mirroring with Epoch Fencing**. Single writable primary per slot; standby receives asynchronous replication stream. On failover, an epoch-fenced coordinator transitions write ownership.

### 2.2.2 Multi-Region Low-Latency Local Writes (Follow-the-Sun / Global Edge)
- **Problem Statement**: Users in Europe write to `eu-west`, users in the US write to `us-east`, users in Asia write to `ap-northeast`. All regions observe a globally converged keyspace while serving local reads/writes at $<1\text{ ms}$.
- **Invariants**:
  - Write Path Latency: $\le 1\text{ ms}$ (RAM mutation + non-blocking WAN dispatch).
  - Monotonic Read Consistency ($W \to R$): A client pinned to a region must observe its own writes monotonically.
  - Causal Consistency: If write $B$ causally depends on write $A$ ($A \to B$), no region applies $B$ before $A$.
- **Technical Options**:
  - **Option 1.2A: Uncoordinated Operation-Based CRDT Broadcast**. Writes applied immediately locally; operation deltas broadcast to all peer DCs asynchronously.
  - **Option 1.2B: Causal Dependency Vector Framing**. Replication frames embed causal dependency vectors; remote nodes buffer out-of-order frames in a staging DAG until predecessors are satisfied.
  - **Option 1.2C: Strict Geographic Key-Range Partitioning with Async Read Replicas**. Keys are strictly owned by specific regions; foreign writes are proxied over WAN.

### 2.2.3 Distributed Session Stores & Shopping Carts
- **Problem Statement**: Web sessions, shopping carts (`HSET cart:123 item_id qty`), and sliding TTLs (`EXPIRE cart:123 3600`) where users roam across geographic edges.
- **Invariants**:
  - Cart Item Convergence: Adding item $X$ in Region 1 and item $Y$ in Region 2 yields $\{X, Y\}$, not $\{X\}$ or $\{Y\}$.
  - Sliding TTL Renewal: TTL refresh in DC-1 must not prematurely expire the key in DC-2 due to clock skew.
- **Technical Options**:
  - **Option 1.3A: Field-Level LWW Hash with Max-TTL Expiration**. Each hash field maintains an independent HLC. Key TTL updates take $\max(\text{TTL}_{\text{local}}, \text{TTL}_{\text{remote}})$.
  - **Option 1.3B: Observed-Removed Set (OR-Set / Add-Wins Set) with Per-Field Registers**. Cart items modeled as OR-Sets with unique add tags; deletion removes only observed tags.
  - **Option 1.3C: Document-Level JSON CRDT**. Sub-document path-level patching with monotonic version vectors.

### 2.2.4 Distributed Counters, Global Rate Limiters & Token Buckets
- **Problem Statement**: High-frequency distributed counters (`INCR`, `INCRBY`), global rate limiters (e.g., 10,000 API requests/minute across 3 DCs), and inventory stock decrement.
- **Invariants**:
  - Commutativity: $\text{INCR}(+5) \circ \text{INCR}(+3) = \text{INCR}(+3) \circ \text{INCR}(+5) = +8$.
  - No Lost Increments: Concurrent increments across $N$ datacenters sum exactly.
  - Floor Bounds: Decrements must not breach invariant $\text{Value} \ge 0$.
- **Technical Options**:
  - **Option 1.4A: Positive-Negative Counter (PN-Counter / Distributed Vector Accumulator)**. Node maintains state vectors $\vec{P} = [p_1, \dots, p_N]$ and $\vec{N} = [n_1, \dots, n_N]$. Value is $\sum p_i - \sum n_i$. Deltas $\Delta = \pm k$ broadcast asynchronously.
  - **Option 1.4B: Token Lease Slicing (Hierarchical Quota Pre-Allocation)**. A coordinator slices quotas (e.g., each DC gets 3,333 tokens). Local DCs serve writes locally from local slice.
  - **Option 1.4C: Sliding-Window ZSet CRDT**. Rate limit event timestamps recorded in a sorted set with Add-Wins semantics; trimmed via `ZREMRANGEBYSCORE`.

### 2.2.5 Distributed Streams Enqueue/Dequeue & PubSub Messaging
- **Problem Statement**: Distributed log ingestion (`XADD`), consumer group coordination (`XREADGROUP`), acknowledgment (`XACK`), and Pending Entries List (`XPEL`) management across regions.
- **Invariants**:
  - Globally Unique Entry IDs: Stream IDs (`<timestamp>-<seq>`) must be globally unique and monotonically increasing.
  - Consumer Group Convergence: `XACK` in DC-1 must remove the pending entry in DC-2.
  - Zero Message Loss: Concurrent `XADD` across DCs interleaves deterministically.
- **Technical Options**:
  - **Option 1.5A: Region-Prefixed Composite IDs with Causal Radix Merge**. Entry IDs formatted as `<HLC_ms>-<RegionID>-<LocalSeq>`. Streams merge as an append-only radix tree ordered by composite ID.
  - **Option 1.5B: Partitioned Single-Region Stream Ownership**. Each stream key is owned by one region; remote regions append via independent regional sub-streams (`events:us`, `events:eu`).
  - **Option 1.5C: Replicated Growth-Only Event Log (RGA CRDT) with Distributed PEL CRDT**. Replicated Graph/List CRDT maintains sequence; PEL modeled as Add-Wins OR-Set.

---

## 2.3 Comprehensive 4-Pillar Evaluation: Workload Options

| Workload Domain & Option | 1. Data Safety & Consistency | 2. Overhead & Performance | 3. Failure Modes & Edge Cases | 4. Complexity & Churn |
|---|---|---|---|---|
| **1.1A: Async HLC-LWW DR** | SEC guaranteed; concurrent writes to same key suffer LWW silent overwrite. | Minimal memory ($+8\text{--}16\text{ B}$/key). Zero CPU overhead. Write latency $<0.5\text{ ms}$. | Clock skew $> \Delta t_{\text{skew\_max}}$ causes newer physical writes to be dropped. | Minimal engine churn ($<3\%$); native RESP commands. |
| **1.1B: Replicated CRDT DR** | SEC guaranteed; mathematical merge preserves all concurrent mutations. | High memory ($+32\text{--}64\text{ B}$/element for tags/tombstones). Merge CPU spikes. | Tombstone heap bloat under continuous add/remove churn. | High engine churn ($>25\%$); custom CRDT type engine required. |
| **1.1C: Active-Passive Mirror** | Strong consistency per-key; zero CRDT merge anomalies. | Zero metadata memory overhead. Standard Valkey engine speed. | Split-brain under partition if both sides accept writes without fencing. | High operational complexity; external routing proxy required. |
| **1.2A: Uncoordinated Op CRDT** | SEC guaranteed; causal inversions possible without dependency tracking. | Minimal write latency ($<0.5\text{ ms}$). Lowest network framing overhead. | Dependent mutations applied out of order (e.g., child before parent). | Low engine churn; simple delta streaming. |
| **1.2B: Causal Dependency Vector** | Causal Consistency with SEC; strictly prevents causal inversions. | Latency $<0.5\text{ ms}$ local; remote apply delayed. Bandwidth: $O(N)$ vector. | Buffer exhaustion if dependent mutation is delayed by packet drop. | High complexity (dependency DAG, timeout fallbacks). |
| **1.2C: Geo-Partitioning Proxy** | Strict linearizability for owner region; non-local writes incur WAN latency. | High write latency for out-of-region clients ($30\text{--}150\text{ ms}$). | Total write unavailability for partition if owning DC fails. | Low engine churn; high client SDK routing complexity. |
| **1.3A: Field-Level LWW Hash** | Field-level SEC; concurrent edits to different fields merge cleanly. | Memory: $+8\text{--}16\text{ B}$ per field. Fast integer timestamp compare. | Premature tombstone purge causes field resurrection. | Low engine churn; fits directly into `dictEntry` metadata. |
| **1.3B: Add-Wins OR-Set Cart** | Add-Wins guarantee: concurrent `ADD` and `DEL` resolves to `ADD`. Zero loss. | Memory: $+32\text{--}64\text{ B}$ per element for tag sets and tombstones. | Tombstone memory leak without stable horizon GC. | Moderate-to-high engine complexity. |
| **1.3C: Document JSON CRDT** | Fine-grained JSON path merge; deterministic conflict resolution. | High memory and CPU serialization overhead (JSON parsing/AST). | High CPU latency spikes during merge of large nested trees. | Severe engine complexity; full JSON engine overhaul. |
| **1.4A: PN-Counter** | 100% accurate eventual sum. Commutative deltas. Cannot enforce floor $>0$. | Memory: $+8\text{ B} \times N_{\text{nodes}}$ per counter. Lightweight integer deltas. | Under partition, counters increment freely; global quota can be exceeded. | Low complexity; straightforward delta-replication. |
| **1.4B: Token Lease Slicing** | Enforces strict floor invariants ($\text{Val} \ge 0$) without global locking. | Zero metadata overhead. Zero WAN bandwidth during steady consumption. | Under partition, exhausted DC cannot claim unspent tokens from peers. | Medium operational complexity (lease sizing, coordinator). |
| **1.4C: Sliding-Window ZSet** | Exact rate limit enforcement within bounded time drift. | High memory ($O(\text{events})$ in window). High CPU (`ZREMRANGEBYSCORE`). | ZSet size explosion under DDoS traffic or GC lag. | Moderate complexity (ZSet GC, TTL management). |
| **1.5A: Region-Prefixed HLC Streams** | All messages preserved. Deterministic append ordering in Radix tree. | Stream memory overhead: $+4\text{ B}$ per entry (Region ID). Fast radix append. | Consumers using `>` may skip messages inserted with older timestamps. | Moderate complexity (radix tree out-of-order insert). |
| **1.5B: Partitioned Single-Region** | Strict FIFO order guaranteed by single primary. Cross-region writes incur WAN. | Zero CRDT overhead. Standard Valkey stream performance. | Region failure halts write availability for owned streams. | Low engine complexity; high client routing complexity. |
| **1.5C: RGA CRDT + OR-Set PEL** | Strict causal delivery. Consumer group PEL state converges with Add-Wins. | High memory overhead (RGA link nodes, tombstone markers, tag sets). | Complex conflict if two consumers in different DCs claim same message. | Extremely high engine complexity (full CRDT stream rewrite). |

---

## 2.4 Master Comparison Matrix: Workload Profiles vs Architectural Requirements

| Workload Profile | Dominant Consistency Model | Optimal Conflict Resolution Strategy | Replication Granularity | Memory Overhead per Object | Write Latency Target |
|---|---|---|---|---|---|
| **Active-Active DR** | Strong Eventual Consistency (SEC) | Hybrid Logical Clock LWW / Multi-Master CRDT | Key-Level / Operation-Based | Low ($8\text{--}16\text{ B}$) | Local write ($<1\text{ ms}$), async WAN ($5\text{--}50\text{ ms}$) |
| **Multi-Region Low Latency** | Causal Consistency with Monotonic Reads | Commutative Delta-CRDT / HLC-LWW | Command/Operation Delta | Low-to-Medium ($16\text{--}32\text{ B}$) | Local write ($<1\text{ ms}$) |
| **Session Stores & Carts** | Field-Level SEC / Add-Wins | Field HLC LWW + Add-Wins OR-Set | Sub-key / Field-Level | Medium ($24\text{--}48\text{ B}$ per field) | Local write ($<1\text{ ms}$) |
| **Distributed Counters / Quotas** | Commutative Monotonic Convergence | PN-Counter / Vector Accumulator | Atomic Delta ($\Delta \pm k$) | Minimal ($8\text{ B} \times N_{\text{nodes}}$) | Local write ($<1\text{ ms}$) |
| **Streams / PubSub** | Causal Total Order / Add-Wins PEL | Region-Tagged HLC Composite Radix Merge | Stream Entry Append Delta | Medium ($16\text{--}32\text{ B}$ per msg) | Local write ($<1\text{ ms}$) |

---

# 3. Domain 2: Topologies & Connection Meshes

## 3.1 Technical Problem Statement & Graph Invariants
Replication topology dictates how mutation frames propagate across $N$ geographically separated datacenters (DCs). The topology must enforce four graph invariants:
1. **Liveness & Reachability**: Every mutation originated at node $u$ must reach every healthy node $v \in V$ ($u \rightsquigarrow v$).
2. **Acyclicity & Loop Termination**: Replication frames must terminate without circulating indefinitely in cycles ($u \to v \to w \to u$).
3. **WAN Egress Bandwidth Bound**: WAN egress amplification must scale sub-quadratically where possible.
4. **Partition Tolerance**: Asymmetric link failures (e.g., DC-1 reaches DC-2, DC-2 reaches DC-3, but DC-1 cannot reach DC-3) must not halt replication across healthy components.

```
       Option 2.1: Full Mesh                    Option 2.2: Hub-and-Spoke
       
            [ DC-1 ]                                      [ Hub DC ]
            /   |   \                                    /    |    \
           /    |    \                                  /     |     \
     [ DC-2 ]---|---[ DC-3 ]                      [ Spoke 1 ] [ Spoke 2 ] [ Spoke 3 ]
           \    |    /
            \   |   /
            [ DC-4 ]
            
       Option 2.3: Ring/Chain                   Option 2.4: Hierarchical Gateway Mesh
       
    [ DC-1 ] ---> [ DC-2 ]                         [ Intra-DC-1 Cluster ]
       ^             |                             (Primaries <-> Replicas)
       |             v                                       ||  (Aggregated TLS Pipe)
    [ DC-4 ] <--- [ DC-3 ]                         [ Intra-DC-2 Cluster ]
```

---

## 3.2 Deep Dive: Four Concrete Topology Options

### Option 2.1: Full Mesh Topology ($N \times (N-1)$ Unidirectional Virtual Channels)
- **Mechanics**: Every cluster primary establishes direct, persistent TCP/TLS replication connections to corresponding peer primaries across all $N-1$ remote DCs.
  - Local writes are broadcast in parallel to all $N-1$ peer connections: $\text{Egress Amplification} = N - 1$.
  - Loop prevention is enforced via an Origin Identifier (`origin_id`) embedded in the wire metadata framing. A node receiving a replication frame from a peer **applies it locally but does NOT re-broadcast it**.
- **Formal Properties**: Diameter $D = 1$ hop; Replication latency $T_{\text{repl}} = \text{RTT}_{\text{direct}}$; WAN connections per shard = $\frac{N(N-1)}{2}$.

### Option 2.2: Hub-and-Spoke / Sequencer Topology
- **Mechanics**: A designated central datacenter acts as the Hub / Sequencer. All peripheral Spokes connect strictly to the Hub.
  - Spoke writes are sent to the Hub. The Hub assigns a global sequence number / HLC, applies the write, and fans out the replication stream to all other Spokes.
  - If a Spoke becomes disconnected from the Hub, it is isolated from cross-DC sync.
- **Formal Properties**: Diameter $D = 2$ hops; Replication latency $T_{\text{repl}} = \text{RTT}_{\text{Spoke1-Hub}} + \text{RTT}_{\text{Hub-Spoke2}}$; WAN connections per shard = $N-1$.

### Option 2.3: Ring / Chain Topology
- **Mechanics**: Nodes are arranged in a fixed logical ring: $\text{DC}_1 \to \text{DC}_2 \to \dots \to \text{DC}_N \to \text{DC}_1$.
  - Each node replicates strictly to its downstream neighbor.
  - A mutation travels around the ring until it returns to its origin node ($\text{origin\_id} == \text{local\_id}$), where it is consumed and discarded.
  - Dynamic bypass routing: If link $\text{DC}_k \to \text{DC}_{k+1}$ fails, $\text{DC}_k$ detects heartbeat timeout and fails over to $\text{DC}_{k+2}$.
- **Formal Properties**: Diameter $D = N - 1$ hops; Replication latency $T_{\text{repl}} = \sum_{i=1}^{N-1} \text{RTT}_{i, i+1}$; WAN connections per shard = $N$.

### Option 2.4: Hierarchical Local-Cluster + Dedicated Cross-DC Gateway Mesh
- **Mechanics**: Decouples internal cluster nodes from the WAN topology.
  - **Intra-DC**: Standard Valkey cluster replication (single primary per slot with $k$ local read-replicas) over high-throughput low-latency LAN.
  - **Inter-DC**: Dedicated, active-active Replication Gateway processes (or elected Gateway Primaries) establish a Full Mesh across datacenters over persistent multiplexed TLS tunnels.
  - The Gateway ingests local replication streams from all local shards, multiplexes them into an aggregated cross-DC tunnel, and demultiplexes incoming remote streams directly to the appropriate local shard primaries.
- **Formal Properties**: WAN connections = $O(M^2)$ where $M$ is the number of Gateways per DC ($M \ll S$ shards); Intra-DC latency $<0.2\text{ ms}$; Inter-DC latency = $1\text{ hop} \times \text{WAN RTT}$.

---

## 3.3 Comprehensive 4-Pillar Evaluation: Topologies

| Evaluation Pillar | Option 2.1 (Full Mesh) | Option 2.2 (Hub-and-Spoke) | Option 2.3 (Ring / Chain) | Option 2.4 (Hierarchical Gateway Mesh) |
|---|---|---|---|---|
| **1. Data Safety & Consistency** | **Highest**: 1-hop direct delivery minimizes causal window and replication lag. No intermediate buffering loss. | **Medium**: Hub failure during transit creates split-brain and message loss for spokes. | **Low**: Long propagation pipeline increases causal reordering probability and conflict windows. | **High**: Dedicated gateway provides persistent disk/memory spooling during network blips. |
| **2. Overhead & Performance** | - Bandwidth: High ($(N-1) \times$ WAN egress).<br>- CPU: $N-1$ serialization threads.<br>- Latency: Lowest ($1\text{ RTT}$). | - Bandwidth: Lowest for spokes ($1\times$), high for hub ($N-1\times$).<br>- Latency: $2\text{ RTT}$.<br>- Hub CPU bottleneck. | - Bandwidth: Minimal ($1\times$ per node).<br>- Latency: Worst ($O(N)\text{ RTT}$).<br>- High memory buffering across chain. | - Bandwidth: Optimized (WAN compression & multiplexing over single TCP pipe).<br>- Latency: $1\text{ RTT} + 0.5\text{ ms}$ proxy delay.<br>- Zero WAN connection overhead on shards. |
| **3. Failure Modes & Edge Cases** | - Asymmetric link partitions create partial graph partitions.<br>- Socket descriptor exhaustion on large shard counts ($S \times (N-1)$). | - Single Point of Failure (Hub DC outage halts all inter-spoke sync).<br>- Complex Hub failover protocol. | - Single link failure breaks entire ring until topology dynamically self-heals.<br>- High risk of message loss during re-routing. | - Gateway crash requires rapid failover to standby Gateway.<br>- Gateway memory pressure under slow shard consumption. |
| **4. Complexity** | - Low routing complexity.<br>- High operational firewall / mesh management overhead. | - Medium complexity.<br>- High operational burden to maintain privileged Hub infra. | - High protocol complexity (ring health-check, node join/leave bypass, token circulation). | - Engine churn: Low (shards use standard replication hooks).<br>- High architectural modularity (gateway deployed as sidecar or separate daemon). |

---

## 3.4 Topologies Comparison Matrix

| Metric / Dimension | Full Mesh (2.1) | Hub-and-Spoke (2.2) | Ring / Chain (2.3) | Hierarchical Gateway (2.4) |
|---|---|---|---|---|
| **Max Network Hops (Diameter)** | $1$ | $2$ | $N - 1$ | $1$ (Inter-DC) |
| **WAN Egress per Write** | $(N - 1) \times \text{size}$ | $1 \times \text{size}$ (Spoke) | $1 \times \text{size}$ | $(N - 1) \times \text{size}$ (Compressed) |
| **WAN Connections per DC ($S$ shards)** | $S \times (N - 1)$ | $S \times 1$ | $S \times 2$ | $M \times (N - 1)$ ($M \ll S$) |
| **Partition Tolerance** | Graceful partial degradation | Catastrophic if Hub isolated | Vulnerable to link breaks | Graceful degradation with tunnel pooling |
| **Loop Prevention Technique** | Origin Tagging (Drop on receipt) | Structural (Hub routes) | Origin Tagging (Drop on loop return) | Origin Tagging + Stream ID Vector |
| **Recommended Deployment** | $N \le 4$ DCs, small shard count | Master-Hub Analytics / Ingestion | Not recommended for Active-Active | Large enterprise clusters ($S \ge 50$, $N \ge 3$) |

---

# 4. Domain 3: Primaryship, Writeability & Sharding Models

## 4.1 Technical Problem Statement & Slot Invariants
Standard Valkey Cluster divides the keyspace into $16,384$ logical hash slots (`CRC16(key) mod 16384`). Each slot is owned exclusively by exactly one primary node. Replicas operate in `READONLY` mode and redirect writes via `-MOVED <slot> <ip>:<port>`.

In an Active-Active multi-cluster architecture:
1. **Slot Ownership Scope**: Do Region A and Region B have independent 16,384-slot rings, or do they share a unified global cluster topology?
2. **Write Permissibility**: Can non-primary nodes accept writes directly, or must all writes go through a shard primary?
3. **Resharding Invariants**: During slot migration (`CLUSTER SETSLOT <slot> MIGRATING/IMPORTING`), how are concurrent cross-DC replication streams routed and merged without losing in-flight keys?

```
      Option 3.1: Inter-Cluster Shard Peering        Option 3.2: Multi-Primary Intra-Shard
      
      [ Cluster East (16384 Slots) ]                 [ Unified Global Cluster ]
       Shard 1 (Slots 0-8191)       <=== WAN ===>     Node A (East)   <--+
       Shard 2 (Slots 8192-16383)   <=== WAN ===>     Node B (West)   <--+ (All Writable for
                                                      Node C (Europe) <--+  Slot 0-16383)
      [ Cluster West (16384 Slots) ]
       Shard 1 (Slots 0-8191)
       Shard 2 (Slots 8192-16383)
```

---

## 4.2 Deep Dive: Three Sharding & Primaryship Architectures

### Option 3.1: Inter-Shard Cross-Cluster Peering (Independent Regional Clusters)
- **Mechanics**:
  - Each datacenter runs an independent, autonomous Valkey Cluster with its own epoch, configuration bus, and 16,384 hash slots.
  - Shard boundaries are configured symmetrically across clusters (e.g., Shard 1 owns slots 0--5460 in DC-East, DC-West, and DC-Europe).
  - Cross-DC replication is established strictly between **matching shard primaries**:
    $$\text{Primary}_{\text{DC-East}}(\text{Slot } k) \longleftrightarrow \text{Primary}_{\text{DC-West}}(\text{Slot } k)$$
  - Clients in DC-East query the local cluster configuration (`CLUSTER SLOTS` / `CLUSTER SHARDS`) and send writes to the local primary for Slot $k$.
  - The local primary executes the write, updates local state, and streams the CRDT mutation frame to its peer primaries in DC-West and DC-Europe.

### Option 3.2: Multi-Primary Intra-Shard Multi-Writable Nodes (Decoupled Primaryship)
- **Mechanics**:
  - Eliminates the single writable primary invariant. All nodes assigned to a shard (regardless of geographical placement) operate in `ACTIVE_ACTIVE_WRITE` mode.
  - Replicas are writable: they execute client commands locally, assign local HLC timestamps, immediately mutate in-memory state, and propagate replication streams to all sibling nodes in the shard.
  - Eliminates `READONLY` restrictions and `-MOVED` redirects across WAN regions.

### Option 3.3: Asymmetric Hash-Slot Partitioning with Remote CRDT Routing Proxy
- **Mechanics**:
  - The 16,384 slot ring is partitioned geographically (e.g., Slots 0--5460 owned by DC-East, 5461--10922 by DC-West, 10923--16383 by DC-Europe).
  - Local clients can write to *any* slot. If the slot is locally owned, it executes with standard single-master speed.
  - If the slot is remotely owned, a local Smart Proxy or CRDT Coordinator accepts the write locally, executes speculative CRDT execution, and forwards the command asynchronously to the authoritative region.

---

## 4.3 Cluster Slot Migrations & Live Resharding in Active-Active Topologies

### Breakdown of Invariants in Active-Active Resharding:
- **Challenge 1: In-Flight Cross-DC Replication During Slot Migration**: If Shard $S_1$ in DC-East is migrating Slot $k$ to Shard $S_2$ in DC-East while DC-West is streaming incoming replication writes for Slot $k$, where does DC-East route the incoming writes?
  - *Resolution Mechanism*: Source shard $S_1$ maintains a **Forwarding Proxy Table**. Incoming cross-DC replication frames for a migrating slot are applied locally if the key is still present, or transparently forwarded to target shard $S_2$ via local IPC/TCP socket.
- **Challenge 2: Metadata & Tombstone Preservation during `MIGRATE`**: Standard `DUMP`/`RESTORE` dumps the raw `robj` payload without CRDT metadata (HLC timestamps, element vector clocks, tombstones). Migrating a slot would strip CRDT metadata, causing revived zombie keys on subsequent sync!
  - *Resolution Mechanism*: Introduce `DUMP_AA` and `RESTORE_AA` commands with binary serializations of the CRDT metadata header (RDB v12+ extension framing).
- **Challenge 3: Asymmetric Topology Convergence**: DC-East completes slot migration from $S_1 \to S_2$, but DC-West still routes replication frames to $S_1$.
  - *Resolution Mechanism*: **Replication Redirection Handshake (`REPL_MOVED`)**. When $S_1$ receives a cross-DC replication frame for a slot it no longer owns, it responds with `REPL_MOVED <slot> <new_shard_gateway_or_primary_id>`, causing the remote sender to update its cross-DC peer routing map.

---

## 4.4 Comprehensive 4-Pillar Evaluation: Sharding & Primaryship

| Evaluation Pillar | Option 3.1 (Inter-Shard Cross-Cluster Peering) | Option 3.2 (Multi-Primary Intra-Shard) | Option 3.3 (Asymmetric Slot Partitioning) |
|---|---|---|---|
| **1. Data Safety & Consistency** | **High**: Clean isolation between cluster failure domains. Shard failover in DC-East does not affect DC-West cluster state. | **Medium-Low**: High risk of split-brain and divergence during cluster topology changes; cascading gossip storms. | **High**: Authoritative slot ownership simplifies linearizability for partition-specific keys. |
| **2. Overhead & Performance** | **Optimal**: Zero proxy overhead. Clients query local cluster topology directly. Local writes execute in $<0.5\text{ ms}$. | **High CPU Overhead**: Every node must perform CRDT conflict resolution and multi-directional stream multiplexing. | **Sub-optimal**: Remote writes incur proxying serialization overhead and latency penalties. |
| **3. Failure Modes & Edge Cases** | - Slot mismatch if clusters reshard independently without global slot alignment.<br>- Primary failover in DC-East requires DC-West to repoint peer connection to newly promoted replica. | - Gossip bus thrashing under WAN partitions.<br>- Multiple primaries claiming slot ownership simultaneously. | - Total write unavailability for slot subset if owning DC goes offline completely. |
| **4. Complexity** | **Low-to-Medium**: Preserves standard Valkey cluster engine internally; cross-cluster peering handled at primary replication layer. | **Extreme**: Fundamental redesign of Valkey cluster state machine, gossip protocol, and `clusterNode` struct. | **High**: Requires specialized routing proxy layer and asymmetric cluster topologies. |

---

## 4.5 Sharding Models Comparison Matrix

| Architectural Dimension | Inter-Shard Cross-Cluster Peering (3.1) | Multi-Primary Intra-Shard (3.2) | Asymmetric Slot Partitioning (3.3) |
|---|---|---|---|
| **Cluster Topology Scope** | Independent per DC (Separate Cluster Bus) | Single Global Stretched Cluster Bus | Shared Global or Hybrid Bus |
| **Client Write Routing** | Local Primary (`CRC16(key) mod 16384`) | Any Local Node (All Writable) | Local Primary or Proxy Forward |
| **Cross-DC Peer Binding** | Primary-to-Primary per Shard | Node-to-All-Nodes in Shard | Primary-to-Owner Primary |
| **Local Primary Failover Impact** | Local Sentinel/Cluster promotes replica $\to$ New Primary re-establishes WAN link | Ambiguous (No single primary exists) | Complex failover of remote ownership |
| **Slot Resharding Autonomy** | Symmetrical resharding required | Global cluster resharding across WAN | Regional independent resharding |
| **Engine Churn** | Minimal ($<5\%$ core changes) | Severe ($>40\%$ core changes) | Moderate ($15\%$ core changes) |

---

# 5. Domain 4: Replication Engine, Protocols & Stream Offsets

### 5.1 Technical Problem Statement: The Breakdown of Classical Replication & Offset Aliasing
Standard Valkey replication relies on two core variables in `server.h`:
- `char replid[41]`: A 40-character pseudo-random hexadecimal string identifying the current history run.
- `long long master_repl_offset`: A scalar monotonically increasing byte count of the replication stream.

```
       Standard Valkey (Linear Master Stream)
       +-------------------------------------------------------------+
       | Master Repl Stream: [ Offset 0 ................... Offset M ]|
       +-------------------------------------------------------------+
                                     |
                                     v
                       Replica: Consumes byte-for-byte
```

### 5.1.1 The Replication Offset Aliasing Anomaly (Silent Split-Brain Skipping)
In classical Valkey, replication streams are assumed to be strictly linear. When multiple concurrent primaries accept writes, **offset aliasing** creates a catastrophic silent data loss vulnerability:

```
       Replication Offset Aliasing Attack Scenario
       
       Common Ancestor: Offset = 50,000 (shared replid = "4a1b2c3d...")
       
       DC-East accepts 10,000 bytes of writes:       Local Offset_East = 60,000
       DC-West accepts 10,000 bytes of OTHER writes:  Local Offset_West = 60,000
       
       Replica / Peer / Proxy connects to DC-East -> streams up to offset 60,000.
       Link to DC-East drops; Replica fails over and reconnects to DC-West:
         -> Replica issues: PSYNC 4a1b2c3d... 60000
         -> DC-West inspects its local backlog offset: 60,000.
         -> DC-West evaluates: (60,000 == 60,000) -> Exact Match!
         -> DC-West responds: +CONTINUE with 0 bytes!
         
       FATAL FAILURE:
       The replica silently skips all 10,000 bytes of DC-West's unique writes!
       Neither node detects the gap; data silently diverges permanently across sites.
```

### 5.1.2 The Three Fundamental Invariants of Active-Active Replication:
1. **Replication ID Isolation**: Replication IDs must **never** be shared across concurrent writable primaries. Every writable node/shard must generate a globally unique, immutable 128-bit origin identifier (`shard_origin_id` / UUIDv4).
2. **Replication Offset Vectorization ($\vec{V}_{\text{repl}}$)**: Offset progress can only be tracked as an $N$-dimensional vector of origin-scoped offsets:
   $$\vec{V}_{\text{repl}} = \big\langle (\text{origin}_1, \text{offset}_1), (\text{origin}_2, \\text{offset}_2), \dots, (\text{origin}_N, \text{offset}_N) \big\rangle$$
3. **Non-Destructive Synchronization**: Replication engines must never invoke `emptyDb()` upon full resynchronization; incoming snapshots must merge into live dictionaries without clearing existing keys.

```
       Active-Active Multi-Stream Concurrency with Vector Offsets
       Origin A Stream: [ Offset_A 0 ........... Offset_A N ]  \
                                                                ===> PER-ORIGIN RING BUFFER MERGE
       Origin B Stream: [ Offset_B 0 ........... Offset_B M ]  /
```

---

## 5.2 Deep Dive: Replication Protocol & Wire Mechanics

### 5.2.1 Dual-Role Master/Replica Lifecycle & State Machine
Every Active-Active Valkey node must operate simultaneously as:
- **Master** to local clients and local read-replicas.
- **Replica** to remote datacenter peer primaries (ingesting, parsing, and merging remote CRDT streams).
- **Master** to remote datacenter peer primaries (serializing and streaming local mutations).

```
                      +-----------------------------+
                      |         INITIALIZING        |
                      +-----------------------------+
                                     |
                                     v
                      +-----------------------------+
                      |     PEER_CONNECTING         |
                      +-----------------------------+
                                     | (TLS Handshake & AUTH)
                                     v
                      +-----------------------------+
                      |      PEER_HANDSHAKE         | <--- Sends PSYNC_AA with V_repl
                      +-----------------------------+
                                     |
                 +-------------------+-------------------+
                 | (Gap in Vector)                       | (In-memory Backlog Match)
                 v                                       v
  +-----------------------------+         +-----------------------------+
  |    RECEIVING_MERGE_RDB      |         |      STREAMING_ONLINE       |
  |  (Non-destructive time-cut  |         |  (Bidirectional incremental |
  |   streaming CRDT merge)     |         |   CRDT frame processing)    |
  +-----------------------------+         +-----------------------------+
                 |                                       |
                 +-------------------+-------------------+
                                     |
                                     v
                      +-----------------------------+
                      |      STEADY_STATE_SYNC      |
                      +-----------------------------+
```

---

### 5.2.2 Wire Framing & Protocol Extension: The `METADATA` Command Wrapper
To maintain wire compatibility with RESP2/RESP3 while transporting CRDT metadata, replication streams use a `METADATA` prefix framing:

```
*<argc> \r\n
$8 \r\n METADATA \r\n
$<hlc_len> \r\n <hlc_timestamp> \r\n
$<origin_len> \r\n <origin_id> \r\n
$<vc_len> \r\n <vector_clock_bytes> \r\n
$<flags_len> \r\n <flags> \r\n
$<cmd_len> \r\n <command_name> \r\n
$<arg1_len> \r\n <arg1> \r\n
...
```

#### Protocol Fields:
- `hlc_timestamp`: 64-bit or 128-bit Hybrid Logical Clock value ($\langle l_{\text{physical}}, c_{\text{logical}} \rangle$).
- `origin_id`: 8-byte unique identifier of the node where the client write originally entered the system.
- `vector_clock_bytes`: Encoded vector $\vec{V} = \{ \text{DC}_1: c_1, \dots, \text{DC}_k: c_k \}$ for causal tracking.
- `flags`: Bitmask:
  - `0x01`: `CRDT_OP_DELTA` (Operation is a pure delta increment).
  - `0x02`: `CRDT_OP_TOMBSTONE` (Explicit deletion marker).
  - `0x04`: `CRDT_ECHO_SUPPRESS` (Do not forward beyond next hop).
  - `0x08`: `CRDT_OP_CONTAINER_EPOCH` (Container-level deletion epoch).

---

### 5.2.3 Multi-Stream Replication Offset Vector ($\vec{V}_{\text{repl}}$) & Isolated Backlogs
Instead of a single scalar `master_repl_offset`, each node maintains an **Offset Vector** and isolated per-origin circular ring buffers:

```c
typedef struct aaOriginBacklog {
    uint32_t origin_id;
    char *buf;                    /* Circular memory ring buffer */
    uint64_t size;                /* Allocated capacity (e.g. 64MB) */
    uint64_t head_offset;         /* Earliest byte offset retained */
    uint64_t tail_offset;         /* Latest byte offset written */
} aaOriginBacklog;

typedef struct aaReplicationEngine {
    uint32_t local_origin_id;
    dict *backlogs_by_origin;     /* Map<origin_id, aaOriginBacklog*> */
    uint64_t repl_vector[MAX_AA_NODES]; /* V_repl: last applied offset per origin */
} aaReplicationEngine;
```

- **Per-Origin Ring Invariant**: A write originating at Node $i$ is appended exclusively to `backlogs_by_origin[i]`. Offsets for Origin $i$ monotonically advance independent of writes occurring at Origin $j$.

---

### 5.2.4 Loop Prevention & Seen-Deduplication Mechanics
In topologies with multiple paths (e.g., Full Mesh, Ring, Gateway), a mutation could circulate indefinitely.
- **Mechanism 1: Origin Dropping**: When a node receives a frame with `origin_id == local_server_id`, it **immediately drops** the frame without applying or forwarding.
- **Mechanism 2: Hop Bitmask / Path Tracking**: The replication frame carries a 64-bit bitmask `seen_nodes_bitmask`. When node $k$ processes the frame, it checks if bit $k$ is set:
  - If set $\implies$ Drop immediately.
  - If not set $\implies$ Set bit $k$, process write, and forward only to peers whose bit is NOT set.
- **Mechanism 3: LRU Transaction ID De-duplication Filter**: A sliding-window Cuckoo Filter / LRU cache stores recent `(origin_id, hlc_timestamp)` hashes. Duplicate arrivals are discarded in $O(1)$ time.

---

### 5.2.5 `PSYNC_AA` Partial Resynchronization Protocol
When a cross-DC link reconnects after a transient drop:

1. **Handshake**:
   - Reconnecting node sends:
     `PSYNC_AA <local_node_id> <V_repl_vector_encoded>`
   - Vector contains the last acknowledged offset for each known origin:
     $$\vec{V}_{\text{local}} = \{ A: 105400, B: 92300, C: 44010 \}$$

2. **Delta Evaluation on Remote Peer**:
   - Remote peer compares $\vec{V}_{\text{local}}$ against its own backlog vector $\vec{V}_{\text{remote}}$:
     $$\Delta \vec{V} = \vec{V}_{\text{remote}} - \vec{V}_{\text{local}} = \{ A: +200, B: +50, C: +0 \}$$
   - **Case 1: Full Hit (`+CONTINUE_AA`)**: For all origins where $\Delta \text{offset} > 0$, the requested offset $\ge \text{head\_offset}$ in the respective origin ring buffer. The peer replies with:
     `+CONTINUE_AA <delta_stream_manifest>`
     and streams the interleaved delta byte buffers.
   - **Case 2: Backlog Miss (`+FULLRESYNC_AA`)**: If any requested origin offset has fallen behind `head_offset` (overwritten in the circular ring buffer), partial sync fails and the peer falls back to **Non-Destructive Full Resync**.

---

### 5.2.6 Non-Destructive Merge-on-Load Full Sync & Event Loop Yielding
To solve the `EMPTYDB` fatal flaw and prevent main-thread latency spikes during full resynchronization:

```
+-----------------------------------------------------------------------------------------------+
|                       NON-DESTRUCTIVE STREAMING MERGE-ON-LOAD ARCHITECTURE                    |
|                                                                                               |
|   Remote Master                              Local Active Node (Serving Client Traffic)       |
|   +-------------------+                      +--------------------------------------------+   |
|   | BGSAVE_AA Fork    |                      | Event Loop (ae.c)                          |   |
|   | Snapshot + Meta   |                      |  ├── Serves Client Commands (<1ms)         |   |
|   +-------------------+                      |  └── Time-Sliced Merge Iteration (<=2ms)   |   |
|             | (TLS Stream)                   +--------------------------------------------+   |
|             v                                                      ^                          |
|   +-------------------+                                            |                          |
|   | Non-Destructive   | ---> Iterative Key Deserializer ---------> Merge Into db->dict        |
|   | Streaming Parser  |      (Leaves existing local keys intact)   (Type-Specific CRDT Merge) |
|   +-------------------+                                                                       |
+-----------------------------------------------------------------------------------------------+
```

#### 1. Background Snapshot Generation (`BGSAVE_AA`)
- The remote master forks a child process to generate an active-active RDB snapshot containing:
  - All key-value payloads.
  - Attached CRDT metadata headers (per-key/per-field HLCs, vector clocks, tombstones).
  - The snapshot boundary replication vector $\vec{V}_{\text{snap}}$.

#### 2. Main-Thread Event Loop Starvation Mitigation (Asynchronous / Time-Sliced Merge)
- **Problem**: In a 50M key database (25 GB RAM), a synchronous blocking RDB load blocks the event loop for 15--45 seconds, triggering client timeouts and cluster failover storms.
- **Architectural Solutions**:
  - **Strategy A: Cooperative Time-Sliced Merge with Event Loop Yielding (In-Core Default)**:
    - The RDB streaming parser executes in chunks. It merges up to $K = 2,000$ keys or consumes $\le 2.0\text{ ms}$ of CPU time per tick.
    - It then yields to the `ae.c` event loop:
      ```c
      void rdbLoadMergeTimeSliced(connection *conn, uint64_t max_slice_us) {
          monotime timer = getMonotonicUs();
          while (rdbHasBytes(conn) && (getMonotonicUs() - timer) < max_slice_us) {
              rdbKeyValEntry entry = rdbParseNextEntry(conn);
              crdtMergeEntryIntoDict(server.db, &entry);
          }
          /* Yield to event loop to service pending client I/O and heartbeats */
          aeProcessEvents(server.el, AE_ALL_EVENTS | AE_DONT_WAIT);
      }
      ```
    - Keeps p99 client latency $< 5\text{ ms}$ and heartbeat liveness active throughout the full sync.
  - **Strategy B: Background Shadow-Dictionary Merge & Atomic Pointer Swap**:
    - A dedicated background worker thread parses the RDB stream into a temporary shadow database (`db->shadow_merge_dict`).
    - During construction, incoming live client writes are dual-applied to both `db->dict` and `db->shadow_merge_dict`.
    - Upon reaching $\vec{V}_{\text{snap}}$, a micro-pause lock ($<0.5\text{ ms}$) atomically swaps the root dictionary pointer.
       - If $K_{\text{incoming}}$ exists locally with metadata $M_{\text{local}}$:
         - Invoke type-specific merge function:
           $$\text{Value}_{\text{merged}}, M_{\text{merged}} = \text{MergeCRDT}(V_{\text{local}}, M_{\text{local}}, V_{\text{incoming}}, M_{\text{incoming}})$$
         - Update local dictionary in-place.
3. **Catch-up Streaming**: After the RDB merge completes, the remote node streams all incremental delta frames generated since $\vec{V}_{\text{snap}}$.

---

### 5.2.7 Causal Dependency Buffering & Delivery
Under asymmetric WAN routing, dependent commands may arrive out of order (e.g., `HSET profile:user_1 name "Alice"` arrives *after* `EXPIRE profile:user_1 3600`).
- **Mechanisms**:
  - **Option 4.7A: Loose Eventual Convergence (No Causal Buffering)**. Commands are applied immediately upon receipt using commutative rules (e.g., `EXPIRE` checks against key creation HLC). Out-of-order execution is tolerated by designing CRDT operations to be globally commutative.
  - **Option 4.7B: In-Memory Causal Dependency Wait-Queue**. Remote frames with vector clock $\vec{V}_{\text{frame}}$ are inspected:
    - If $\vec{V}_{\text{frame}} \le \vec{V}_{\text{local}} + \text{unit\_step} \implies$ Apply immediately.
    - If $\vec{V}_{\text{frame}}$ has unmet causal predecessors $\implies$ Park frame in `causal_buffer_queue` with a maximum hold timeout $\tau_{\text{hold}} = 500\text{ ms}$. If timeout expires, force apply with conflict resolution to prevent buffer starvation.

---

### 5.2.8 Backpressure, Flow Control & Buffer Exhaustion Handling
When a WAN link degrades or a remote peer processes frames slower than local write throughput:
- **Client Output Buffer Limit (`client-output-buffer-limit peer_aa`)**:
  - Sets soft and hard memory ceilings on peer replication output buffers.
- **Shedding & Degradation Hierarchy**:
  1. *Level 1: Compression & Coalescing*. Coalesce redundant in-flight updates to the same key (e.g., multiple `INCR` operations to counter $X$ are collapsed into a single $\Delta = +k$).
  2. *Level 2: Replication Throttling / Pause*. If peer buffer reaches $90\%$ of hard limit, throttle local client write rate (inject micro-sleeps in client event loop) to match sustainable WAN egress.
  3. *Level 3: Peer Disconnect & Fallback to Disk Spooling*. If buffer hits hard limit, sever the replication connection to protect instance stability, triggering Option 9.2 Full Merge on reconnect.

---

## 5.3 Comprehensive 4-Pillar Evaluation: Replication Protocols

| Evaluation Pillar | Option 4.1: Vectorized PSYNC (`PSYNC_AA`) | Option 4.2: Non-Destructive Merge RDB | Option 4.3: Causal Dependency Buffering | Option 4.4: Loose Commutative Execution |
|---|---|---|---|
| **1. Data Safety & Consistency** | **Highest**: Zero data loss, strictly causal byte stream playback within buffer window. | **Highest**: Recovers from unbounded partitions without wiping local concurrent writes. | **High**: Eliminates all causal inversion anomalies across independent keys. | **Medium**: Eventual consistency guaranteed; temporary causal anomalies possible. |
| **2. Overhead & Performance** | Minimal CPU (direct ring buffer copy). Memory: $N \times$ ring buffer size. | CPU spike during fork and RDB merge deserialization. Low steady-state memory. | RAM overhead for staging queues ($O(\text{unresolved frames})$); remote apply latency. | Zero RAM staging overhead; instantaneous frame execution upon arrival. |
| **3. Failure Modes & Edge Cases** | Buffer wrap-around during extended outages requires fallback to Full Merge RDB. | Copy-on-write memory spike if client write rate is high during RDB merge. | Buffer queue starvation/deadlock if a predecessor frame is dropped permanently. | Application logic must handle out-of-order execution anomalies. |
| **4. Complexity** | Moderate: Vector comparison logic $\Delta \vec{V}$ and multi-origin ring management. | High: Custom RDB parser hook in `rdb.c` that merges into existing dicts. | Very High: Dependency graph maintenance, timeout timers, and deadlock detection. | Minimal: In-line command execution using CRDT merge functions. |

---

# 6. Domain 5: Clocks, Ordering & Metadata Primitives

## 6.1 Technical Problem Statement
In an Active-Active distributed system, nodes generate mutations concurrently without a shared physical clock. Physical wall clocks (`CLOCK_REALTIME` via NTP) suffer from clock drift, non-monotonic jumps (leap seconds, NTP step corrections), and clock skew ($\Delta t_{\text{skew}} \approx 1\text{--}100\text{ ms}$).

Relying on raw physical timestamps for Last-Writer-Wins (LWW) leads to:
- **Causality Violations**: Write $B$ caused by Write $A$ receives a lower timestamp if Node $B$'s physical clock runs behind Node $A$'s clock.
- **Write Starvation & Clock Drag**: A node whose physical clock drifts into the future will have its writes permanently dominate all other nodes, dragging peer clocks forward into corrupted time.

```
       Physical Clock Drift Anomaly
       Node A (Clock: 10:00.005): SET k "A" (TS: 10:00.005) ----> Node B receives
       Node B (Clock: 09:59.950): SET k "B" (TS: 09:59.950) ----> Stale TS! Dropped!
       (Node B's newer causal write is permanently dropped due to -55ms drift!)
```

---

## 6.2 Deep Dive: Clock & Ordering Primitives

### Option 5.1: Adversarially Hardened 64-bit Hybrid Logical Clock (HLC-64)
Combines physical time and a logical counter into a single 64-bit integer, hardened against **16-bit logical counter overflow** and **forward clock contagion**:

```c
typedef union hlc64_t {
    uint64_t raw;
    struct {
        uint64_t logical:16;   /* Low 16 bits: logical counter (0 .. 65,535) */
        uint64_t physical:48;  /* High 48 bits: physical ms (up to 8,925 years) */
    } parts;
} hlc64_t;

typedef struct hlcState {
    hlc64_t current_hlc;
    uint64_t max_skew_ms;       /* Strict remote clock drift ceiling (default: 500ms) */
    uint64_t max_borrow_ms;     /* Max allowed forward physical borrow (default: 500ms) */
    uint64_t borrowed_ms;       /* Total ms borrowed due to logical counter saturation */
    uint64_t backpressure_stalls; /* Metric: client read event loop stalls */
} hlcState;
```

#### 1. Logical Counter Saturation, Clock Borrowing & Backpressure (`hlc_now`)
- **Vulnerability**: In burst workloads ($>65,536\text{ writes/ms}$), naïve `logical++` overflows 16 bits and wraps to 0, causing the HLC to jump backwards within the millisecond and newer writes to be discarded as stale.
- **Hardened Algorithm**:
  1. If `pt_ms > state.physical`, advance `state.physical = pt_ms`, reset `logical = 0`, and clear `borrowed_ms`.
  2. If `pt_ms <= state.physical`, increment `logical++`.
  3. If `logical == 0xFFFF` (saturation), do **NOT** wrap to 0. Artificially bump `state.physical++` (borrow 1ms from physical time), reset `logical = 0`, and increment `borrowed_ms++`.
  4. If `borrowed_ms > max_borrow_ms` ($500\text{ ms}$), trigger **client backpressure** by yielding the client event loop and deferring the command execution by 1ms to allow physical time to catch up.

```c
hlc64_t hlc_now(hlcState *state, uint64_t pt_ms, int *backpressure_required) {
    *backpressure_required = 0;
    if (pt_ms > state->current_hlc.parts.physical) {
        state->current_hlc.parts.physical = pt_ms;
        state->current_hlc.parts.logical = 0;
        state->borrowed_ms = 0;
    } else {
        if (state->current_hlc.parts.logical < 0xFFFF) {
            state->current_hlc.parts.logical++;
        } else {
            /* 16-bit Saturation: Borrow 1ms forward */
            state->current_hlc.parts.physical++;
            state->current_hlc.parts.logical = 0;
            state->borrowed_ms++;
            if (state->borrowed_ms > state->max_borrow_ms) {
                *backpressure_required = 1;
                state->backpressure_stalls++;
            }
        }
    }
    return state->current_hlc;
}
```

#### 2. Strict Drift Fencing & Forward Clock Drag Protection (`hlc_recv`)
- **Vulnerability (Future Clock Bomb)**: If Node $R$ suffers an NTP failure or malice and sets its clock to $T_{\text{future}} = T_{\text{real}} + 1\text{ hour}$, raw $\max(\text{local}, \text{remote})$ ratchets all healthy nodes into the future, permanently corrupting TTLs and rejecting writes for 1 hour.
- **Hardened Algorithm**:
  - In `hlc_recv()`, assert: $\text{remote\_hlc.physical} \le pt_{\text{ms}} + \Delta t_{\text{skew\_max}}$ ($500\text{ ms}$).
  - If breached: **Reject the frame**, do **NOT** advance local HLC, suspend the peer replication link, and emit critical alert `valkey_aa_clock_skew_violation`.

```c
int hlc_recv(hlcState *state, hlc64_t remote_hlc, uint64_t pt_ms, hlc64_t *out_hlc) {
    /* 1. Strict Clock Skew Fencing */
    if (remote_hlc.parts.physical > pt_ms + state->max_skew_ms) {
        /* Remote clock is running dangerously far in the future: Reject & Quarantine */
        return ERR_HLC_SKEW_EXCEEDED;
    }
    
    /* 2. Standard Monotonic Ratchet */
    uint64_t max_pt = pt_ms > state->current_hlc.parts.physical ? pt_ms : state->current_hlc.parts.physical;
    if (remote_hlc.parts.physical > max_pt) {
        max_pt = remote_hlc.parts.physical;
    }
    
    if (max_pt == state->current_hlc.parts.physical && max_pt == remote_hlc.parts.physical) {
        uint64_t max_log = state->current_hlc.parts.logical > remote_hlc.parts.logical ?
                           state->current_hlc.parts.logical : remote_hlc.parts.logical;
        state->current_hlc.parts.logical = max_log + 1;
    } else if (max_pt == state->current_hlc.parts.physical) {
        state->current_hlc.parts.logical++;
    } else if (max_pt == remote_hlc.parts.physical) {
        state->current_hlc.parts.logical = remote_hlc.parts.logical + 1;
    } else {
        state->current_hlc.parts.logical = 0;
    }
    state->current_hlc.parts.physical = max_pt;
    *out_hlc = state->current_hlc;
    return C_OK;
}
```

---

### Option 5.2: 128-bit Extended Hybrid Logical Clock (HLC-128)
- **Mechanics**: Nanosecond physical precision and globally unique tie-breaking:
  - 64 bits: Nanosecond physical time.
  - 32 bits: Logical sequence counter.
  - 32 bits: Origin Node Identifier (`origin_id`).
```c
typedef struct hlc128_t {
    uint64_t physical_ns;
    uint32_t logical;
    uint32_t origin_id;
} hlc128_t;
```

### Option 5.3: Vector Clocks (Full Causal Tracking)
- **Mechanics**: Explicit causal vector $\vec{V} = [c_1, \dots, c_N]$. Identifies true concurrency ($\vec{V}_A \parallel \vec{V}_B$), at the expense of $O(N)$ memory and CPU per element.

### Option 5.4: Lamport Timestamps with Lease Clocks
- **Mechanics**: Scalar logical counters coupled with authoritative lease boundaries.

---

## 6.3 In-Memory Metadata Storage & Compact Encoding Preservation Architecture

### 6.3.1 The Memory De-Optimization Cliff (Intset & Listpack Destruction)
In standard Valkey, small collections use compact contiguous flat buffers:
- **Small Sets ($\le 512$ integers)**: `OBJ_ENCODING_INTSET` (2, 4, or 8 bytes per integer, zero pointer overhead).
- **Small Hashes & ZSets ($\le 128$ entries)**: `OBJ_ENCODING_LISTPACK` (contiguous memory buffer, zero pointer overhead).

```
+-----------------------------------------------------------------------------------------------+
|                       THE INTSET / LISTPACK MEMORY CLIFF REALITY                              |
|                                                                                               |
|   Standard Valkey Intset (100 integers):                                                      |
|   [ Header: 8B ][ 100 x 8-byte ints ] = 808 Bytes Total (8.08 Bytes/element)                  |
|                                                                                               |
|   Naïve CRDT Degradation (Forced Dict Conversion):                                            |
|   [ dict + dictEntry (24B) + robj (16B) + jemalloc bucket (32B) + CRDT tag (24B) ]            |
|   = ~96 Bytes per element (9,600 Bytes Total)                                                 |
|   ===> 12x Memory Amplification Factor (1,200% RAM Overhead Explosion!)                       |
+-----------------------------------------------------------------------------------------------+
```

### 6.3.2 Mitigation: Metadata-Aware Compact Encodings (`crdt_intset` & `crdt_listpack`)
To prevent catastrophic memory degradation, Active-Active Valkey implements compact CRDT encodings:

```
Layout A: Compact crdt_intset
+-----------------------------------------------------------------------------------------------+
| base_hlc: 8B | origin_id: 2B | count: 2B | int_array[]: 8B x N | delta_metadata_table (opt)   |
+-----------------------------------------------------------------------------------------------+

Layout B: Compact crdt_listpack Entry
+-----------------------------------------------------------------------------------------------+
| lp_entry_len | flags: 1B | varint_hlc_delta (1-4B) | varint_origin (1-2B) | raw_field_data... |
+-----------------------------------------------------------------------------------------------+
```

1. **`crdt_intset`**:
   - Stores a single **Container Base HLC** (`base_hlc`) in the intset header.
   - For all elements added at container creation or updated concurrently with the same timestamp, per-element metadata overhead is **0 bytes**.
   - If individual elements are updated asynchronously, an auxiliary compact delta table (varint delta offsets) is appended at the tail of the intset buffer.
2. **`crdt_listpack`**:
   - Encodes HLC timestamps as **LEB128 variable-length deltas** relative to `base_hlc` (consuming only 1--3 bytes per entry instead of 8--16 bytes).
   - Prevents premature listpack size threshold breaches and preserves compact memory density.

---

### 6.3.3 Exact Memory Footprint Breakdown Matrix

| Data Structure & Encoding | Standard Valkey Memory | Naïve CRDT Memory (Forced Dict) | Compact CRDT Memory (`crdt_*`) | Active-Active Overhead |
|---|---|---|---|---|
| **String (`SET key val`, 64B val)** | $24\text{B} + 16\text{B} + 64\text{B} = 104\text{B}$ | $144\text{B}$ (Shadow dict) | $112\text{B}$ (Intrusive 8B HLC) | $+7.6\%$ |
| **Small Intset (500 integers)** | $8\text{B} + (500 \times 8\text{B}) = 4,008\text{B}$ | $500 \times 96\text{B} = 48,000\text{B}$ | $12\text{B} + (500 \times 8\text{B}) = 4,012\text{B}$ | **$+0.1\%$** (Avoids $12\times$ cliff!) |
| **Small Hash (50 fields, Listpack)**| $6\text{B} + 50 \times (10\text{B} + 20\text{B}) = 1,506\text{B}$| $50 \times 96\text{B} = 4,800\text{B}$ | $14\text{B} + 50 \times (30\text{B} + 3\text{B}) = 1,664\text{B}$ | **$+10.5\%$** (Preserves listpack) |
| **Large Hash (10,000 fields, Dict)**| $10,000 \times 48\text{B} = 480\text{ KB}$ | $10,000 \times 96\text{B} = 960\text{ KB}$ | $10,000 \times 56\text{B} = 560\text{ KB}$ | $+16.6\%$ |

---

## 6.4 Comprehensive 4-Pillar Evaluation: Clocks & Metadata

| Evaluation Pillar | Option 5.1 (Hardened HLC-64) | Option 5.2 (HLC-128 Extended) | Option 5.3 (Vector Clocks) | Option 5.4 (Lamport Timestamps) |
|---|---|---|---|---|
| **1. Data Safety & Consistency** | **Highest**: Clock-borrowing prevents counter wrap; drift clamp prevents future clock contagion. | Maximal: Nanosecond resolution + unique NodeID tie-breaker eliminates collisions. | Maximal: Detects true concurrency ($\vec{V}_A \parallel \vec{V}_B$); zero false-causality orderings. | Medium: Enforces total causal order, but logical time diverges completely from real-world time. |
| **2. Overhead & Performance** | **Optimal**: 8 bytes per key/field. Arithmetic comparison in a single CPU instruction (`CMP`). | Medium: 16 bytes per key/field. Requires 128-bit comparison (two 64-bit integer ops). | **Worst**: $O(N)$ bytes per element ($8\text{ B} \times N_{\text{DCs}}$). Comparison takes $O(N)$ CPU steps. | **Optimal**: 8 bytes (counter + node ID). Fast integer comparison. |
| **3. Failure Modes & Edge Cases** | High throughput bursts handled via clock borrowing; extreme skew safely quarantined. | Clock drift mitigation same as HLC-64; larger memory footprint across billions of sub-elements. | Vector size explosion if node membership changes dynamically; complex vector compaction. | Extreme skew from real time causes TTL/expiration logic to decouple from wall-clock time. |
| **4. Complexity** | **Lowest**: Compact integer fits in word registers; trivial wire framing. | Low: Simple 16-byte struct serialization. | **Extreme**: Dynamic topology tracking, pruning stale entries, causal dependency queues. | Low: Simple logical counter increment on read/write. |

---

## 6.5 Master Comparison Matrix: Clock Primitives

| Metric / Dimension | Hardened HLC-64 (5.1) | HLC-128 (5.2) | Vector Clocks (5.3) | Lamport Clocks (5.4) |
|---|---|---|---|---|
| **Memory Footprint per Key** | 8 Bytes | 16 Bytes | $8 \times N_{\text{DCs}}$ Bytes | 8 Bytes |
| **Memory Footprint (100M Keys)** | $800\text{ MB}$ | $1.6\text{ GB}$ | $4.0\text{ GB}$ ($N=5$) | $800\text{ MB}$ |
| **CPU Compare Complexity** | $O(1)$ (1 instruction) | $O(1)$ (2 instructions) | $O(N)$ vector traversal | $O(1)$ (1 instruction) |
| **Counter Saturation Protection** | **Yes** (Clock Borrow + Backpressure) | Yes (32-bit counter) | N/A | N/A |
| **Future Clock Contagion Fencing** | **Yes** (`MAX_CLOCK_SKEW_MS` clamp) | Yes (`MAX_CLOCK_SKEW_MS`) | N/A | N/A |
| **Compact Encodings (`intset/lp`)** | **Full Support** | Partial (Buffer bloat) | No (Requires Dict) | Full Support |
| **Concurrency Detection** | No (LWW collapse) | No (LWW collapse) | **Yes** (Identifies $\parallel$) | No (Total order forced) |
| **Physical Time Correlation** | Millisecond ($\pm \text{drift}$) | Nanosecond ($\pm \text{drift}$) | None | None |
| **Wire Protocol Overhead** | 8 Bytes | 16 Bytes | $8 \times N$ Bytes | 8 Bytes |

---

# 7. Domain 6: Conflict Resolution Mechanisms & CRDT Models

## 7.1 Mathematical Foundations of Strong Eventual Consistency (SEC)
An Active-Active replication engine achieves Strong Eventual Consistency (SEC) if all nodes that have received the same set of updates reach mathematically identical states, irrespective of the order of delivery.

### Formal Semilattice Invariants
State-based CRDTs (CvRDT) are defined as a join-semilattice $\langle S, \sqcup \rangle$:
1. **Commutativity**: $x \sqcup y = y \sqcup x$ (arrival order invariance).
2. **Associativity**: $(x \sqcup y) \sqcup z = x \sqcup (y \sqcup z)$ (message batching invariance).
3. **Idempotence**: $x \sqcup x = x$ (duplicate message delivery invariance).

Operation-based CRDTs (CmRDT) define operations $o_1, o_2$ such that for any concurrent operations $o_1 \parallel o_2$:
$$s \cdot o_1 \cdot o_2 = s \cdot o_2 \cdot o_1 \quad (\text{Commutative Operation Execution})$$

---

## 7.2 The IEEE 754 Floating-Point Non-Associativity Invalidation & Resolution

### 7.2.1 Mathematical Proof of SEC Breakdown in Naïve Float PN-Counters
IEEE 754 double-precision floating-point addition is **not associative**:
$$(a + b) + c \neq a + (b + c)$$

#### Concrete Attack Scenario:
Consider initial value $V = 0.0$ and three concurrent increments from three datacenters:
- $\Delta_1 = +1.0 \times 10^{16}$
- $\Delta_2 = -1.0 \times 10^{16}$
- $\Delta_3 = +3.141592653589793$

Trace execution order at two replicas:
- **Node A receives $\Delta_1$, then $\Delta_2$, then $\Delta_3$**:
  $$V_A = ((0.0 + 10^{16}) - 10^{16}) + 3.141592653589793 = 0.0 + 3.141592653589793 = \mathbf{3.141592653589793}$$
- **Node B receives $\Delta_1$, then $\Delta_3$, then $\Delta_2$**:
  $$V_B = (0.0 + 10^{16}) + 3.141592653589793 = 10000000000000003.0 \text{ (precision lost due to 53-bit mantissa!)}$$
  $$V_B = 10000000000000003.0 - 10^{16} = \mathbf{3.0} \neq 3.141592653589793$$

**Fatal Failure**: Both nodes processed the exact same set of operations, but reached **divergent in-memory floating point states** ($3.141592653589793 \neq 3.0$), permanently violating SEC.

---

### 7.2.2 The Two Architectural Solutions for Floating-Point Operations:

#### Option 6.5A: Scaled 128-bit Fixed-Point / Arbitrary-Precision Decimal Vector (Recommended)
- **Mechanics**: Floating point values are converted to 128-bit signed scaled integers with fixed scale $10^{-18}$ (or canonical arbitrary-precision decimal strings).
  $$\text{IntVal} = \text{round}(\text{FloatVal} \times 10^{18})$$
- Integer addition is **strictly associative and commutative** in $\mathbb{Z}$:
  $$(a + b) + c = a + (b + c)$$
- All replicas converge to the exact same bit-for-bit integer value; formatting back to float occurs strictly during client RESP serialization.

#### Option 6.5B: Explicit Demotion to HLC-LWW Register Semantics
- **Mechanics**: `INCRBYFLOAT` is demoted from a commutative PN-counter to an HLC-LWW Register.
- Local node calculates absolute result $V_{\text{new}} = V_{\text{curr}} + \Delta$ and assigns an HLC timestamp.
- Cross-DC replication propagates $V_{\text{new}}$ as an absolute LWW update.
- *Trade-off*: Loses commutativity under concurrent increments (one increment wins, the other is overwritten), but guarantees mathematical SEC convergence.

---

## 7.3 Deep Dive: Four Conflict Resolution Models

```
   Option 6.1: LWW-CRDT                    Option 6.2: Operation-based CmRDT
   +---------------------------+           +---------------------------------------+
   | Incoming: HLC=105, Val="B"|           | Local Op: INCRBY counter 5            |
   | Local:    HLC=102, Val="A"|           | Remote Broadcast: Delta = +5          |
   | Resolution: "B" Overwrites|           | Remote Apply: counter = counter + 5   |
   +---------------------------+           +---------------------------------------+
   
   Option 6.3: Commutative CmRDT           Option 6.4: Delta State-based CRDT
   +---------------------------+           +---------------------------------------+
   | Op: SADD set "item_1"     |           | Local Mutation: Mutates state lattice |
   | Tag: <HLC, NodeID>        |           | Extract Minimal Delta Lattice: dS     |
   | Merge: Union of Tag Sets  |           | Remote Merge: S_remote = S_remote u dS|
   +---------------------------+           +---------------------------------------+
```

### Option 6.1: Last-Writer-Wins (LWW) with Deterministic Node-ID Tie-Breaking
- **Algorithm**:
  $$\text{Value}_{\text{final}} = \begin{cases} V_{\text{incoming}} & \text{if } \text{HLC}_{\text{incoming}} > \text{HLC}_{\text{local}} \\ V_{\text{local}} & \text{if } \text{HLC}_{\text{incoming}} < \text{HLC}_{\text{local}} \\ V_{\text{incoming}} & \text{if } \text{HLC}_{\text{incoming}} == \text{HLC}_{\text{local}} \land \text{NodeID}_{\text{incoming}} > \text{NodeID}_{\text{local}} \\ V_{\text{local}} & \text{otherwise} \end{cases}$$
- **C Implementation**:
```c
int crdtResolveLWW(uint64_t local_hlc, uint32_t local_node, 
                   uint64_t remote_hlc, uint32_t remote_node) {
    if (remote_hlc > local_hlc) return 1; /* Remote wins */
    if (remote_hlc < local_hlc) return 0; /* Local wins */
    return (remote_node > local_node) ? 1 : 0; /* Deterministic tie-break */
}
```

### Option 6.2: Operation-Based CRDTs with Reliable Causal Broadcast
- **Mechanics**: Separated into two execution phases:
  1. *Prepare Phase (Local Node)*: Client command executed against local state; generates side-effect-free message $\Delta_{\text{op}}$.
  2. *Effect Phase (Local + Remote Nodes)*: $\Delta_{\text{op}}$ broadcast via Reliable Causal Broadcast and applied commutatively on all replicas.

### Option 6.3: Pure Commutative CRDTs with Delete Re-Propagation
- **Mechanics**: Operations designed to commute without requiring causal delivery order. Deletions propagate tombstones with unique invocation tags; late-arriving additions verify tag membership against tombstone sets.

### Option 6.4: Delta State-Based CRDTs ($\delta$-CRDTs)
- **Mechanics**: Instead of transmitting the entire state lattice $S$ (CvRDT) or raw operations (CmRDT), the node computes a minimal delta state $\delta S$ representing recent mutations:
  $$S_{\text{new}} = S_{\text{old}} \sqcup \delta S$$

---

## 7.4 Comprehensive 4-Pillar Evaluation: Conflict Resolution Models

| Evaluation Pillar | Option 6.1 (LWW Deterministic) | Option 6.2 (Op-Based CmRDT) | Option 6.3 (Pure Commutative) | Option 6.4 (Delta State $\delta$-CRDT) |
|---|---|---|---|---|
| **1. Data Safety & Consistency** | **SEC Guaranteed**; LWW lossy: concurrent write to same register silently discarded. | **SEC Guaranteed**; zero update loss; preserves all concurrent element additions. | **SEC Guaranteed**; out-of-order execution safe; robust against packet drops. | **SEC Guaranteed**; mathematically idempotent, associative, and commutative. |
| **2. Overhead & Performance** | **Optimal**: 8 bytes metadata. Zero merge CPU overhead ($O(1)$ integer compare). | Medium: Memory for element tag sets ($+24\text{ B}$/element). Low network delta bytes. | Medium-High: Tombstone tags retained in memory until GC horizon. | High Bandwidth: Delta state serialization overhead; high CPU lattice join. |
| **3. Failure Modes & Edge Cases** | Clock skew causes newer writes to lose to older writes; tie-breaker favors higher NodeID. | Causal delivery queue buffer bloat if causal predecessor packet is lost. | Tombstone accumulation under continuous churn requires aggressive GC. | Complex delta garbage collection and lattice inflation under deep DAGs. |
| **4. Complexity** | **Lowest**: Minimal engine churn ($<5\%$); fits directly into Valkey data paths. | High: Requires Reliable Causal Broadcast transport layer. | Moderate-to-High: Requires custom sub-element metadata structures. | Very High: Custom join-semilattice merge algorithms for every data type. |

---

# 8. Domain 7: Data Structure Command Matrix & Resolution Semantics

## 8.1 Technical Problem Statement
Valkey supports rich abstract data types: Strings, Hashes, Sets, Sorted Sets, Lists, Streams, and Bitmaps. Each type requires specific CRDT mathematics to resolve concurrent mutations across regions.

---

## 8.2 Deep Dive: Resolution Mechanics per Data Structure

### 8.2.1 Strings & Bitmaps: Register LWW vs Block-wise RGA
- **`SET key val`**: HLC-LWW register. Higher HLC overwrites lower HLC.
- **`APPEND key val` / `SETRANGE key offset val`**:
  - *Option A (LWW Register)*: Entire string resolved via HLC-LWW. Concurrent `APPEND` in DC-1 and DC-2 results in one append completely overwriting the other.
  - *Option B (Block-wise Replicated Growable Array - BwRGA)*: String modeled as a sequence of text blocks. Appends insert new blocks with `(origin_id, hlc)` sequence tags, preserving concurrent text insertions.
- **`SETBIT key offset val`**: Bit-level LWW. Each byte/word maintains an HLC timestamp, or bit mutations convert to field-level deltas.

### 8.2.2 Numeric Counters: Integer PN-Counters vs Scaled Fixed-Point Floats
- **`INCR key` / `INCRBY key val` / `DECRBY key val`**:
  - Modeled as an integer PN-Counter with state vectors $\vec{P} = [p_1, \dots, p_N]$ and $\vec{N} = [n_1, \dots, n_N]$.
  - Delta updates $\Delta = \pm k$ sum commutatively across all regions.
- **`INCRBYFLOAT key val` / `HINCRBYFLOAT key field val`**:
  - Modeled using **Option 6.5A (128-bit Scaled Fixed-Point Decimal Vector)** with scale $10^{-18}$. Prevents IEEE 754 non-associative divergence.
  - Replicas maintain integer state vectors $\vec{P}_{\text{fp}}, \vec{N}_{\text{fp}}$ where $1.0 = 10^{18}$. Addition is strictly associative in $\mathbb{Z}$.

### 8.2.3 Hashes: Field-Level LWW, Field Tombstones & Hierarchical Deletion Semantics
- **`HSET key field val`**: Each field in the hash table carries an independent HLC timestamp:
```c
typedef struct crdtHashEntry {
    sds field;
    sds value;
    uint64_t field_hlc;
    uint32_t origin_id;
} crdtHashEntry;
```
- **`HDEL key field`**: Replaces the value with a **Field Tombstone** containing `delete_hlc`.
- **`HINCRBY key field val`**: Field-level PN-counter.
- **Formal Hierarchical Deletion Invariant (Key `DEL` vs Concurrent Field `HSET`)**:
  - Suppose DC-1 executes `DEL user:100` at $\text{HLC} = 100$ (creating Key Tombstone $T_{\text{key}}$).
  - Concurrently, DC-2 executes `HSET user:100 email "a@b.com"` at $\text{HLC} = 95$, and `HSET user:100 name "Alice"` at $\text{HLC} = 105$.
  - **The Hierarchical Survival Rule**:
    $$\text{Field } f \text{ survives} \iff \text{HLC}(f) > \max\Big( \text{HLC}_{\text{key\_tombstone}}, \text{HLC}_{\text{field\_tombstone}}(f) \Big)$$
  - **Result**: Field `email` ($95 < 100$) is purged; Field `name` ($105 > 100$) **survives**. The root hash container is reified containing exclusively `name = "Alice"`. If no fields survive, the entire container is collapsed into a root tombstone.

### 8.2.4 Sets: Observed-Removed (OR-Set / Add-Wins Set)
- **`SADD key member`**: Generates a globally unique tag $t = \langle \text{HLC}, \text{NodeID}, \text{Seq} \rangle$. Member is added to set: $\text{Element}(m) \leftarrow \text{Tags}(m) \cup \{ t \}$.
- **`SREM key member`**: Fetches all *currently observed* tags for $m$: $\text{ObservedTags}(m) = \{ t_1, \dots, t_k \}$. Emits deletion delta `CRDT.SREM key member ObservedTags`.
- **Merge Semantics**:
  $$\text{Tags}_{\text{final}}(m) = (\text{Tags}_{\text{local}}(m) \cup \text{Tags}_{\text{remote}}(m)) \setminus (\text{RemovedTags}_{\text{local}} \cup \text{RemovedTags}_{\text{remote}})$$
  - *Add-Wins Behavior*: If DC-1 adds $m$ (tag $t_2$) concurrently while DC-2 deletes $m$ (observing tag $t_1$), tag $t_2$ survives the deletion.

### 8.2.5 Sorted Sets (ZSets): Add-Wins Elements with Max/LWW Scores
- **`ZADD key score member`**: Combines OR-Set membership with LWW score registers.
  - Member presence governed by Add-Wins OR-Set tags.
  - Score resolved via:
    - *Policy 1 (HLC-LWW Score)*: Higher HLC score update wins.
    - *Policy 2 (Max Score Wins)*: Score = $\max(\text{Score}_{\text{local}}, \text{Score}_{\text{remote}})$.
- **`ZINCRBY key delta member`**: Score increment modeled as commutative PN-counter.

### 8.2.6 Lists: Replicated Growable Array (RGA CRDT)
- **Problem**: Standard Valkey lists rely on integer index positions (`LINDEX 0`, `LINSERT`). Concurrent inserts at index 0 in two DCs cause element displacement and interleaving corruption.
- **RGA Mechanics**:
  - Each list element is a vertex $v = \langle \text{id}, \text{val}, \text{deleted\_flag} \rangle$ where $\text{id} = \langle \text{HLC}, \text{NodeID} \rangle$.
  - Vertices form a linked list. Insert operation specifies predecessor vertex ID: $\text{insertAfter}(v_{\text{prev}}, v_{\text{new}})$.
  - Concurrent inserts after the same predecessor are ordered deterministically by $\text{id}$ ($\text{HLC}_{\text{new}}$ descending).
  - Deletions set $\text{deleted\_flag} = \text{true}$ (tombstone) to preserve position references for concurrent inserts.

### 8.2.7 Streams: Region-Prefixed Disjoint IDs & Add-Wins PEL
- **`XADD key id field val ...`**:
  - Stream IDs structured as: `<Physical_HLC_ms>-<Origin_Node_ID>-<Local_Sequence>`.
  - Eliminates stream ID collisions across regions.
  - Entries inserted into the stream radix tree ordered by composite ID.
- **`XACK key group id`**:
  - Consumer group Pending Entries List (PEL) entry modeled as an Add-Wins OR-Set. Concurrent ack in DC-1 and read in DC-2 converges to acknowledged state once ack propagates.

### 8.2.8 Non-Monotonic Conditional Predicates (`SETNX`): AP Impossibility & Dual Modes

#### 1. Formal Proof of AP Impossibility for Mutual Exclusion
- In a distributed system, mutual exclusion (ensuring at most one client holds exclusivity over a resource) requires **linearizable consensus (CP)**.
- Under an AP partition where both DC-1 and DC-2 accept writes:
  - Client 1 in DC-1 executes `SET lock:order_99 "client1" NX EX 30` $\to$ Returns `(integer) 1` (Lock Acquired).
  - Client 2 in DC-2 executes `SET lock:order_99 "client2" NX EX 30` $\to$ Returns `(integer) 1` (Lock Acquired).
  - **Catastrophic Violation**: Both clients enter the critical section simultaneously, causing double-spending or inventory corruption. When cross-DC replication arrives, LWW resolves the key to Client 2, silently evaporating Client 1's lock.

#### 2. The Two Architectural Operating Modes for `NX` Predicates:
1. **Mode A: Authoritative Shard Lease / CP Sequencer Routing (Safe Default for Distributed Locks)**:
   - Keys using `NX` predicates are routed to an authoritative single-primary region via synchronous lease consensus ($30\text{--}100\text{ ms}$ WAN latency). Guarantees true mutual exclusion.
2. **Mode B: Documented Local-Only AP Exclusivity (`aa-unsafe-nx-predicates enable`)**:
   - `NX` is evaluated purely against the local node keyspace.
   - Valkey emits an explicit startup warning and configuration requirement:
     `WARNING: aa-unsafe-nx-predicates is enabled. NX commands guarantee LOCAL-REGION exclusivity only and MUST NOT be used for multi-region distributed locking.`

---

## 8.3 Master Command-by-Command CRDT Resolution Matrix

| Command Family | Supported Commands | CRDT Mathematical Model | Conflict Resolution Policy | Tombstone Retained? |
|---|---|---|---|---|
| **Strings** | `SET`, `SETEX`, `PSETEX`, `MSET` | HLC-LWW Register | Higher HLC wins; NodeID tie-breaker | Yes (Key Tombstone) |
| **Strings (Text)** | `APPEND`, `SETRANGE` | Block-wise RGA / LWW | Block insertion by HLC sequence | Yes (Vertex Tombstone) |
| **Numbers (Int)** | `INCR`, `DECR`, `INCRBY` | Integer PN-Counter | Commutative Delta Sum ($\sum P - \sum N$) | No |
| **Numbers (Float)**| `INCRBYFLOAT`, `HINCRBYFLOAT` | 128-bit Scaled Fixed-Point / LWW | Exact fixed-point delta sum or HLC-LWW | No / Yes |
| **Bitmaps** | `SETBIT`, `BITOP` | Bit-level LWW / Word HLC | Higher HLC per bit/word | No |
| **Hashes** | `HSET`, `HMSET` | Field-Level LWW Register | Field HLC comparison; NodeID tie-break | Yes (Field Tombstone) |
| **Hashes (Counters)**| `HINCRBY` | Field-Level PN-Counter | Commutative field delta sum | No |
| **Sets** | `SADD`, `SREM`, `SMOVE` | Add-Wins Observed-Removed Set | Unique Tag Set Union $\setminus$ Removed Tags | Yes (Tag Tombstone) |
| **Sorted Sets** | `ZADD`, `ZREM`, `ZINCRBY` | Add-Wins Set + LWW/Max Score | Add-Wins membership; $\max(\text{score})$ or LWW | Yes (Element Tombstone) |
| **Lists** | `LPUSH`, `RPUSH`, `LINSERT`, `LSET`, `LPOP` | Replicated Growable Array (RGA) | Predecessor ID + HLC deterministic insert | Yes (Node Tombstones) |
| **Streams** | `XADD`, `XACK`, `XDEL`, `XCLAIM` | Disjoint HLC Radix + Add-Wins PEL | Composite ID ordering; Add-Wins PEL ack | Yes (Stream Tombstones) |
| **Keyspace** | `DEL`, `UNLINK`, `EXPIRE`, `EXPIREAT` | Hierarchical LWW Tombstone | $\text{HLC}(f) > \max(T_{\text{key}}, T_f)$; $\max(\text{expire})$ | Yes (Keyspace Tombstone) |
| **Non-Monotonic** | `SETNX`, `HSETNX`, `MULTI/EXEC` | Authoritative CP Lease OR Local AP | CP Sequencer Lease or Local Speculative | Yes |

---

# 9. Domain 8: Deletions, Tombstones & Anti-Entropy Garbage Collection

## 9.1 Technical Problem Statement: The Deletion Dilemma
In single-master replication, `DEL key` frees memory immediately. In Active-Active systems, if Node A deletes key $K$ and frees its memory immediately, a concurrent or in-flight update to $K$ from Node B arriving at Node A appears as a **new key creation**, resurrecting the deleted key!

```
       The Zombie Key Resurrection Anomaly
       Node A: DEL key:123 (Frees memory immediately)
       Node B: SET key:123 "old_val" (In-flight replication packet)
       Node A receives packet from Node B:
         -> Checks db->dict: key:123 not found!
         -> Inserts key:123 = "old_val" (ZOMBIE RESURRECTION!)
```

To prevent resurrection, deletions must create a **Tombstone**—a persistent record that key $K$ was deleted at timestamp $\text{HLC}_{\text{del}}$. However, if tombstones are never freed, memory grows monotonically until Out-Of-Memory (OOM) occurs.

---

## 9.2 Deep Dive: Tombstone Garbage Collection Algorithms

```
+-------------------------------------------------------------------------------------------------------+
|                       TOMBSTONE LIFECYCLE & STABILITY HORIZON GC                                      |
|                                                                                                       |
|   [Client DEL] ---> [Active Tombstone Created] ---> [Replicated to All Nodes]                         |
|                                                               |                                       |
|                                                               v                                       |
|   [Safe Memory Free / GC] <--- [Tombstone HLC <= Global Stability Horizon H_stable^Q]                  |
|                                                               ^                                       |
|                                                               | (Node Timeout > tau_eject)            |
|   [Dead-Node Ejection: M_active = M \ {k}] -------------------+                                       |
|                                                                                                       |
|   [Reconnecting Node k] ---> [REINTEGRATION_FENCED] ---> [AAE_MERKLE / FULLRESYNC] ---> [Rejoin]     |
+-------------------------------------------------------------------------------------------------------+
```

### Option 8.1: Dynamic Quorum Stability Horizon ($H_{\text{stable}}^Q$) & Dead Node Lease Eviction ($\tau_{\text{eject}}$)

#### 1. The Straggler Deadlock & OOM Trap
In a naive global minimum stability horizon:
$$H_{\text{stable}} = \min_{j=1}^N \Big( \text{last\_acked\_hlc}(j) \Big)$$
If a single node $k$ suffers a catastrophic hardware failure, permanent network partition, or is decommissioned without administrative unregistration, its `last_acked_hlc(k)` stops advancing. Consequently:
1. $H_{\text{stable}}$ freezes globally at the timestamp of node $k$'s failure.
2. All subsequent deletions across the entire cluster generate tombstones that can **never be collected** ($\text{HLC}(T) > H_{\text{stable}}$ forever).
3. Tombstone memory grows monotonically, eventually exhausting server RAM and causing cluster-wide Out-Of-Memory (OOM) fatal crashes.

#### 2. Dynamic Quorum Stability Horizon Calculation
To prevent straggler deadlocks, the stability horizon is calculated over an active dynamic quorum:
$$H_{\text{stable}}^Q = \operatorname{quantile}_Q \Big( \big\{ \text{last\_acked\_hlc}(j) \mid j \in M_{\text{active}} \big\} \Big)$$
Where $M_{\text{active}} \subseteq \{1, \dots, N\}$ is the set of currently active cluster nodes, and $Q$ is the quorum quantile threshold (default: $Q = \min(M_{\text{active}})$, representing all active members in $M_{\text{active}}$).

```c
/* Dynamic Stability Horizon Computation */
uint64_t compute_stability_horizon(cluster_aa_state_t *cluster) {
    uint64_t active_hlcs[MAX_NODES];
    size_t active_count = 0;
    uint64_t now_ms = mstime();

    for (size_t i = 0; i < cluster->total_nodes; i++) {
        node_aa_state_t *node = &cluster->nodes[i];
        /* Skip administratively decommissioned or heartbeat-ejected nodes */
        if (!node->is_active || (now_ms - node->last_heartbeat_ms > cluster->tau_eject_ms)) {
            continue;
        }
        active_hlcs[active_count++] = node->last_acked_hlc;
    }

    if (active_count == 0) return 0;

    /* Sort ascending to find minimum across active membership */
    qsort(active_hlcs, active_count, sizeof(uint64_t), compare_uint64);
    return active_hlcs[0]; /* Minimum across active quorum */
}
```

#### 3. Heartbeat Leases & Straggler Eviction Protocol ($\tau_{\text{eject}}$)
Each node maintains a rolling heartbeat lease $\tau_{\text{eject}}$ (configurable, default: `3600000` ms / 1 hour):
1. **Heartbeat Tracking**: Every replication ping/ack frame updates `node->last_heartbeat_ms`.
2. **Lease Expiration & Ejection**: If node $k$ fails to acknowledge replication traffic for $t > \tau_{\text{eject}}$:
   - Node $k$ is marked `EJECTED_FROM_GC_QUORUM`:
     $$M_{\text{active}} \leftarrow M_{\text{active}} \setminus \{k\}$$
   - $H_{\text{stable}}^Q$ is dynamically recalculated over $M_{\text{active}}$, instantly unfreezing tombstone garbage collection across surviving nodes.
   - The server increments the diagnostic counter `valkey_aa_straggler_nodes_ejected_total` and emits a high-priority warning log.
3. **Active Tombstone Purge**: The background GC cron iterates over `db->tombstone_dict` and `crdt_tombstones`:
   - Any tombstone $T$ with $\text{HLC}(T) \le H_{\text{stable}}^Q$ is freed immediately via `zfree()`.

#### 4. Dead-Node Fencing & Safe Reintegration Protocol
When an ejected node $k$ recovers and reconnects to the cluster after $\tau_{\text{eject}}$:
1. **State Isolation (`REINTEGRATION_FENCED`)**:
   - The surviving cluster marks node $k$ as `REINTEGRATION_FENCED`.
   - **Invariant**: Node $k$ is strictly forbidden from directly streaming raw replication delta frames into surviving nodes. Because surviving nodes have already garbage-collected tombstones with $\text{HLC} \le H_{\text{stable}}^Q$, raw writes from node $k$ with old timestamps would resurrect deleted keys as zombies!
2. **Mandatory Anti-Entropy Sync**:
   - Node $k$ is forced to initiate a non-destructive active-active state sync (`FULLRESYNC_MERGE` or `AAE_MERKLE`).
   - During sync, keys present on node $k$ with $\text{HLC} \le H_{\text{stable}}^Q$ that do not exist on the surviving cluster are identified as previously deleted keys and deleted locally on node $k$.
3. **Quorum Re-admission**:
   - Once node $k$'s Merkle tree matches the cluster digest up to $H_{\text{stable}}^Q$, its status transitions to `ACTIVE_MEMBER`.
   - Node $k$ is re-added to $M_{\text{active}}$, its heartbeat lease is renewed, and it resumes participation in future $H_{\text{stable}}^Q$ calculations.

---

### Option 8.2: Tombstone Time-To-Live (TTL Horizon Pruning)
- **Mathematical Invariant**:
  - Tombstones are assigned a fixed physical retention duration $T_{\text{tombstone\_ttl}}$ (e.g., 7 days):
    $$T_{\text{tombstone\_ttl}} \ge 2 \cdot \Delta t_{\text{skew\_max}} + T_{\text{max\_partition\_sla}}$$
  - A background active-expiration thread iterates over `db->tombstone_dict` and deletes expired tombstones.
  - *Failure Risk*: If a network partition lasts longer than $T_{\text{tombstone\_ttl}}$, reconnecting nodes may resurrect zombie keys unless fenced by the reintegration protocol.

### Option 8.3: Whole-Database Purge via Database Epoch Versioning (`FLUSHDB` / `FLUSHALL`)
- **Problem**: Executing `FLUSHDB` on a database with $100\text{M}$ keys cannot allocate $100\text{M}$ tombstones without crashing the server due to OOM!
- **Mechanics**:
  - Each database maintains a 64-bit **Database Epoch Counter** $E_{\text{db}}$ and an associated $\text{HLC}_{\text{flush}}$.
  - Executing `FLUSHDB` increments $E_{\text{db}} \leftarrow E_{\text{db}} + 1$, records $\text{HLC}_{\text{flush}}$, and immediately wipes all memory keys in $O(1)$ time.
  - When an incoming replication frame arrives with $\text{HLC}_{\text{cmd}} < \text{HLC}_{\text{flush}}$, it is dropped immediately at the protocol gate without dictionary lookup.

---

## 9.3 Comprehensive 4-Pillar Evaluation: Deletion & GC Strategies

| Evaluation Pillar | Option 8.1 (Dynamic Quorum Stability Horizon) | Option 8.2 (Tombstone TTL Pruning) | Option 8.3 (Database Epoch Versioning) |
|---|---|---|---|
| **1. Data Safety & Consistency** | **Maximal**: Mathematically impossible to resurrect zombie keys; 100% SEC safe with Reintegration Fencing. | **High within SLA**: Safe as long as partition duration $< T_{\text{tombstone\_ttl}}$. | **Maximal**: Protects entire database flush operations from cross-DC zombie insertion. |
| **2. Overhead & Performance** | Memory: Tombstones freed as fast as WAN replication acknowledges ($<100\text{ ms}$). Zero OOM lockup. | Memory: Tombstones retained for full TTL duration (e.g. 7 days), consuming RAM. | **Zero Overhead**: $O(1)$ memory and CPU for whole-database purge. |
| **3. Failure Modes & Edge Cases** | Node ejection unfreezes GC; rejoining node requires full Merkle/RDB sync before delta streaming. | If outage exceeds $T_{\text{tombstone\_ttl}}$, zombie resurrection occurs upon link restoration without fencing. | Requires handling key recreation after flush with higher HLC. |
| **4. Complexity** | Medium-High: Dynamic quorum membership, heartbeat lease timers, and fencing state machine. | Low: Simple timer-based sliding expiration thread. | Minimal: Single integer check in command execution path. |

---

# 10. Domain 9: Network Partitions & Outage Reconciliation

## 10.1 Technical Problem Statement: Backlog Overflow & Stream Divergence
Under prolonged WAN partitions, the circular replication backlog ring buffers inevitably overflow. Classical Valkey responds with `FULLRESYNC`, executing `emptyDb()` on the replica before loading the snapshot.

In Active-Active systems:
1. **Destructive Reset Catastrophe**: Calling `emptyDb()` wipes all local concurrent writes accepted during the partition.
2. **Stream Divergence**: Both partitions accepted independent writes; neither side is a pure linear predecessor of the other.

---

## 10.2 Deep Dive: Four Partition Reconciliation Options

```
+-------------------------------------------------------------------------------------------------------+
|                                OUTAGE RECONCILIATION TAXONOMY                                         |
|                                                                                                       |
| Option 9.1: Vectorized PSYNC (`PSYNC_AA`)       - Multi-origin circular ring delta streaming          |
| Option 9.2: Merge-on-Load RDB (`FULLRESYNC_MERGE`) - Non-destructive snapshot merge loader            |
| Option 9.3: Merkle Anti-Entropy (`AAE_MERKLE`)    - Hierarchical cryptographic key tree exchange      |
| Option 9.4: Multi-Phase Key-Scan (`ITERATIVE_SCAN`) - Cursor-based chunked streaming & delta drain     |
+-------------------------------------------------------------------------------------------------------+
```

### Option 9.1: Multi-Origin Backlogs & Vectorized PSYNC (`PSYNC_AA`)
- **Mechanics**:
  - Each node maintains an independent circular backlog for every origin node: $\text{Backlog}[ \text{NodeID}_k ]$.
  - Reconnecting node requests delta via:
    `PSYNC_AA <local_node_id> <V_repl_vector>`
  - Remote peer evaluates $\Delta \vec{V} = \vec{V}_{\text{remote}} - \vec{V}_{\text{local}}$. If all required offsets are present in the origin backlogs, it streams missing delta frames sequentially.

### Option 9.2: Non-Destructive Merge-on-Load Full RDB Synchronization
- **Mechanics**:
  - Remote peer forks `bgsave` to produce an Active-Active RDB snapshot containing CRDT metadata headers.
  - Receiving node receives the RDB stream over TLS.
  - **CRITICAL**: The receiver **bypasses `emptyDb()`**.
  - An iterative merge loader parses incoming keys:
    - If key does not exist locally $\implies$ Insert directly.
    - If key exists locally $\implies$ Invoke type-specific CRDT merge function:
      $$\text{Value}_{\text{merged}} = \text{MergeCRDT}(V_{\text{local}}, V_{\text{incoming}})$$

### Option 9.3: Range-Partitioned Merkle Tree Anti-Entropy Background Sync (`AAE_MERKLE`)
- **Mechanics**:
  - Keyspace divided into $S = 16,384$ cluster hash slots.
  - Each slot maintains a hierarchical binary or 16-ary Merkle tree:
    $$\text{LeafHash}(B_j) = \bigoplus_{k \in B_j} \text{BLAKE3}\Big( k \;\|\; \text{Type}(k) \;\|\; \text{HLC}(k) \;\|\; \text{CRDT\_Digest}(k) \Big)$$
    $$\text{NodeHash}(N) = \text{BLAKE3}\Big( \text{ChildHash}_1 \;\|\; \dots \;\|\; \text{ChildHash}_{16} \Big)$$
  - Nodes exchange top-level root hashes ($O(1)$). If roots match, keyspaces are identical (0 bytes exchanged). If roots differ, traverse tree in $O(\log K)$ to pinpoint and synchronize divergent keys.

### Option 9.4: Multi-Phase Key-Scan & Delta-Dump Reconciliation (`ITERATIVE_SCAN`)
- **Mechanics**:
  - Cursor-based non-blocking keyspace sweep in small time-sliced batches (e.g., 100 keys per 5ms event loop tick).
  - Modified keys during scan tracked in a transient dirty-keys buffer; drained to peer upon sweep completion.

---

## 10.3 Comprehensive 4-Pillar Evaluation: Partition Reconciliation

| Evaluation Pillar | Option 9.1 (`PSYNC_AA`) | Option 9.2 (`FULLRESYNC_MERGE`) | Option 9.3 (`AAE_MERKLE`) | Option 9.4 (`ITERATIVE_SCAN`) |
|---|---|---|---|---|
| **1. Data Safety & Consistency** | **SEC Guaranteed**; strictly causal delta playback. Zero data loss. | **SEC Guaranteed**; non-destructive snapshot merge preserves local writes. | **Deterministic SEC**; continuously repairs silent corruptions and bit-rot. | **SEC Guaranteed**; multi-phase delta drain guarantees convergence. |
| **2. Overhead & Performance** | Near-zero CPU (direct ring buffer socket write). Memory: $N \times \text{backlog}$. | High CPU spike during RDB fork and merge deserialization. Low steady RAM. | Continuous low CPU (background BLAKE3 updates). Memory: $4\text{--}16\text{ MB}$/1M keys. | Low & smooth CPU distributed across event loop ticks. Minimal memory. |
| **3. Failure Modes & Edge Cases** | Backlog wrap-around requires fallback to Option 9.2 or 9.3. | Memory spike if massive Copy-on-Write occurs during background fork. | High write churn causes frequent tree hash recalculations. | Scan starvation under high keyspace churn; longer convergence time. |
| **4. Complexity** | Moderate: Vector comparison and multi-origin ring management. | High: Custom streaming RDB merge engine in `rdb.c`. | High: Merkle tree maintenance in RAM and background sync worker thread. | Low-Medium: Cursor iteration and dirty buffer tracking. |

---

# 11. Domain 10: Engine Architecture, Extensibility & Persistence Subsystems

## 11.1 Technical Problem Statement: Core vs Module vs Hybrid
Valkey's core engine is optimized for single-threaded speed, zero-copy memory access, and tight cache locality. Integrating Active-Active replication presents a fundamental trade-off:

```
+--------------------------------------------------------------------------------------------------------+
|                                ENGINE EXTENSION ARCHITECTURES                                          |
|                                                                                                        |
| Option 10.1: Native In-Core Extension      - Direct modifications to Valkey C source tree              |
| Option 10.2: Valkey Module API Extension   - 100% out-of-tree dynamic module (`aa_crdt.so`)             |
| Option 10.3: Hybrid Core-Hooked Engine     - Thin fast-path C hook layer + Pluggable CRDT library      |
+--------------------------------------------------------------------------------------------------------+
```

---

## 11.2 Deep Dive: Three Engine Architectures

### Option 10.1: Native In-Core Engine Extension
- **Mechanics**: Direct integration into `server.h`, `dict.c`, `rdb.c`, `aof.c`, and `replication.c`.
  - Bitfield packing in `redisObject`:
```c
struct redisObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:LRU_BITS;
    int refcount;
    void *ptr;
    uint64_t hlc; /* 48-bit physical ms + 16-bit logical counter */
};
```
  - Direct replication interception in `server.c:call()` and `replicationFeedSlaves()`.

### Option 10.2: Pure Valkey Module API Custom Types & Hooks
- **Mechanics**: Implemented as a standalone loadable module (`aa_crdt.so`) using `ValkeyModule_CreateDataType()`, `ValkeyModule_RegisterCommandFilter()`, and Module I/O callbacks.
- **Limitations**: Command filter string parsing copies arguments; custom types require new command names (`AA.SET`, `AA.HSET`) unless core command tables are dynamically intercepted.

### Option 10.3: Hybrid Core-Hooked Engine Architecture
- **Mechanics**: Introduces a minimal fast-path C hook table in Valkey core ($<300$ lines of churn), delegating CRDT algorithms and merge math to a dynamically linked library:
```c
typedef struct valkey_aa_hooks {
    int (*on_pre_command)(client *c, uint64_t *hlc, uint32_t *origin_id);
    void (*on_post_command)(client *c, robj *key, robj *val);
    int (*crdt_merge_fn)(robj *local_val, robj *incoming_val, uint64_t remote_hlc);
    int (*rdb_load_merge)(sds key, robj *val, uint64_t hlc);
} valkey_aa_hooks;
```

---

## 11.3 Persistence Integration: RDB v12+ & AOF Rewrite

### RDB v12+ Metadata Extension Opcodes
Active-Active metadata is serialized using standardized auxiliary RDB opcodes to maintain backward compatibility:

```
Standard Key-Value in RDB:
[EXPIRETIME_MS: 1B][Timestamp: 8B][ValueType: 1B][Key: String][Value: Blob]

Active-Active Extended Key-Value in RDB v12+:
[RDB_OPCODE_AA_META: 1B]
  ├── [HLC_Physical: 6B] (48-bit ms)
  ├── [HLC_Logical: 2B]  (16-bit counter)
  ├── [Origin_Node_ID: 2B] (16-bit cluster origin index)
  └── [Flags: 1B] (0x01: IsTombstone, 0x02: HasVectorClock, 0x04: CRDTExt)
[EXPIRETIME_MS: 1B][Timestamp: 8B][ValueType: 1B][Key: String][Value: Blob]
```

### AOF Rewrite Integration & Replication Interleaving
- **The Causality Problem**: Standard `BGREWRITEAOF` reconstructs keys without CRDT metadata. Replaying an AOF would reset all HLCs to restart time, destroying causal lineage.
- **Solution (`AA.RESTORE`)**: AOF rewrite emits binary CRDT snapshot blocks:
  `AA.RESTORE <key> <ttl> <binary_crdt_payload>`
  Replay is bit-for-bit CRDT state reproduction with zero parse overhead.

---

## 11.4 Comprehensive 4-Pillar Evaluation: Engine Architecture

| Evaluation Pillar | Option 10.1 (Native In-Core) | Option 10.2 (Pure Module API) | Option 10.3 (Hybrid Core-Hooked) |
|---|---|---|---|
| **1. Data Safety & Guarantees** | **Maximal**: Direct control over memory fences, AOF ordering, and dict mutations. | **High**: Safe within module boundary; edge cases in command filter ordering. | **Maximal**: Core enforces deterministic dispatch; module provides verified CRDT logic. |
| **2. Performance & Latency** | **Zero Overhead**: Zero-copy C struct access; zero function call overhead. | **5% - 15% Latency Penalty**: Command filter parsing, string copies, API wrappers. | **< 1% Overhead**: Direct inline C function pointers; zero-copy buffers. |
| **3. Memory Footprint** | **Optimal**: Bitfield packing inside `robj` and `dictEntry`. | **High Overhead**: Separate module wrapper structs and indirect dicts. | **Near Optimal**: Inline struct in core types + compact library descriptors. |
| **4. Upstream Maintainability** | **Poor**: High rebase conflict risk against upstream Valkey releases. | **Flawless**: Decoupled from core source tree; builds as standalone `.so`. | **Excellent**: Core patch is small and static; CRDT engine decoupled. |

---

# 12. Domain 11: Observability, Telemetry & Diagnostics Subsystems

## 12.1 Technical Problem Statement: Multi-Master Telemetry Invariants
Single-master replication tracks a single scalar lag: $\Delta_{\text{offset}} = \text{master\_offset} - \text{replica\_offset}$.

Active-Active systems operate in an $N$-dimensional, partially ordered state space:
1. **$N \times N$ Matrix Dimensionality**: In a 5-region mesh, 20 directional replication links exist with independent transit and queue delays.
2. **Invisible Data Overwrites**: LWW and CRDT merges resolve conflicts silently without returning client errors. Telemetry must expose silent overwrite rates.
3. **Tombstone Heap Bloat**: If one region stalls, GC freezes globally; telemetry must pinpoint the stalling peer.

---

## 12.2 Telemetry Architecture & Metrics Catalog

### 12.2.1 Two-Way Link Lag Probing (`AA.PING` / `AA.PONG`)
- Heartbeat packets transmitted every $100\text{ ms}$ calculate:
  - **One-Way Network Transit Latency**: $T_{\text{transit}} = \frac{T_{\text{recv}} - T_{\text{send}}}{2}$.
  - **Replication Queue Lag**: Unprocessed bytes buffered in socket egress/ingress descriptors.
  - **Logical Lag (Bytes)**: $\Delta_{\text{bytes}}(i, j) = \text{Offset}_{\text{sent}}^i(j) - \text{Offset}_{\text{acked}}^i(j)$.
  - **Time-Based Lag (Wall Clock)**: $\Delta_{\text{time}}(i, j) = \text{WallClock}_{\text{now}} - \text{HLC}_{\text{latest\_applied}}(j)$.

### 12.2.2 Offset Vector Skew & Divergence Velocity
- **Vector Skew**: Manhattan distance between vector clocks:
  $$D_{\text{skew}}(A, B) = \sum_{k=1}^N \Big| V_A[k] - V_B[k] \Big|$$
- **Divergence Velocity**: Rate of change of unmerged operations per second:
  $$v_{\text{div}} = \frac{d}{dt} D_{\text{skew}}(A, B)$$

### 12.2.3 Conflict & Convergence Telemetry Struct
```c
typedef struct aaConflictMetrics {
    atomic_uint_fast64_t lww_eval_total;
    atomic_uint_fast64_t lww_local_won;             /* Local write was newer; remote discarded */
    atomic_uint_fast64_t lww_remote_won;            /* Remote write was newer; local overwritten */
    atomic_uint_fast64_t lww_tie_break_nodeid;      /* Timestamps identical; NodeID tie-break */
    atomic_uint_fast64_t type_clash_total;          /* Concurrent writes with different types */
    atomic_uint_fast64_t crdt_hash_field_merges;
    atomic_uint_fast64_t crdt_set_or_merges;
    atomic_uint_fast64_t crdt_pn_counter_deltas;
    atomic_uint_fast64_t crdt_rga_vertex_inserts;
    atomic_uint_fast64_t hlc_forward_drifts;        /* Local HLC bumped by remote HLC */
    atomic_uint_fast64_t hlc_max_drift_ms;          /* Maximum observed clock discrepancy */
    atomic_uint_fast64_t clock_skew_violations;     /* Remote packets rejected (> MAX_CLOCK_SKEW_MS) */
    atomic_uint_fast64_t hlc_borrowed_ms_total;     /* Cumulative borrowed time during counter saturation */
    atomic_uint_fast64_t hlc_backpressure_stalls;   /* Client write micro-throttles triggered */
    atomic_uint_fast64_t straggler_nodes_ejected;   /* Stragglers removed from GC quorum (> tau_eject) */
    atomic_uint_fast64_t rdb_merge_yield_cycles;    /* Event-loop cooperative yields during RDB merge */
    atomic_uint_fast64_t fixed_point_overflows;     /* Overflows detected in 128-bit float counters */
} aaConflictMetrics;
```

---

## 12.3 Comprehensive Prometheus / OpenTelemetry Metrics Catalog

```
# HELP valkey_aa_replication_lag_seconds Directional replication lag per peer node in seconds
# TYPE valkey_aa_replication_lag_seconds gauge
valkey_aa_replication_lag_seconds{local_region="us-east",peer_region="eu-west"} 0.042
valkey_aa_replication_lag_seconds{local_region="us-east",peer_region="ap-northeast"} 0.118

# HELP valkey_aa_replication_backlog_bytes Unacknowledged bytes in replication egress buffer
# TYPE valkey_aa_replication_backlog_bytes gauge
valkey_aa_replication_backlog_bytes{peer_region="eu-west"} 1048576

# HELP valkey_aa_conflict_events_total Total number of concurrent write conflicts evaluated
# TYPE valkey_aa_conflict_events_total counter
valkey_aa_conflict_events_total{type="lww_local_won"} 14209
valkey_aa_conflict_events_total{type="lww_remote_won"} 13890
valkey_aa_conflict_events_total{type="lww_nodeid_tie_break"} 42
valkey_aa_conflict_events_total{type="type_clash_overwrite"} 3

# HELP valkey_aa_tombstone_memory_bytes Total memory consumed by deletion tombstones
# TYPE valkey_aa_tombstone_memory_bytes gauge
valkey_aa_tombstone_memory_bytes 41943040

# HELP valkey_aa_stability_horizon_timestamp_ms Current global stability horizon timestamp
# TYPE valkey_aa_stability_horizon_timestamp_ms gauge
valkey_aa_stability_horizon_timestamp_ms 1724214580000

# HELP valkey_aa_clock_drift_ms Maximum observed physical clock drift against peer nodes
# TYPE valkey_aa_clock_drift_ms gauge
valkey_aa_clock_drift_ms{peer_region="eu-west"} 4.2

# HELP valkey_aa_clock_skew_violations_total Untrusted remote packets rejected due to excessive clock skew
# TYPE valkey_aa_clock_skew_violations_total counter
valkey_aa_clock_skew_violations_total{peer_region="eu-west"} 0

# HELP valkey_aa_hlc_borrowed_ms_total Total milliseconds borrowed due to logical counter saturation
# TYPE valkey_aa_hlc_borrowed_ms_total counter
valkey_aa_hlc_borrowed_ms_total 0

# HELP valkey_aa_hlc_backpressure_stalls_total Event loop client write pauses triggered by HLC backpressure
# TYPE valkey_aa_hlc_backpressure_stalls_total counter
valkey_aa_hlc_backpressure_stalls_total 0

# HELP valkey_aa_straggler_nodes_ejected_total Straggler nodes evicted from stability horizon GC quorum
# TYPE valkey_aa_straggler_nodes_ejected_total counter
valkey_aa_straggler_nodes_ejected_total 0

# HELP valkey_aa_rdb_merge_yield_cycles_total Time-sliced cooperative event loop yields during RDB merge
# TYPE valkey_aa_rdb_merge_yield_cycles_total counter
valkey_aa_rdb_merge_yield_cycles_total 128

# HELP valkey_aa_topology_health_status 0=Healthy, 1=Degraded, 2=Asymmetric, 3=SplitBrain
# TYPE valkey_aa_topology_health_status gauge
valkey_aa_topology_health_status 0
```

---

# 13. Domain 12: Architectural Synthesis, Decision Framework & Implementation Roadmap

## 13.1 Master Architectural Component Interaction Graph

```
+---------------------------------------------------------------------------------------------------+
|                               VALKEY ACTIVE-ACTIVE CORE ENGINE ARCHITECTURE                       |
|                                                                                                   |
|  [Client Application]                                                                             |
|          | (RESP3 Commands)                                                                       |
|          v                                                                                        |
|  +---------------------------------------------------------------------------------------------+  |
|  | Event Loop (ae.c) & Fast-Path Hook Interceptor                                              |  |
|  | - Extracts/Synthesizes HLC Timestamp (HLC-64 with clock borrowing & drift clamping)         |  |
|  | - Evaluates Event-Loop Micro-Throttling Backpressure (borrowed_ms > 500ms)                  |  |
|  | - Evaluates Non-Monotonic Predicates (CP Sequencer Lease vs Local AP Speculative)           |  |
|  +---------------------------------------------------------------------------------------------+  |
|          |                                                  |                                     |
|          v (Local In-Memory Execution)                      v (Replication Serialization)         |
|  +---------------------------------------+      +----------------------------------------------+  |
|  | Core Keyspace & CRDT Tables           |      | Dual-Role Replication Stream Multiplexer     |  |
|  | - Intrusive dictEntry (8B HLC)        |      | - Wire Framer (METADATA <HLC> <Origin> <Cmd>)|  |
|  | - crdt_listpack / crdt_intset Compact |      | - Vector Offset Manager (V_repl)             |  |
|  | - 128-bit Scaled Fixed-Point Counters |      | - Isolated Per-Origin Circular Ring Backlogs |  |
|  | - Field-Level LWW Hash Entries        |      | - Loop Drop & Seen-Bitmask Deduplicator      |  |
|  | - Add-Wins OR-Set Tag Registries      |      +----------------------------------------------+  |
|  | - RGA List Vertices & Streams Radix   |                      |                                 |
|  +---------------------------------------+                      | (TLS Cross-DC Streaming)        |
|          ^                     ^                                v                                 |
|          | (Merge Updates)     | (GC Prune)             +----------------------------------------+  |
|  +-----------------------+ +-----------------------+    | Remote Peer Gateway / Peer Primary     |  |
|  | Time-Sliced Merge Eng | | Stability Horizon GC  |    | (DC-West, DC-Europe, DC-Asia)          |  |
|  | - PSYNC_AA Processor  | | - Dynamic Quorum H_st |    +----------------------------------------+  |
|  | - <=2ms Slice Merge   | | - Heartbeat Lease Evic|                                              |
|  | - Merkle Anti-Entropy | | - Reintegration Fence |                                              |
|  +-----------------------+ +-----------------------+                                              |
+---------------------------------------------------------------------------------------------------+
```

---

## 13.2 Subsystem Architectural Decision Matrix

| Subsystem Dimension | Recommended Primary Architecture | Alternative Architecture | Key Justification |
|---|---|---|---|
| **Topology (Domain 2)** | **Hierarchical Gateway Mesh (2.4)** for large clusters; **Full Mesh (2.1)** for $N \le 4$. | Hub-and-Spoke (2.2) | Minimizes WAN connection scaling ($O(M^2)$ vs $O(S^2)$) and enables WAN compression. |
| **Sharding (Domain 3)** | **Inter-Shard Cross-Cluster Peering (3.1)** | Multi-Primary Intra-Shard (3.2) | Completely isolates failure domains; preserves standard Valkey cluster gossip engine. |
| **Replication Wire (Domain 4)** | **`METADATA` Wrapper + Offset Vector $\vec{V}_{\text{repl}}$ + Per-Origin Backlogs** | Binary Framing Protocol | 100% RESP2/RESP3 compatible; eliminates offset aliasing and silent data truncation during failover. |
| **Clocks & Ordering (Domain 5)** | **HLC-64 + Clock Borrowing + Drift Clamping ($\Delta_{\max} = 500\text{ ms}$)** | 128-bit Extended HLC | 8 bytes per key/field, handles $>65.5\text{M}$ ops/sec without counter overflow, rejects malicious/broken clock drag. |
| **Compact CRDT Memory (Domain 5)** | **`crdt_intset` (Base HLC) & `crdt_listpack` (LEB128 Delta Encoding)** | Universal Dict Promotion | Eliminates the $1,200\%$ ($12\times$) memory cliff, maintaining memory parity with standard Valkey. |
| **Float CRDTs (Domain 6)** | **128-bit Scaled Decimal Fixed-Point ($\times 10^{18}$)** | Float PN-Counter / LWW | Guarantees strict associativity/commutativity, eliminating non-deterministic IEEE 754 float drift. |
| **Hierarchical Deletion (Domain 7)** | **$\text{Field } f \text{ Survives} \iff \text{HLC}(f) > \max(\text{HLC}_{\text{key\_tomb}}, \text{HLC}_{\text{field\_tomb}})$** | Container Epoch Wipe | Deterministic, order-independent reconciliation of container vs field mutations. |
| **Non-Monotonic Ops (Domain 7)** | **Mode A: CP Authoritative Sequencer Lease; Mode B: Explicit Local AP** | Unsafe Silent AP | Proves AP mutual exclusion fallacy; prevents silent split-brain lock/sequence corruption. |
| **Tombstone GC (Domain 8)** | **Dynamic Quorum Stability Horizon ($H_{\text{stable}}^Q$) + Lease Eviction ($\tau_{\text{eject}}$) + Fencing** | Fixed Tombstone TTL | Guarantees zero zombie resurrection while preventing dead-node stragglers from freezing GC and causing OOM. |
| **Partition Recovery (Domain 9)** | **Tier 1: `PSYNC_AA` $\to$ Tier 2: Time-Sliced Merge-on-Load RDB ($\le 2\text{ms}$ ticks) $\to$ Tier 3: Merkle Anti-Entropy** | Key-Scan Dump Sync | Tiered escalation: microsecond deltas for blips; non-blocking snapshot merge yielding to `aeProcessEvents()`. |
| **Engine Structure (Domain 10)** | **Hybrid Core-Hooked Architecture (10.3)** | Native In-Core (10.1) | Keeps Valkey core rebase clean while avoiding the 15% latency penalty of vanilla Module API. |
| **Telemetry (Domain 11)** | **Two-Way Heartbeat Probing + Skew/Borrow/Yield/Straggler Prometheus Catalog** | Basic `INFO replication` | Exposes multidimensional link lag, silent overwrite rates, tombstone heap sizing, and clock borrowing metrics. |

---

## 13.3 Phased Implementation Roadmap

```
+-----------------------------------------------------------------------------------------------+
|                             FOUR-PHASE IMPLEMENTATION ROADMAP                                 |
|                                                                                               |
| Phase 1: Core Engine Hooks, Clocks & Replication Wire Protocol (Weeks 1 - 8)                  |
| - Implement HLC-64 clock generator with borrowing & drift clamping in `src/hlc.c`             |
| - Implement `METADATA` command wrapper framing in `src/replication.c`                         |
| - Implement Offset Vector $\vec{V}_{\text{repl}}$ and per-origin circular ring backlogs       |
| - Implement `PSYNC_AA` handshake and multi-origin delta streaming engine                      |
|                                                                                               |
| Phase 2: Primitive Data Structures, Compact Encodings & RDB Merge Engine (Weeks 9 - 16)       |
| - Implement HLC-LWW Register for Strings and Bitmaps with 8-byte `dictEntry` embedding       |
| - Implement 128-bit Scaled Fixed-Point Counter for `INCRBY` / `INCRBYFLOAT`                   |
| - Implement `crdt_intset` and `crdt_listpack` compact memory encodings with LEB128 varints    |
| - Implement Time-Sliced Merge-on-Load RDB parser with cooperative event-loop yielding        |
| - Implement RDB v12+ `RDB_OPCODE_AA_META` metadata serialization                              |
|                                                                                               |
| Phase 3: Complex Collection CRDTs, Dynamic Stability GC & Anti-Entropy (Weeks 17 - 26)        |
| - Implement Field-Level LWW Hash Table with Hierarchical Tombstones                           |
| - Implement Observed-Removed Set (Add-Wins OR-Set) for Sets and Sorted Sets                   |
| - Implement Replicated Growable Array (RGA) list CRDT                                         |
| - Implement Dynamic Quorum Stability Horizon GC with Heartbeat Lease Eviction ($\tau_{eject}$)|
| - Implement Dead-Node Reintegration Fencing and Background Merkle Anti-Entropy (`AAE_MERKLE`) |
|                                                                                               |
| Phase 4: Production Hardening, Cluster Mesh & Observability (Weeks 27 - 36)                   |
| - Implement CP Sequencer Lease proxy routing for `SETNX` / non-monotonic commands             |
| - Implement Hierarchical Replication Gateway daemon (`valkey-aa-gateway`)                     |
| - Implement Cluster Slot Migration Handshake (`REPL_MOVED` and `DUMP_AA` / `RESTORE_AA`)     |
| - Implement Full Prometheus Metrics Catalog, Skew Detectors, and Telemetry Counters          |
| - Comprehensive Chaos Testing: Asymmetric partitions, clock skew injection, Jepsen validation|
+-----------------------------------------------------------------------------------------------+
```

---
*End of Master Architecture Specification.*
