# Valkey Active-Active CRDT List: Architecture & Implementation Specification

**Target System**: Valkey Core (Active-Active Multi-Primary Engine)  
**Component**: In-Core Sequence CRDT (RGA List Engine)  
**Status**: Implemented & Verified  

---

## 1. Executive Summary & Problem Statement

### 1.1 The Classical Sequential Collection Dilemma
In single-primary Valkey, lists rely on relative integer array indexing (`LINDEX 0`, `LINSERT AFTER pivot val`, `LPUSH`). 

In an Active-Active multi-primary topology where multiple geographically separated primaries accept writes concurrently without synchronous coordination, integer indexing breaks completely:
- If Primary 1 executes `LPUSH list "A"` at index 0, and Primary 2 concurrently executes `LPUSH list "B"` at index 0, naive operation replication causes non-deterministic interleaving, silent displacement, and state divergence.
- If Primary 1 deletes an element (`LPOP`) while Primary 2 concurrently inserts a child next to that element, naive index shifts cause the insertion to attach to the wrong neighbor or cause data corruption.

```
       Naive Integer Indexing Inversion
       Initial: ["X", "Y"]
       DC-1: LPUSH list "A"  ---> Local state: ["A", "X", "Y"]
       DC-2: LPUSH list "B"  ---> Local state: ["B", "X", "Y"]
       Cross-Replication:
         - DC-1 receives LPUSH "B" ---> ["B", "A", "X", "Y"]
         - DC-2 receives LPUSH "A" ---> ["A", "B", "X", "Y"]  <=== FATAL DIVERGENCE!
```

### 1.2 The Active-Active RGA Solution
To guarantee **Strong Eventual Consistency (SEC)** while maintaining sub-millisecond local write latency ($<1\text{ ms}$), Valkey implements an in-core **Replicated Growable Array (RGA)** CRDT.

Key Guarantees:
1. **Local-Latency Writes**: All list mutations (`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LINSERT`, `LSET`, `LREM`) execute in local RAM in $O(1)$ / $O(\log N)$ time.
2. **Deterministic Total Order**: Sibling insertions anchored to the same predecessor vertex are ordered deterministically by composite Hybrid Logical Clock (HLC) timestamps with node ID tie-breaking.
3. **Lossless Partition Recovery**: Network partitions and buffer overruns recover via a non-destructive state merge (`FULLRESYNC_MERGE`), joining remote and local list states ($S_{\text{local}} \sqcup S_{\text{remote}}$) without invoking `emptyDb()`.
4. **Bounded Memory via Stability Horizon GC**: Deletions create tombstones to preserve position references for concurrent inserts; tombstones are safely garbage-collected once all active replicas acknowledge past the deletion timestamp ($H_{\text{stable}}^Q$).

---

## 2. Mathematical Foundations & RGA Algorithms

### 2.1 Hybrid Logical Clock (HLC-64) & Vertex Identity
Every vertex in the list carries a globally unique 96-bit coordinate $\text{crdtId}$:
$$\text{crdtId} = \langle \text{HLC} \ (64\text{-bit}), \ \text{origin\_id} \ (32\text{-bit}) \rangle$$

```c
typedef union crdtClock {
    uint64_t raw;
    struct {
        uint64_t logical  : 16;  /* Logical sequence counter (0..65,535 ops/ms) */
        uint64_t physical : 48;  /* Physical millisecond timestamp (~8,925 years) */
    } parts;
} crdtClock;

typedef struct crdtId {
    uint64_t hlc;       /* 64-bit HLC */
    uint32_t origin_id; /* 32-bit Node/Datacenter Identifier */
} crdtId;
```

#### Monotonic Clock Advance & Hardening:
- **`hlc_now()`**: If physical time advances, the logical counter resets to 0. If multiple writes occur within the same physical millisecond, `logical++` increments. If `logical == 65535` (saturation), it borrows 1ms forward into physical time (`borrowed_ms++`). If borrowing exceeds `max_borrow_ms` (500ms), it triggers backpressure.
- **`hlc_recv()`**: Upon receiving a remote timestamp $T_{\text{remote}}$, the local clock ratchets forward:
  $$\text{HLC}_{\text{local}} \gets \max(\text{physical}_{\text{local}}, T_{\text{remote}}.\text{physical}) + \text{logical}$$
  If $T_{\text{remote}}.\text{physical} > \text{physical}_{\text{now}} + \text{max\_skew\_ms}$, the frame is quarantined for clock drift violation.

#### Deterministic Comparison Function:
$$\text{crdtIdCmp}(A, B) = \begin{cases} +1 & \text{if } A.\text{hlc} > B.\text{hlc} \\ -1 & \text{if } A.\text{hlc} < B.\text{hlc} \\ +1 & \text{if } A.\text{hlc} == B.\text{hlc} \land A.\text{origin\_id} > B.\text{origin\_id} \\ -1 & \text{if } A.\text{hlc} == B.\text{hlc} \land A.\text{origin\_id} < B.\text{origin\_id} \\ 0 & \text{if } A == B \end{cases}$$

---

### 2.2 In-Memory RGA Graph Representation
The RGA list is represented as a doubly-linked chain indexed by a Radix Tree (`rax`) for $O(\log N)$ random-access lookups:

```
[ Head Sentinel (0,0) ] <---> [ Vertex 1 ] <---> [ Vertex 2 (Tombstone) ] <---> [ Vertex 3 ] <---> [ Tail (MAX,MAX) ]
         |                           ^                     ^                           ^
         |                           |                     |                           |
         +=========== rax Tree ======+=====================+===========================+
                      (O(log N) crdtId -> crdtListVertex* index)
```

```c
typedef struct crdtListVertex {
    crdtId id;                  /* Unique vertex identity */
    crdtId parent_id;           /* Predecessor vertex identity when inserted */
    sds val;                    /* String payload (NULL if tombstone) */
    uint64_t del_hlc;           /* Timestamp of deletion (0 if active) */
    uint32_t del_origin;        /* Origin node ID that deleted the vertex */
    uint8_t deleted;            /* 1 = tombstone, 0 = visible */
    uint8_t gc_keep;            /* Mark-and-sweep GC retention flag */
    struct crdtListVertex *parent; /* Direct parent tree link */
    struct crdtListVertex *prev;   /* Doubly-linked list predecessor */
    struct crdtListVertex *next;   /* Doubly-linked list successor */
} crdtListVertex;

typedef struct crdtList {
    crdtListVertex *head;       /* Sentinel root */
    crdtListVertex *tail;       /* Sentinel tail */
    rax *index;                 /* Radix tree index: crdtId -> crdtListVertex* */
    size_t length;              /* Visible count (non-deleted items) */
    size_t tombstone_count;     /* Count of deleted tombstone vertices */
    size_t total_vertices;      /* Total vertices in memory */
    uint64_t max_hlc;           /* Highest observed HLC */
} crdtList;
```

---

### 2.3 Deterministic Sibling Linearization Algorithm
When inserting a new vertex $v_{\text{new}}$ with parent anchor $p$:
1. Locate parent vertex $p$ in $O(\log N)$ time via `raxFind(list->index, &parent_id)`.
2. Traverse forward from $p\text{.next}$:
   - For every vertex $v_{\text{curr}}$ encountered:
     - If $v_{\text{curr}}$ is a sibling (inserted after the same parent $p$):
       - If $\text{crdtIdCmp}(v_{\text{new}}.\text{id}, v_{\text{curr}}.\text{id}) > 0 \implies$ insert $v_{\text{new}}$ before $v_{\text{curr}}$ (higher HLC sits closer to parent).
       - Else, skip past $v_{\text{curr}}$ and all of $v_{\text{curr}}$'s descendant sub-tree.
     - Else (if $v_{\text{curr}}$ is a descendant of a sibling with higher priority), skip past it.
3. Splice $v_{\text{new}}$ into the doubly-linked list and insert into `list->index`.

```c
crdtListVertex *crdtListInsertAfter(crdtList *list, crdtId parent_id, crdtId new_id, sds val) {
    crdtListVertex *parent = crdtListLookupVertex(list, parent_id);
    if (!parent) parent = list->head;

    /* Sibling RGA traversal */
    crdtListVertex *curr = parent->next;
    while (curr != list->tail) {
        if (crdtIdEquals(curr->parent_id, parent_id)) {
            if (crdtIdCmp(new_id, curr->id) > 0) break;
        } else {
            crdtListVertex *p = crdtListLookupVertex(list, curr->parent_id);
            if (!crdtListIsAncestor(list, p, parent)) break;
        }
        curr = curr->next;
    }

    /* Splice vertex */
    crdtListVertex *v = zmalloc(sizeof(*v));
    v->id = new_id;
    v->parent_id = parent_id;
    v->val = val;
    v->deleted = 0;
    v->del_hlc = 0;
    v->del_origin = 0;
    v->parent = parent;

    v->prev = curr->prev;
    v->next = curr;
    curr->prev->next = v;
    curr->prev = v;

    crdtListIndexInsert(list, new_id, v);
    list->length++;
    list->total_vertices++;
    return v;
}
```

---

## 3. Engine Integration & Command Interception Layer

### 3.1 Object Encoding (`OBJ_ENCODING_CRDT_LIST`)
- Registered encoding `OBJ_ENCODING_CRDT_LIST = 15` in `src/server.h`.
- When `active-active yes` is enabled in `valkey.conf`, list creation defaults to `OBJ_ENCODING_CRDT_LIST`.
- Legacy `OBJ_ENCODING_LISTPACK` or `OBJ_ENCODING_QUICKLIST` structures are dynamically converted to CRDT lists on demand via `listTypeConvertToCrdt()`.

### 3.2 Transparent Native Command Interception
All standard Valkey list commands in [`src/t_list.c`](file:///usr/local/google/home/nandihalli/experiment/active-active/valkey/src/t_list.c) are intercepted:

| Client RESP Command | Internal CRDT Execution | Emitted Replication Frame |
|---|---|---|
| `LPUSH key val` | Insert after `head` with new HLC | `CRDT.LINSERT key 0 0 <hlc> <node> val` |
| `RPUSH key val` | Insert after current tail vertex with new HLC | `CRDT.LINSERT key <tail_hlc> <tail_node> <hlc> <node> val` |
| `LPOP key [count]` | Mark visible head vertex as deleted | `CRDT.LDELETE key <id_hlc> <id_node> <del_hlc> <del_node>` |
| `RPOP key [count]` | Mark visible tail vertex as deleted | `CRDT.LDELETE key <id_hlc> <id_node> <del_hlc> <del_node>` |
| `LINSERT key BEFORE\|AFTER pivot val` | Locate pivot vertex, insert with new HLC | `CRDT.LINSERT key <anchor_hlc> <anchor_node> <hlc> <node> val` |
| `LSET key idx val` | Delete vertex at visible index, insert child | `CRDT.LDELETE` + `CRDT.LINSERT` |
| `LREM key count val` | Mark matching visible vertices as deleted | `CRDT.LDELETE` |
| `LRANGE key start stop` | Traverse non-deleted vertices via `rax` | None (Local read) |
| `LINDEX key idx` | Seek visible index in $O(K)$ | None (Local read) |
| `LLEN key` | Returns `crdtList->length` in $O(1)$ | None (Local read) |

---

## 4. Replication, Buffering & Non-Destructive State Sync

```
+---------------------------------------------------------------------------------------------------+
|                        REPLICATION & RECOVERY STATE MACHINE                                       |
|                                                                                                   |
|   [Steady-State Delta Streaming]                                                                  |
|   Primary A ──(CRDT.LINSERT / CRDT.LDELETE)──> Primary B                                          |
|                                                                                                   |
|   [WAN Congestion / Slow Replica]                                                                 |
|   Primary A Output Buffer > active-active-peer-buffer-limit (e.g. 64MB)                           |
|       ├── Sever peer connection (Protect Primary A RAM)                                           |
|       └── Local client writes continue unblocked at <1ms                                          |
|                                                                                                   |
|   [Partition Recovery & Reconnect]                                                                |
|   Primary B Reconnects ──(PSYNC_AA)──> Backlog Overwritten                                        |
|       ├── Fallback to FULLRESYNC_MERGE (RDB stream)                                               |
|       ├── Receiver BYPASSES emptyDb() (Preserves local concurrent writes)                         |
|       └── Key-by-key in-place crdtListMerge() (Join-Semilattice S_local ⊔ S_remote)               |
+---------------------------------------------------------------------------------------------------+
```

### 4.1 Non-Destructive Snapshot Merge (`FULLRESYNC_MERGE`)
- In standard Valkey, receiving an RDB snapshot calls `emptyDb()`, permanently deleting local concurrent data.
- In Active-Active Valkey:
  1. `RDBFLAGS_EMPTY_DATA` is omitted when `server.active_active_enabled` is true.
  2. The streaming RDB parser in [`src/rdb.c`](file:///usr/local/google/home/nandihalli/experiment/active-active/valkey/src/rdb.c) detects existing `OBJ_ENCODING_CRDT_LIST` keys.
  3. Executes **Join-Semilattice Merge ($S_{\text{local}} \sqcup S_{\text{remote}}$)** via `crdtListMerge()`:
     - For every remote vertex: if missing locally, insert at its deterministic RGA position.
     - For every remote tombstone: merge deletion metadata using Last-Writer-Wins ($\text{del\_hlc} = \max(\text{local.del\_hlc}, \text{remote.del\_hlc})$).

---

## 5. Dynamic Stability Horizon & Tombstone Garbage Collection

### 5.1 The Zombie Resurrection Vulnerability
If a node deletes an element and frees its memory immediately, an in-flight or partitioned write from a peer primary referencing that deleted element arrives later and is treated as an insertion after an unknown parent, or resurrects the deleted key.

To prevent this, deletions create **Tombstones** ($v.\text{deleted} = 1$). To prevent tombstones from consuming unbounded RAM, the cluster computes a **Dynamic Stability Horizon** ($H_{\text{stable}}^Q$).

### 5.2 Dynamic Quorum Stability Horizon Calculation
Each primary tracks the acknowledgment progress of all active peer primaries:
$$M_{\text{active}} = \{ j \in \text{Peers} \mid (\text{now} - \text{last\_heartbeat}_j) \le \tau_{\text{eject}} \}$$
$$H_{\text{stable}}^Q = \min_{j \in M_{\text{active}}} \Big( \text{last\_acked\_hlc}_j, \ \text{local\_hlc} \Big)$$

- **Straggler Lease Eviction ($\tau_{\text{eject}}$)**: If a peer goes silent for $> \tau_{\text{eject}}$ (`active-active-gc-lease-ms`, default 1 hour), it is temporarily ejected from the GC quorum, allowing surviving nodes to unfreeze tombstone garbage collection.

### 5.3 Descendant-Safe 3-Pass Tombstone Pruning Algorithm
Tombstones with $\text{del\_hlc} \le H_{\text{stable}}^Q$ cannot be blindly freed if they are referenced as parent anchors by live descendant vertices.

The engine executes an $O(N)$ **3-Pass Mark-and-Sweep**:
1. **Pass 1 (Clear)**: Set `v->gc_keep = 0` for all non-sentinel vertices.
2. **Pass 2 (Ancestor Traversal)**: For every vertex $u$ that is live (`!u->deleted`) or unpruned (`u->del_hlc > H_stable`), mark `u->gc_keep = 1` and walk up the `u->parent` chain marking ancestors until reaching an already-marked ancestor or `head`.
3. **Pass 3 (Sweep & Reclaim)**: For every vertex with `gc_keep == 0`:
   - Detach from doubly-linked list (`prev->next = next`, `next->prev = prev`).
   - Remove from Radix tree (`raxRemove(list->index, id, 12, NULL)`).
   - Free memory (`zfree(v)`).

---

## 6. Verification Suite & Test Commands

The implementation is verified by 5 comprehensive test suites:

### 1. C Unit & Invariant Test Binary
```bash
make crdt_list_test && ./tests/unit/crdt_list_test
```
*Evaluates 14 test suites and 682 assertions verifying HLC monotonicity, counter borrowing, skew clamping, commutativity, associativity, idempotence, concurrent delete/insert anchor safety, and 5-node random fuzzing convergence.*

### 2. Command Interception & Compatibility Suite
```bash
./runtest --single unit/crdt_command_interception_test
```
*Validates 15 scenarios: transparent command routing, LPUSH/RPUSH/LPOP/RPOP/LSET/LREM/LTRIM/LMOVE, dynamic conversion from quicklist/listpack, and 1000-op stress mutations.*

### 3. Persistence & FULLRESYNC_MERGE Suite
```bash
./runtest --single unit/crdt_rdb_replication_test
```
*Validates RDB DUMP/RESTORE, server restart persistence, non-destructive snapshot join, concurrent tombstone merges, and peer output buffer limit enforcement.*

### 4. Stability Horizon & GC Suite
```bash
./runtest --single unit/crdt_gc_stability_test
```
*Validates tombstone pruning under Stability Horizon, descendant anchor safety, peer ACK tracking, straggler lease eviction, and 3-node replication mesh simulation.*

### 5. Network Partition & Buffer Overrun Suite
```bash
./runtest --single unit/crdt_partition_buffer_overrun_test
```
*Simulates 2-node severed WAN partition with >100KB conflicting mutations exceeding 16KB ring backlogs (>6x overrun), backlog wrap-around, and bit-for-bit final state convergence via FULLRESYNC_MERGE.*
