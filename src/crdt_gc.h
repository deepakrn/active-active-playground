/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Anti-Entropy & Stability Horizon Tombstone Garbage Collection Subsystem
 */

#ifndef CRDT_GC_H
#define CRDT_GC_H

#include "crdt_clock.h"
#include "crdt_list.h"
#include "sds.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef long long mstime_t;

/* Peer tracking state for dynamic stability horizon computation */
typedef struct crdtPeerState {
    uint32_t node_id;
    uint64_t last_acked_hlc;
    mstime_t last_heartbeat_time;
    int is_ejected;               /* 1 if temporarily ejected due to lease expiry */
    int is_active;                /* 1 if registered and active */
} crdtPeerState;

/* GC statistics */
typedef struct crdtGcStats {
    unsigned long long tombstones_collected;
    unsigned long long gc_sweeps;
    mstime_t last_sweep_time;
    uint64_t last_h_stable;
    unsigned long long straggler_ejections;
} crdtGcStats;

/* Forward declaration */
struct client;

/* Initialize and reset CRDT GC subsystem */
void crdtGcInit(void);
void crdtGcReset(void);

/* Peer tracking functions */
int crdtPeerTrackAck(uint32_t peer_id, uint64_t acked_hlc, mstime_t now);
int crdtPeerHeartbeat(uint32_t peer_id, mstime_t now);
int crdtPeerRegister(uint32_t peer_id, uint64_t initial_hlc);
int crdtPeerUnregister(uint32_t peer_id);
int crdtPeerEject(uint32_t peer_id);
crdtPeerState *crdtPeerFind(uint32_t peer_id);

/* Stability Horizon Computation:
 * H_stable^Q = min_{j in M_active} (last_acked_hlc_j) */
uint64_t crdtComputeStabilityHorizon(void);

/* Quorum metrics */
size_t crdtGetTrackedPeerCount(void);
size_t crdtGetActiveQuorumCount(void);
size_t crdtGetEjectedPeerCount(void);

/* Database-wide & Cron GC Sweeps */
size_t crdtDatabaseGCSweep(int dbid, uint64_t h_stable, size_t max_keys);
size_t crdtAllDatabasesGCSweep(uint64_t h_stable);
void crdtCronGCSweep(void);

/* Info string generator */
sds genCrdtInfoString(sds info);

/* Commands */
void crdtGcCommand(struct client *c);
void crdtStabilityCommand(struct client *c);

#ifdef __cplusplus
}
#endif

#endif /* CRDT_GC_H */
