/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Hierarchical Key Tombstone & Keyspace Deletion Subsystem
 */

#ifndef CRDT_TOMBSTONE_H
#define CRDT_TOMBSTONE_H

#include "crdt_clock.h"
#include "sds.h"
#include "dict.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef long long mstime_t;

/* Key tombstone metadata representing explicit keyspace deletions (DEL / UNLINK) */
typedef struct crdtKeyTombstone {
    crdtId del_id;        /* HLC timestamp and origin node ID of the deletion */
    mstime_t deleted_at;  /* Wall-clock timestamp when tombstone was recorded */
} crdtKeyTombstone;

/* Forward declarations */
struct redisDb;
struct robj;

/* Record or update an explicit key-level deletion tombstone with LWW semantics */
int crdtKeyTombstoneRecord(dict *tombstones, sds key, crdtId del_id, mstime_t now);

/* Lookup key tombstone in dictionary */
crdtKeyTombstone *crdtKeyTombstoneLookup(dict *tombstones, sds key);

/* Check if a write with given crdtId is permitted against key tombstones (Hierarchical Survival Rule).
 * Returns 1 if permitted (no tombstone or write is newer than tombstone), 0 if masked by tombstone. */
int crdtKeyCanMutate(dict *tombstones, sds key, crdtId write_id);

/* Prune tombstones whose del_id.hlc <= h_stable */
size_t crdtKeyTombstonePrune(dict *tombstones, uint64_t h_stable);

/* Dictionary type helpers for key tombstones */
dict *crdtKeyTombstoneDictCreate(void);
void crdtKeyTombstoneDictRelease(dict *tombstones);

#ifdef __cplusplus
}
#endif

#endif /* CRDT_TOMBSTONE_H */
