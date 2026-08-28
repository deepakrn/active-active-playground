/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Hierarchical Key Tombstone & Keyspace Deletion Subsystem
 */

#include "crdt_tombstone.h"
#include "zmalloc.h"
#include <string.h>
#include <strings.h>

static uint64_t tombstoneKeyHash(const void *key) {
    return dictGenHashFunction((const unsigned char *)key, sdslen((char *)key));
}

static int tombstoneKeyCompare(const void *key1, const void *key2) {
    size_t l1 = sdslen((sds)key1);
    size_t l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static void tombstoneEntryDestructor(void *entry) {
    dictEntry *de = entry;
    sdsfree(dictGetKey(de));
    zfree(dictGetVal(de));
    zfree(de);
}

static dictType crdtKeyTombstoneDictType = {
    .entryGetKey = dictEntryGetKey,
    .hashFunction = tombstoneKeyHash,
    .keyCompare = tombstoneKeyCompare,
    .entryDestructor = tombstoneEntryDestructor,
};

/* Record or update an explicit key-level deletion tombstone with LWW semantics */
int crdtKeyTombstoneRecord(dict *tombstones, sds key, crdtId del_id, mstime_t now) {
    if (!tombstones || !key) return 0;

    crdtKeyTombstone *existing = crdtKeyTombstoneLookup(tombstones, key);
    if (existing) {
        /* LWW update on key tombstone */
        if (crdtIdCmp(del_id, existing->del_id) > 0) {
            existing->del_id = del_id;
            existing->deleted_at = now;
            return 1;
        }
        return 0; /* Stale deletion */
    }

    crdtKeyTombstone *t = zmalloc(sizeof(*t));
    t->del_id = del_id;
    t->deleted_at = now;

    sds key_copy = sdsdup(key);
    dictAdd(tombstones, key_copy, t);
    return 1;
}

/* Lookup key tombstone in dictionary */
crdtKeyTombstone *crdtKeyTombstoneLookup(dict *tombstones, sds key) {
    if (!tombstones || !key) return NULL;
    return dictFetchValue(tombstones, key);
}

/* Check if a write with given crdtId is permitted against key tombstones (Hierarchical Survival Rule).
 * Returns 1 if permitted (no tombstone or write is newer than tombstone), 0 if masked by tombstone. */
int crdtKeyCanMutate(dict *tombstones, sds key, crdtId write_id) {
    if (!tombstones || !key) return 1;
    crdtKeyTombstone *t = crdtKeyTombstoneLookup(tombstones, key);
    if (!t) return 1;
    return (crdtIdCmp(write_id, t->del_id) > 0);
}

/* Prune tombstones whose del_id.hlc <= h_stable */
size_t crdtKeyTombstonePrune(dict *tombstones, uint64_t h_stable) {
    if (!tombstones || dictSize(tombstones) == 0) return 0;

    size_t pruned = 0;
    dictIterator *di = dictGetSafeIterator(tombstones);
    dictEntry *de;

    while ((de = dictNext(di)) != NULL) {
        crdtKeyTombstone *t = dictGetVal(de);
        if (t && t->del_id.hlc <= h_stable) {
            sds key = dictGetKey(de);
            dictDelete(tombstones, key);
            pruned++;
        }
    }
    dictReleaseIterator(di);
    return pruned;
}

/* Dictionary type helpers for key tombstones */
dict *crdtKeyTombstoneDictCreate(void) {
    return dictCreate(&crdtKeyTombstoneDictType);
}

void crdtKeyTombstoneDictRelease(dict *tombstones) {
    if (tombstones) {
        dictRelease(tombstones);
    }
}
