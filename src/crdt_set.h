/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Set Engine (Two-Phase LWW-Element-Set)
 */

#ifndef CRDT_SET_H
#define CRDT_SET_H

#include "crdt_clock.h"
#include "sds.h"
#include "dict.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Set member metadata */
typedef struct crdtSetMember {
    uint64_t t_add;       /* HLC timestamp of addition */
    uint32_t add_origin;  /* Origin node ID of addition */
    uint64_t t_rem;       /* HLC timestamp of removal */
    uint32_t rem_origin;  /* Origin node ID of removal */
} crdtSetMember;

/* CRDT Set Container */
typedef struct crdtSet {
    dict *dict;             /* maps sds member -> crdtSetMember* */
    size_t live_count;      /* cached count of active members (t_add > t_rem) */
    size_t tombstone_count; /* count of inactive members (t_rem >= t_add) */
} crdtSet;

/* Create a new crdtSet instance */
crdtSet *crdtSetCreate(void);

/* Free a crdtSet instance */
void crdtSetFree(crdtSet *cs);

/* Membership predicate. Returns 1 if active member, 0 if deleted/tombstone. */
static inline int crdtSetIsMember(const crdtSetMember *m) {
    if (!m) return 0;
    if (m->t_add > m->t_rem) return 1;
    if (m->t_add < m->t_rem) return 0;
    return (m->add_origin > m->rem_origin); /* Deterministic tie-breaker */
}

/* Add member with timestamp. Returns 1 if member transitioned from absent to present, 0 otherwise. */
int crdtSetAdd(crdtSet *cs, sds member, crdtId add_id);

/* Remove member with timestamp. Returns 1 if member transitioned from present to absent, 0 otherwise. */
int crdtSetRemove(crdtSet *cs, sds member, crdtId rem_id);

/* Check if member is present in set */
int crdtSetContains(crdtSet *cs, sds member);

/* Return visible cardinal size of set */
size_t crdtSetCard(crdtSet *cs);

/* Merge two crdtSet instances (S_local ⊔ S_remote) using join-semilattice.
 * Returns 1 if local state changed, 0 otherwise. */
int crdtSetMerge(crdtSet *local, crdtSet *remote);

/* Prune tombstones whose t_rem <= h_stable */
size_t crdtSetPruneTombstones(crdtSet *cs, uint64_t h_stable);

#ifdef __cplusplus
}
#endif

#endif /* CRDT_SET_H */
