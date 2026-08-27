/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Replicated Growable Array (RGA) List Engine
 *
 * Implements a state-based / operation-based Replicated Growable Array (RGA)
 * sequence CRDT with deterministic tie-breaking, tombstone deletion semantics,
 * and O(log N) vertex lookup via Radix Tree (rax).
 */

#ifndef CRDT_LIST_H
#define CRDT_LIST_H

#include "crdt_clock.h"
#include "rax.h"
#include "sds.h"
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Individual RGA List Vertex */
typedef struct crdtListVertex {
    crdtId id;                     /* Unique identifier of this vertex (HLC + origin) */
    crdtId parent_id;              /* Predecessor vertex ID at insertion time ({0,0} for head) */
    sds val;                       /* Payload value (SDS dynamic string, NULL if sentinel or freed) */
    int deleted;                   /* Tombstone flag: 1 if deleted, 0 if visible */
    uint64_t del_hlc;              /* Deletion HLC timestamp */
    uint32_t del_origin;           /* Deletion origin node ID */
    struct crdtListVertex *prev;   /* Predecessor in the doubly-linked sequence */
    struct crdtListVertex *next;   /* Successor in the doubly-linked sequence */
    struct crdtListVertex *parent; /* Direct parent pointer in insertion tree */
    uint8_t gc_keep;               /* GC mark flag: 1 = do not prune, 0 = prunable */
} crdtListVertex;

/* RGA CRDT List Container */
typedef struct crdtList {
    crdtListVertex *head;          /* Sentinel root vertex ({0,0}) */
    crdtListVertex *tail;          /* Tail vertex in doubly-linked list */
    rax *index;                    /* Radix tree index: 12-byte crdtId -> crdtListVertex* */
    size_t length;                 /* Count of visible (non-deleted) items */
    size_t tombstone_count;        /* Count of tombstoned items */
    size_t total_vertices;         /* Total vertex count (excluding sentinel) */
} crdtList;

/* Create an empty CRDT List */
crdtList *crdtListCreate(void);

/* Free an entire CRDT List and all its vertices */
void crdtListRelease(crdtList *list);

/* Find a vertex by its crdtId in O(log N) time using the rax index.
 * Returns pointer to vertex or NULL if not found. */
crdtListVertex *crdtListFindVertex(crdtList *list, crdtId id);

/* Insert a new vertex with value `val` immediately after `parent_id`.
 * Implements deterministic RGA sibling ordering:
 * - Higher HLC comes first.
 * - If HLC is identical, higher origin_id comes first.
 * - Respects descendant subtree boundaries.
 * Idempotent: If `new_id` already exists, returns the existing vertex without duplicating.
 * Returns the created or existing vertex, or NULL on error. */
crdtListVertex *crdtListInsertAfter(crdtList *list, crdtId parent_id, crdtId new_id, sds val);

/* Mark a vertex as deleted (tombstone).
 * Uses Last-Writer-Wins (LWW) to resolve concurrent delete metadata updates.
 * Returns:
 *   1 if vertex was newly deleted.
 *   2 if vertex was already deleted (and tombstone metadata updated/idempotent).
 *   0 if vertex was not found. */
int crdtListDeleteVertex(crdtList *list, crdtId id, uint64_t del_hlc, uint32_t del_origin);

/* Return the visible length (count of non-deleted items) of the list */
size_t crdtListLength(crdtList *list);

/* Get vertex at a specific 0-based visible index.
 * Supports negative indices: -1 is the last visible item, -2 is second to last, etc.
 * Returns NULL if out of bounds or list is empty. */
crdtListVertex *crdtListGetVisibleIndex(crdtList *list, long index);

/* Get range of visible items between start and end (inclusive).
 * Supports negative indices (e.g. 0 to -1 returns all visible items).
 * Returns a dynamically allocated array of cloned sds strings (length placed in *out_len).
 * The returned array should be freed with crdtListFreeRange(). */
sds *crdtListGetVisibleRange(crdtList *list, long start, long end, size_t *out_len);

/* Free the range array returned by crdtListGetVisibleRange */
void crdtListFreeRange(sds *items, size_t len);

/* Join-Semilattice Merge: Merges `remote` into `local` state (local = local ⊔ remote).
 * Guarantees commutativity, associativity, and idempotence.
 * Returns 0 on success, or -1 on error. */
int crdtListMerge(crdtList *local, crdtList *remote);

/* High-level list operation helpers */
crdtListVertex *crdtListPushHead(crdtList *list, crdtClock *clock, sds val);
crdtListVertex *crdtListPushTail(crdtList *list, crdtClock *clock, sds val);
int crdtListPopHead(crdtList *list, crdtClock *clock, sds *out_val);
int crdtListPopTail(crdtList *list, crdtClock *clock, sds *out_val);

/* Clone a CRDT list into a new independent list */
crdtList *crdtListClone(crdtList *list);

/* Compare two CRDT lists for structural and semantic equality (returns 1 if identical, 0 otherwise) */
int crdtListEquals(crdtList *a, crdtList *b);

/* Prune tombstones that have del_hlc <= h_stable and have no active or unpruned descendants.
 * Returns the number of tombstones pruned and freed. */
size_t crdtListPruneTombstones(crdtList *list, uint64_t h_stable);

#ifdef __cplusplus
}
#endif

#endif /* CRDT_LIST_H */
