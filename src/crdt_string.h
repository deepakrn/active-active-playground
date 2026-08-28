/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT String Engine (HLC-LWW Register)
 */

#ifndef CRDT_STRING_H
#define CRDT_STRING_H

#include "crdt_clock.h"
#include "sds.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* CRDT String Container: Last-Writer-Wins Register */
typedef struct crdtString {
    crdtId id;  /* 64-bit HLC timestamp + 32-bit node origin ID */
    sds val;    /* SDS payload */
} crdtString;

/* Create a new crdtString instance */
crdtString *crdtStringCreate(crdtId id, sds val);

/* Free a crdtString instance */
void crdtStringFree(crdtString *cs);

/* Update string with new value using HLC-LWW conflict resolution.
 * Returns 1 if updated (winner), 0 if ignored (stale). */
int crdtStringUpdate(crdtString *cs, crdtId new_id, sds new_val);

/* Append to string value and assign new HLC */
int crdtStringAppend(crdtString *cs, crdtId new_id, sds append_val);

/* Merge two crdtString instances (S_local ⊔ S_remote) using join-semilattice LWW.
 * Returns 1 if local state changed, 0 otherwise. */
int crdtStringMerge(crdtString *local, crdtString *remote);

#ifdef __cplusplus
}
#endif

#endif /* CRDT_STRING_H */
