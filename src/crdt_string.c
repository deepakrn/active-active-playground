/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT String Engine (HLC-LWW Register)
 */

#include "crdt_string.h"
#include "zmalloc.h"

/* Create a new crdtString instance */
crdtString *crdtStringCreate(crdtId id, sds val) {
    crdtString *cs = zmalloc(sizeof(*cs));
    cs->id = id;
    cs->val = val ? sdsdup(val) : sdsempty();
    return cs;
}

/* Free a crdtString instance */
void crdtStringFree(crdtString *cs) {
    if (!cs) return;
    if (cs->val) sdsfree(cs->val);
    zfree(cs);
}

/* Update string with new value using HLC-LWW conflict resolution.
 * Returns 1 if updated (winner), 0 if ignored (stale). */
int crdtStringUpdate(crdtString *cs, crdtId new_id, sds new_val) {
    if (!cs) return 0;

    if (crdtIdCmp(new_id, cs->id) > 0) {
        cs->id = new_id;
        sdsfree(cs->val);
        cs->val = new_val ? sdsdup(new_val) : sdsempty();
        return 1;
    }
    return 0;
}

/* Append to string value and assign new HLC */
int crdtStringAppend(crdtString *cs, crdtId new_id, sds append_val) {
    if (!cs || !append_val) return 0;
    cs->id = new_id;
    cs->val = sdscatsds(cs->val, append_val);
    return 1;
}

/* Merge two crdtString instances (S_local ⊔ S_remote) using join-semilattice LWW.
 * Returns 1 if local state changed, 0 otherwise. */
int crdtStringMerge(crdtString *local, crdtString *remote) {
    if (!local || !remote) return 0;

    if (crdtIdCmp(remote->id, local->id) > 0) {
        local->id = remote->id;
        sdsfree(local->val);
        local->val = sdsdup(remote->val);
        return 1;
    }
    return 0;
}
