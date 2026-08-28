/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Set Engine (Two-Phase LWW-Element-Set)
 */

#include "crdt_set.h"
#include "zmalloc.h"
#include <string.h>
#include <strings.h>

static uint64_t setMemberHash(const void *key) {
    return dictGenHashFunction((const unsigned char *)key, sdslen((char *)key));
}

static int setMemberCompare(const void *key1, const void *key2) {
    size_t l1 = sdslen((sds)key1);
    size_t l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static void setMemberDestructor(void *entry) {
    dictEntry *de = entry;
    sdsfree(dictGetKey(de));
    zfree(dictGetVal(de));
    zfree(de);
}

static dictType crdtSetDictType = {
    .entryGetKey = dictEntryGetKey,
    .hashFunction = setMemberHash,
    .keyCompare = setMemberCompare,
    .entryDestructor = setMemberDestructor,
};

/* Create a new crdtSet instance */
crdtSet *crdtSetCreate(void) {
    crdtSet *cs = zmalloc(sizeof(*cs));
    cs->dict = dictCreate(&crdtSetDictType);
    cs->live_count = 0;
    cs->tombstone_count = 0;
    return cs;
}

/* Free a crdtSet instance */
void crdtSetFree(crdtSet *cs) {
    if (!cs) return;
    if (cs->dict) {
        dictRelease(cs->dict);
    }
    zfree(cs);
}

/* Add member with timestamp. Returns 1 if member transitioned from absent to present, 0 otherwise. */
int crdtSetAdd(crdtSet *cs, sds member, crdtId add_id) {
    if (!cs || !member) return 0;

    crdtSetMember *m = dictFetchValue(cs->dict, member);
    if (!m) {
        m = zmalloc(sizeof(*m));
        m->t_add = add_id.hlc;
        m->add_origin = add_id.origin_id;
        m->t_rem = 0;
        m->rem_origin = 0;

        sds member_copy = sdsdup(member);
        dictAdd(cs->dict, member_copy, m);
        cs->live_count++;
        return 1;
    }

    int was_member = crdtSetIsMember(m);
    /* Update t_add if newer */
    crdtId curr_add = crdtIdMake(m->t_add, m->add_origin);
    if (crdtIdCmp(add_id, curr_add) > 0) {
        m->t_add = add_id.hlc;
        m->add_origin = add_id.origin_id;
    }

    int is_member = crdtSetIsMember(m);
    if (!was_member && is_member) {
        cs->live_count++;
        if (cs->tombstone_count > 0) cs->tombstone_count--;
        return 1;
    }
    return 0;
}

/* Remove member with timestamp. Returns 1 if member transitioned from present to absent, 0 otherwise. */
int crdtSetRemove(crdtSet *cs, sds member, crdtId rem_id) {
    if (!cs || !member) return 0;

    crdtSetMember *m = dictFetchValue(cs->dict, member);
    if (!m) {
        /* Record removal tombstone even for previously unobserved member to handle out-of-order adds */
        m = zmalloc(sizeof(*m));
        m->t_add = 0;
        m->add_origin = 0;
        m->t_rem = rem_id.hlc;
        m->rem_origin = rem_id.origin_id;

        sds member_copy = sdsdup(member);
        dictAdd(cs->dict, member_copy, m);
        cs->tombstone_count++;
        return 0;
    }

    int was_member = crdtSetIsMember(m);
    crdtId curr_rem = crdtIdMake(m->t_rem, m->rem_origin);
    if (crdtIdCmp(rem_id, curr_rem) > 0) {
        m->t_rem = rem_id.hlc;
        m->rem_origin = rem_id.origin_id;
    }

    int is_member = crdtSetIsMember(m);
    if (was_member && !is_member) {
        if (cs->live_count > 0) cs->live_count--;
        cs->tombstone_count++;
        return 1;
    }
    return 0;
}

/* Check if member is present in set */
int crdtSetContains(crdtSet *cs, sds member) {
    if (!cs || !member) return 0;
    crdtSetMember *m = dictFetchValue(cs->dict, member);
    return crdtSetIsMember(m);
}

/* Return visible cardinal size of set */
size_t crdtSetCard(crdtSet *cs) {
    return cs ? cs->live_count : 0;
}

/* Merge two crdtSet instances (S_local ⊔ S_remote) using join-semilattice.
 * Returns 1 if local state changed, 0 otherwise. */
int crdtSetMerge(crdtSet *local, crdtSet *remote) {
    if (!local || !remote) return 0;

    int changed = 0;
    dictIterator *di = dictGetSafeIterator(remote->dict);
    dictEntry *de;

    while ((de = dictNext(di)) != NULL) {
        sds member = dictGetKey(de);
        crdtSetMember *r_m = dictGetVal(de);
        crdtSetMember *l_m = dictFetchValue(local->dict, member);

        if (!l_m) {
            l_m = zmalloc(sizeof(*l_m));
            l_m->t_add = r_m->t_add;
            l_m->add_origin = r_m->add_origin;
            l_m->t_rem = r_m->t_rem;
            l_m->rem_origin = r_m->rem_origin;

            sds member_copy = sdsdup(member);
            dictAdd(local->dict, member_copy, l_m);
            if (crdtSetIsMember(l_m)) {
                local->live_count++;
            } else {
                local->tombstone_count++;
            }
            changed = 1;
        } else {
            int was_member = crdtSetIsMember(l_m);
            crdtId r_add = crdtIdMake(r_m->t_add, r_m->add_origin);
            crdtId l_add = crdtIdMake(l_m->t_add, l_m->add_origin);
            if (crdtIdCmp(r_add, l_add) > 0) {
                l_m->t_add = r_m->t_add;
                l_m->add_origin = r_m->add_origin;
                changed = 1;
            }

            crdtId r_rem = crdtIdMake(r_m->t_rem, r_m->rem_origin);
            crdtId l_rem = crdtIdMake(l_m->t_rem, l_m->rem_origin);
            if (crdtIdCmp(r_rem, l_rem) > 0) {
                l_m->t_rem = r_m->t_rem;
                l_m->rem_origin = r_m->rem_origin;
                changed = 1;
            }

            int is_member = crdtSetIsMember(l_m);
            if (!was_member && is_member) {
                local->live_count++;
                if (local->tombstone_count > 0) local->tombstone_count--;
            } else if (was_member && !is_member) {
                if (local->live_count > 0) local->live_count--;
                local->tombstone_count++;
            }
        }
    }
    dictReleaseIterator(di);
    return changed;
}

/* Prune tombstones whose t_rem <= h_stable */
size_t crdtSetPruneTombstones(crdtSet *cs, uint64_t h_stable) {
    if (!cs || cs->tombstone_count == 0) return 0;

    size_t pruned = 0;
    dictIterator *di = dictGetSafeIterator(cs->dict);
    dictEntry *de;

    while ((de = dictNext(di)) != NULL) {
        crdtSetMember *m = dictGetVal(de);
        if (!crdtSetIsMember(m) && m->t_rem <= h_stable) {
            sds member = dictGetKey(de);
            dictDelete(cs->dict, member);
            if (cs->tombstone_count > 0) cs->tombstone_count--;
            pruned++;
        }
    }
    dictReleaseIterator(di);
    return pruned;
}
