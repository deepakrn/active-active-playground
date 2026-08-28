/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Counter Engine (Origin-Partitioned PN-Counter with 128-bit Scaled Fixed-Point)
 */

#include "crdt_counter.h"
#include "zmalloc.h"
#include <math.h>

#define INITIAL_ORIGINS_CAP 4

/* Helper to find or allocate an origin counter entry */
static crdtOriginCounter *getOrAllocOrigin(crdtCounter *c, uint32_t origin_id) {
    for (size_t i = 0; i < c->num_origins; i++) {
        if (c->entries[i].origin_id == origin_id) {
            return &c->entries[i];
        }
    }

    if (c->num_origins >= c->capacity) {
        size_t new_cap = c->capacity ? c->capacity * 2 : INITIAL_ORIGINS_CAP;
        c->entries = zrealloc(c->entries, new_cap * sizeof(crdtOriginCounter));
        c->capacity = new_cap;
    }

    crdtOriginCounter *entry = &c->entries[c->num_origins++];
    entry->origin_id = origin_id;
    entry->pos = 0;
    entry->neg = 0;
    entry->pos_fp = 0;
    entry->neg_fp = 0;
    return entry;
}

/* Create a new crdtCounter instance */
crdtCounter *crdtCounterCreate(int is_float) {
    crdtCounter *c = zmalloc(sizeof(*c));
    c->is_float = is_float;
    c->num_origins = 0;
    c->capacity = INITIAL_ORIGINS_CAP;
    c->entries = zmalloc(c->capacity * sizeof(crdtOriginCounter));
    return c;
}

/* Free a crdtCounter instance */
void crdtCounterFree(crdtCounter *c) {
    if (!c) return;
    if (c->entries) zfree(c->entries);
    zfree(c);
}

/* Increment integer counter by delta */
int crdtCounterIncr(crdtCounter *c, uint32_t origin_id, int64_t delta) {
    if (!c) return 0;
    crdtOriginCounter *entry = getOrAllocOrigin(c, origin_id);
    if (delta >= 0) {
        entry->pos += (uint64_t)delta;
    } else if (delta == INT64_MIN) {
        entry->neg += 9223372036854775808ULL;
    } else {
        entry->neg += (uint64_t)(-delta);
    }
    return 1;
}

/* Increment float counter by delta */
int crdtCounterIncrFloat(crdtCounter *c, uint32_t origin_id, double delta) {
    if (!c) return 0;
    c->is_float = 1;
    crdtOriginCounter *entry = getOrAllocOrigin(c, origin_id);

    /* Convert double to 128-bit scaled fixed point */
    double scaled = delta * (double)CRDT_FLOAT_SCALE;
    crdt_int128 delta_fp = (crdt_int128)(scaled >= 0 ? (scaled + 0.5) : (scaled - 0.5));

    if (delta_fp >= 0) {
        entry->pos_fp += delta_fp;
    } else {
        entry->neg_fp += (-delta_fp);
    }
    return 1;
}

/* Evaluate current integer value */
int64_t crdtCounterValue(const crdtCounter *c) {
    if (!c) return 0;
    int64_t total = 0;
    for (size_t i = 0; i < c->num_origins; i++) {
        total += (c->entries[i].pos - c->entries[i].neg);
    }
    return total;
}

/* Evaluate current float value */
double crdtCounterValueFloat(const crdtCounter *c) {
    if (!c) return 0.0;
    crdt_int128 total_fp = 0;
    for (size_t i = 0; i < c->num_origins; i++) {
        total_fp += (c->entries[i].pos_fp - c->entries[i].neg_fp);
    }
    return (double)total_fp / (double)CRDT_FLOAT_SCALE;
}

/* Merge two crdtCounter instances using component-wise max */
int crdtCounterMerge(crdtCounter *local, const crdtCounter *remote) {
    if (!local || !remote) return 0;
    int changed = 0;

    if (remote->is_float) local->is_float = 1;

    for (size_t i = 0; i < remote->num_origins; i++) {
        const crdtOriginCounter *r = &remote->entries[i];
        crdtOriginCounter *l = getOrAllocOrigin(local, r->origin_id);

        if (r->pos > l->pos) {
            l->pos = r->pos;
            changed = 1;
        }
        if (r->neg > l->neg) {
            l->neg = r->neg;
            changed = 1;
        }
        if (r->pos_fp > l->pos_fp) {
            l->pos_fp = r->pos_fp;
            changed = 1;
        }
        if (r->neg_fp > l->neg_fp) {
            l->neg_fp = r->neg_fp;
            changed = 1;
        }
    }
    return changed;
}
