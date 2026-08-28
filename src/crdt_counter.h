/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Counter Engine (Origin-Partitioned PN-Counter with 128-bit Scaled Fixed-Point)
 */

#ifndef CRDT_COUNTER_H
#define CRDT_COUNTER_H

#include "crdt_clock.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CRDT_FLOAT_SCALE 1000000000000000000ULL /* 10^18 */
typedef __int128_t crdt_int128;

/* Origin-scoped positive/negative accumulators */
typedef struct crdtOriginCounter {
    uint32_t origin_id;
    uint64_t pos;         /* Integer positive increments */
    uint64_t neg;         /* Integer negative decrements */
    crdt_int128 pos_fp;   /* Fixed-point float positive increments (scaled by 10^18) */
    crdt_int128 neg_fp;   /* Fixed-point float negative decrements (scaled by 10^18) */
} crdtOriginCounter;

/* CRDT Counter Container */
typedef struct crdtCounter {
    int is_float;         /* 1 if storing scaled fixed-point, 0 if integer */
    size_t num_origins;
    size_t capacity;
    crdtOriginCounter *entries;
} crdtCounter;

/* Create a new crdtCounter instance */
crdtCounter *crdtCounterCreate(int is_float);

/* Free a crdtCounter instance */
void crdtCounterFree(crdtCounter *c);

/* Increment integer counter by delta */
int crdtCounterIncr(crdtCounter *c, uint32_t origin_id, int64_t delta);

/* Increment float counter by delta */
int crdtCounterIncrFloat(crdtCounter *c, uint32_t origin_id, double delta);

/* Evaluate current integer value */
int64_t crdtCounterValue(const crdtCounter *c);

/* Evaluate current float value */
double crdtCounterValueFloat(const crdtCounter *c);

/* Merge two crdtCounter instances using component-wise max */
int crdtCounterMerge(crdtCounter *local, const crdtCounter *remote);

#ifdef __cplusplus
}
#endif

#endif /* CRDT_COUNTER_H */
