/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Hybrid Logical Clock (HLC) Engine
 *
 * Provides high-performance 64-bit Hybrid Logical Clocks combining a 48-bit
 * physical millisecond timestamp and a 16-bit monotonic logical counter,
 * paired with a 32-bit replica/node origin identifier (crdtId).
 *
 * Features:
 * - Counter saturation forward physical time borrowing with backpressure signalling.
 * - Strict future clock drift clamping on remote message receipt.
 * - Deterministic total order comparison and big-endian binary encoding for index keys.
 */

#ifndef CRDT_CLOCK_H
#define CRDT_CLOCK_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* HLC Bit Layout Masks and Shifts:
 * Bits [63:16]: 48-bit physical time in milliseconds (~8925 years range).
 * Bits [15:0]:  16-bit logical sequence counter (up to 65,535 operations per ms).
 */
#define CRDT_HLC_PHYSICAL_MASK   0x0000FFFFFFFFFFFFULL
#define CRDT_HLC_LOGICAL_MASK    0x000000000000FFFFULL
#define CRDT_HLC_PHYSICAL_SHIFT  16
#define CRDT_HLC_MAX_LOGICAL     0xFFFFU

/* Default clock drift and borrowing thresholds */
#define CRDT_DEFAULT_MAX_BORROW_MS  5000ULL   /* Max forward physical borrow before backpressure */
#define CRDT_DEFAULT_MAX_SKEW_MS    10000ULL  /* Max future clock drift tolerated from remote */

/* Status return codes */
#define CRDT_CLOCK_OK            0
#define CRDT_CLOCK_BACKPRESSURE  1
#define CRDT_CLOCK_ERR_SKEW     -1
#define CRDT_CLOCK_ERR_INVALID  -2

/* Composite CRDT Identifier: 64-bit HLC + 32-bit Node Origin ID */
typedef struct crdtId {
    uint64_t hlc;       /* (physical_ms << 16) | (logical & 0xFFFF) */
    uint32_t origin_id; /* Node / replica unique identifier */
} crdtId;

/* CRDT Hybrid Logical Clock state */
typedef struct crdtClock {
    uint64_t last_physical_ms; /* 48-bit physical millisecond baseline */
    uint16_t logical_counter;  /* 16-bit logical counter */
    uint32_t origin_id;        /* Node / replica origin ID */
    uint64_t max_borrow_ms;    /* Max physical time forward borrow threshold (ms) */
    uint64_t max_skew_ms;      /* Max allowable remote clock skew / future drift (ms) */
} crdtClock;

typedef struct crdtClock crdtClockState;

/* Initialize a CRDT HLC clock instance */
void crdtClockInit(crdtClock *clock, uint32_t origin_id, uint64_t max_borrow_ms, uint64_t max_skew_ms);

/* Get current physical wall-clock time in milliseconds */
uint64_t crdtClockPhysicalNowMs(void);

/* Decompose and compose HLC components */
static inline uint64_t crdtHlcCompose(uint64_t physical_ms, uint16_t logical) {
    return ((physical_ms & CRDT_HLC_PHYSICAL_MASK) << CRDT_HLC_PHYSICAL_SHIFT) |
           (uint64_t)(logical & CRDT_HLC_MAX_LOGICAL);
}

static inline uint64_t crdtHlcPhysical(uint64_t hlc) {
    return (hlc >> CRDT_HLC_PHYSICAL_SHIFT) & CRDT_HLC_PHYSICAL_MASK;
}

static inline uint16_t crdtHlcLogical(uint64_t hlc) {
    return (uint16_t)(hlc & CRDT_HLC_LOGICAL_MASK);
}

static inline crdtId crdtIdMake(uint64_t hlc, uint32_t origin_id) {
    crdtId id;
    id.hlc = hlc;
    id.origin_id = origin_id;
    return id;
}

/* Compare two crdtId structs for deterministic total ordering.
 * Returns:
 *   > 0 if a > b (a has higher HLC, or equal HLC with higher origin_id)
 *   < 0 if a < b (a has lower HLC, or equal HLC with lower origin_id)
 *     0 if a == b (identical HLC and origin_id)
 */
static inline int crdtIdCmp(crdtId a, crdtId b) {
    if (a.hlc > b.hlc) return 1;
    if (a.hlc < b.hlc) return -1;
    if (a.origin_id > b.origin_id) return 1;
    if (a.origin_id < b.origin_id) return -1;
    return 0;
}

static inline int crdtIdEqual(crdtId a, crdtId b) {
    return (a.hlc == b.hlc && a.origin_id == b.origin_id);
}

/* Big-endian binary serialization of crdtId (12 bytes total: 8 bytes HLC, 4 bytes origin).
 * Suitable as exact lexicographically ordered keys in radix trees (rax). */
void crdtIdEncode(crdtId id, unsigned char out[12]);
crdtId crdtIdDecode(const unsigned char in[12]);

/* Generate next local HLC timestamp (hlc_now).
 * If current_physical_ms is 0, retrieves current physical time automatically.
 * Handles counter saturation forward physical borrowing (up to max_borrow_ms).
 * Sets backpressure_warn to 1 if forward borrow exceeds max_borrow_ms.
 * Returns CRDT_CLOCK_OK (0) or CRDT_CLOCK_BACKPRESSURE (1). */
int hlc_now(crdtClock *clock, uint64_t current_physical_ms, crdtId *out_id, int *backpressure_warn);

/* Update local clock upon receiving a remote HLC timestamp (hlc_recv).
 * If current_physical_ms is 0, retrieves current physical time automatically.
 * Strictly checks remote clock drift against max_skew_ms.
 * Returns CRDT_CLOCK_OK (0) on success, or CRDT_CLOCK_ERR_SKEW (-1) if remote timestamp exceeds skew threshold. */
int hlc_recv(crdtClock *clock, uint64_t current_physical_ms, crdtId remote_id);

#ifdef __cplusplus
}
#endif

#endif /* CRDT_CLOCK_H */
