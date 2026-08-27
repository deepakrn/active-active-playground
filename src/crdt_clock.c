/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "crdt_clock.h"
#include <time.h>
#include <string.h>

uint64_t crdtClockPhysicalNowMs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ((uint64_t)ts.tv_sec * 1000ULL) + ((uint64_t)ts.tv_nsec / 1000000ULL);
}

void crdtClockInit(crdtClock *clock, uint32_t origin_id, uint64_t max_borrow_ms, uint64_t max_skew_ms) {
    if (!clock) return;
    clock->last_physical_ms = 0;
    clock->logical_counter = 0;
    clock->origin_id = origin_id;
    clock->max_borrow_ms = (max_borrow_ms > 0) ? max_borrow_ms : CRDT_DEFAULT_MAX_BORROW_MS;
    clock->max_skew_ms = (max_skew_ms > 0) ? max_skew_ms : CRDT_DEFAULT_MAX_SKEW_MS;
}

void crdtIdEncode(crdtId id, unsigned char out[12]) {
    /* 8 bytes big-endian HLC */
    out[0] = (unsigned char)((id.hlc >> 56) & 0xFF);
    out[1] = (unsigned char)((id.hlc >> 48) & 0xFF);
    out[2] = (unsigned char)((id.hlc >> 40) & 0xFF);
    out[3] = (unsigned char)((id.hlc >> 32) & 0xFF);
    out[4] = (unsigned char)((id.hlc >> 24) & 0xFF);
    out[5] = (unsigned char)((id.hlc >> 16) & 0xFF);
    out[6] = (unsigned char)((id.hlc >> 8) & 0xFF);
    out[7] = (unsigned char)(id.hlc & 0xFF);

    /* 4 bytes big-endian origin_id */
    out[8]  = (unsigned char)((id.origin_id >> 24) & 0xFF);
    out[9]  = (unsigned char)((id.origin_id >> 16) & 0xFF);
    out[10] = (unsigned char)((id.origin_id >> 8) & 0xFF);
    out[11] = (unsigned char)(id.origin_id & 0xFF);
}

crdtId crdtIdDecode(const unsigned char in[12]) {
    crdtId id;
    id.hlc = ((uint64_t)in[0] << 56) |
             ((uint64_t)in[1] << 48) |
             ((uint64_t)in[2] << 40) |
             ((uint64_t)in[3] << 32) |
             ((uint64_t)in[4] << 24) |
             ((uint64_t)in[5] << 16) |
             ((uint64_t)in[6] << 8)  |
             ((uint64_t)in[7]);

    id.origin_id = ((uint32_t)in[8] << 24) |
                   ((uint32_t)in[9] << 16) |
                   ((uint32_t)in[10] << 8) |
                   ((uint32_t)in[11]);
    return id;
}

int hlc_now(crdtClock *clock, uint64_t current_physical_ms, crdtId *out_id, int *backpressure_warn) {
    if (!clock) return CRDT_CLOCK_ERR_INVALID;

    uint64_t pt = (current_physical_ms > 0) ? current_physical_ms : crdtClockPhysicalNowMs();
    int backpressure = 0;

    if (pt > clock->last_physical_ms) {
        clock->last_physical_ms = pt;
        clock->logical_counter = 0;
    } else {
        if (clock->logical_counter < CRDT_HLC_MAX_LOGICAL) {
            clock->logical_counter++;
        } else {
            /* 16-bit counter saturation: borrow forward physical millisecond */
            clock->last_physical_ms++;
            clock->logical_counter = 0;

            uint64_t borrowed_ms = (clock->last_physical_ms > pt) ? (clock->last_physical_ms - pt) : 0;
            if (clock->max_borrow_ms > 0 && borrowed_ms > clock->max_borrow_ms) {
                backpressure = 1;
            }
        }
    }

    if (backpressure_warn) {
        *backpressure_warn = backpressure;
    }

    uint64_t hlc = crdtHlcCompose(clock->last_physical_ms, clock->logical_counter);
    if (out_id) {
        out_id->hlc = hlc;
        out_id->origin_id = clock->origin_id;
    }

    return backpressure ? CRDT_CLOCK_BACKPRESSURE : CRDT_CLOCK_OK;
}

int hlc_recv(crdtClock *clock, uint64_t current_physical_ms, crdtId remote_id) {
    if (!clock) return CRDT_CLOCK_ERR_INVALID;

    uint64_t pt = (current_physical_ms > 0) ? current_physical_ms : crdtClockPhysicalNowMs();
    uint64_t remote_pt = crdtHlcPhysical(remote_id.hlc);
    uint16_t remote_log = crdtHlcLogical(remote_id.hlc);

    /* Strict future clock drift clamping */
    if (clock->max_skew_ms > 0 && remote_pt > (pt + clock->max_skew_ms)) {
        return CRDT_CLOCK_ERR_SKEW;
    }

    uint64_t max_pt = pt;
    if (clock->last_physical_ms > max_pt) max_pt = clock->last_physical_ms;
    if (remote_pt > max_pt) max_pt = remote_pt;

    uint32_t next_log = 0;
    if (max_pt == clock->last_physical_ms && max_pt == remote_pt) {
        next_log = (clock->logical_counter > remote_log ? clock->logical_counter : remote_log) + 1;
    } else if (max_pt == clock->last_physical_ms) {
        next_log = (uint32_t)clock->logical_counter + 1;
    } else if (max_pt == remote_pt) {
        next_log = (uint32_t)remote_log + 1;
    } else {
        /* max_pt == pt > (last_physical_ms, remote_pt) */
        next_log = 0;
    }

    if (next_log <= CRDT_HLC_MAX_LOGICAL) {
        clock->last_physical_ms = max_pt;
        clock->logical_counter = (uint16_t)next_log;
    } else {
        clock->last_physical_ms = max_pt + 1;
        clock->logical_counter = 0;
    }

    return CRDT_CLOCK_OK;
}
