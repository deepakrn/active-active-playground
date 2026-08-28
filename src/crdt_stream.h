/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Stream Engine (Disjoint Triplet Stream IDs & Add-Wins PEL)
 */

#ifndef CRDT_STREAM_H
#define CRDT_STREAM_H

#include "crdt_clock.h"
#include "sds.h"
#include "rax.h"
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Disjoint Active-Active Stream Identifier: 16 bytes */
typedef struct crdtStreamID {
    uint64_t ms;        /* Unix timestamp in milliseconds */
    uint32_t origin_id; /* Origin node ID */
    uint32_t seq;       /* Sequence counter within the millisecond */
} crdtStreamID;

/* Stream entry field-value pair */
typedef struct crdtStreamField {
    sds field;
    sds value;
} crdtStreamField;

/* Stream entry payload */
typedef struct crdtStreamEntry {
    crdtStreamID id;
    size_t num_fields;
    crdtStreamField *fields;
    int deleted;        /* 1 if tombstone */
    uint64_t del_hlc;
} crdtStreamEntry;

/* Consumer Group PEL Entry */
typedef struct crdtPelEntry {
    crdtStreamID id;
    sds consumer;
    uint64_t delivery_time;
    uint64_t delivery_count;
    int acked;
    uint64_t ack_hlc;
} crdtPelEntry;

/* CRDT Stream Container */
typedef struct crdtStream {
    rax *entries;       /* Radix tree mapping encoded 16-byte crdtStreamID -> crdtStreamEntry* */
    rax *pel;           /* Radix tree mapping encoded 16-byte crdtStreamID -> crdtPelEntry* */
    size_t length;      /* Number of non-deleted entries */
    crdtStreamID last_id;
} crdtStream;

/* Encode 16-byte big-endian binary key for Radix tree indexing */
void crdtStreamIDEncode(crdtStreamID id, unsigned char *buf);
void crdtStreamIDDecode(const unsigned char *buf, crdtStreamID *id);

/* Compare two stream IDs. Returns 1 if a > b, -1 if a < b, 0 if equal. */
int crdtStreamIDCmp(crdtStreamID a, crdtStreamID b);

/* Convert stream ID to/from string (<ms>-<origin>-<seq>) */
sds crdtStreamIDToString(crdtStreamID id);
int crdtStringToStreamID(const char *s, size_t len, crdtStreamID *id);

/* Create and free crdtStream instances */
crdtStream *crdtStreamCreate(void);
void crdtStreamFree(crdtStream *cs);

/* Append item to stream. Returns added entry */
crdtStreamEntry *crdtStreamAppend(crdtStream *cs, crdtStreamID id, size_t num_fields, const sds *fields, const sds *values);

/* Acknowledge message in PEL with HLC timestamp */
int crdtStreamAck(crdtStream *cs, crdtStreamID id, uint64_t ack_hlc);

/* Merge two crdtStream instances (S_local ⊔ S_remote) */
int crdtStreamMerge(crdtStream *local, crdtStream *remote);

#ifdef __cplusplus
}
#endif

#endif /* CRDT_STREAM_H */
