/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Stream Engine (Disjoint Triplet Stream IDs & Add-Wins PEL)
 */

#include "crdt_stream.h"
#include "zmalloc.h"
#include "endianconv.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

/* Encode 16-byte big-endian binary key for Radix tree indexing */
void crdtStreamIDEncode(crdtStreamID id, unsigned char *buf) {
    uint64_t ms_be = htonu64(id.ms);
    uint32_t orig_be = htonl(id.origin_id);
    uint32_t seq_be = htonl(id.seq);
    memcpy(buf, &ms_be, 8);
    memcpy(buf + 8, &orig_be, 4);
    memcpy(buf + 12, &seq_be, 4);
}

void crdtStreamIDDecode(const unsigned char *buf, crdtStreamID *id) {
    uint64_t ms_be;
    uint32_t orig_be, seq_be;
    memcpy(&ms_be, buf, 8);
    memcpy(&orig_be, buf + 8, 4);
    memcpy(&seq_be, buf + 12, 4);
    id->ms = ntohu64(ms_be);
    id->origin_id = ntohl(orig_be);
    id->seq = ntohl(seq_be);
}

/* Compare two stream IDs. Returns 1 if a > b, -1 if a < b, 0 if equal. */
int crdtStreamIDCmp(crdtStreamID a, crdtStreamID b) {
    if (a.ms > b.ms) return 1;
    if (a.ms < b.ms) return -1;
    if (a.origin_id > b.origin_id) return 1;
    if (a.origin_id < b.origin_id) return -1;
    if (a.seq > b.seq) return 1;
    if (a.seq < b.seq) return -1;
    return 0;
}

/* Convert stream ID to/from string (<ms>-<origin>-<seq>) */
sds crdtStreamIDToString(crdtStreamID id) {
    return sdscatprintf(sdsempty(), "%llu-%u-%u", (unsigned long long)id.ms, id.origin_id, id.seq);
}

int crdtStringToStreamID(const char *s, size_t len, crdtStreamID *id) {
    if (!s || len == 0 || !id) return 0;
    unsigned long long ms = 0;
    unsigned int origin = 0;
    unsigned int seq = 0;

    int parsed = sscanf(s, "%llu-%u-%u", &ms, &origin, &seq);
    if (parsed == 3) {
        id->ms = ms;
        id->origin_id = origin;
        id->seq = seq;
        return 1;
    } else if (parsed == 1) {
        /* Fallback for <ms>-<seq> */
        unsigned long long s_seq = 0;
        if (sscanf(s, "%llu-%llu", &ms, &s_seq) == 2) {
            id->ms = ms;
            id->origin_id = 0;
            id->seq = (uint32_t)s_seq;
            return 1;
        }
    }
    return 0;
}

/* Create and free crdtStream instances */
crdtStream *crdtStreamCreate(void) {
    crdtStream *cs = zmalloc(sizeof(*cs));
    cs->entries = raxNew();
    cs->pel = raxNew();
    cs->length = 0;
    memset(&cs->last_id, 0, sizeof(cs->last_id));
    return cs;
}

static void freeStreamEntry(crdtStreamEntry *entry) {
    if (!entry) return;
    if (entry->fields) {
        for (size_t i = 0; i < entry->num_fields; i++) {
            sdsfree(entry->fields[i].field);
            sdsfree(entry->fields[i].value);
        }
        zfree(entry->fields);
    }
    zfree(entry);
}

static void freePelEntry(crdtPelEntry *pel) {
    if (!pel) return;
    if (pel->consumer) sdsfree(pel->consumer);
    zfree(pel);
}

void crdtStreamFree(crdtStream *cs) {
    if (!cs) return;

    if (cs->entries) {
        raxIterator ri;
        raxStart(&ri, cs->entries);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri)) {
            freeStreamEntry(ri.data);
        }
        raxStop(&ri);
        raxFree(cs->entries);
    }

    if (cs->pel) {
        raxIterator ri;
        raxStart(&ri, cs->pel);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri)) {
            freePelEntry(ri.data);
        }
        raxStop(&ri);
        raxFree(cs->pel);
    }

    zfree(cs);
}

/* Append item to stream. Returns added entry */
crdtStreamEntry *crdtStreamAppend(crdtStream *cs, crdtStreamID id, size_t num_fields, const sds *fields, const sds *values) {
    if (!cs) return NULL;

    unsigned char enc_id[16];
    crdtStreamIDEncode(id, enc_id);

    /* Idempotence check */
    void *dummy = NULL;
    if (raxFind(cs->entries, enc_id, 16, &dummy)) {
        return NULL;
    }

    crdtStreamEntry *e = zmalloc(sizeof(*e));
    e->id = id;
    e->num_fields = num_fields;
    e->deleted = 0;
    e->del_hlc = 0;

    if (num_fields > 0 && fields && values) {
        e->fields = zmalloc(num_fields * sizeof(crdtStreamField));
        for (size_t i = 0; i < num_fields; i++) {
            e->fields[i].field = sdsdup(fields[i]);
            e->fields[i].value = sdsdup(values[i]);
        }
    } else {
        e->fields = NULL;
    }

    raxInsert(cs->entries, enc_id, 16, e, NULL);
    cs->length++;
    if (crdtStreamIDCmp(id, cs->last_id) > 0) {
        cs->last_id = id;
    }
    return e;
}

/* Acknowledge message in PEL with HLC timestamp */
int crdtStreamAck(crdtStream *cs, crdtStreamID id, uint64_t ack_hlc) {
    if (!cs) return 0;

    unsigned char enc_id[16];
    crdtStreamIDEncode(id, enc_id);

    void *val = NULL;
    crdtPelEntry *pel = NULL;
    if (!raxFind(cs->pel, enc_id, 16, &val)) {
        pel = zmalloc(sizeof(*pel));
        pel->id = id;
        pel->consumer = NULL;
        pel->delivery_time = 0;
        pel->delivery_count = 0;
        pel->acked = 1;
        pel->ack_hlc = ack_hlc;
        raxInsert(cs->pel, enc_id, 16, pel, NULL);
        return 1;
    } else {
        pel = val;
    }

    if (ack_hlc > pel->ack_hlc) {
        pel->acked = 1;
        pel->ack_hlc = ack_hlc;
        return 1;
    }
    return 0;
}

/* Merge two crdtStream instances (S_local ⊔ S_remote) */
int crdtStreamMerge(crdtStream *local, crdtStream *remote) {
    if (!local || !remote) return 0;
    int changed = 0;

    raxIterator ri;
    raxStart(&ri, remote->entries);
    raxSeek(&ri, "^", NULL, 0);

    while (raxNext(&ri)) {
        crdtStreamEntry *r_e = ri.data;
        void *local_val = NULL;
        if (!raxFind(local->entries, ri.key, ri.key_len, &local_val)) {
            sds *fields = zmalloc(r_e->num_fields * sizeof(sds));
            sds *values = zmalloc(r_e->num_fields * sizeof(sds));
            for (size_t i = 0; i < r_e->num_fields; i++) {
                fields[i] = r_e->fields[i].field;
                values[i] = r_e->fields[i].value;
            }
            crdtStreamEntry *new_e = crdtStreamAppend(local, r_e->id, r_e->num_fields, fields, values);
            if (new_e && r_e->deleted) {
                new_e->deleted = r_e->deleted;
                new_e->del_hlc = r_e->del_hlc;
            }
            zfree(fields);
            zfree(values);
            changed = 1;
        } else {
            crdtStreamEntry *l_e = local_val;
            if (r_e->deleted && (!l_e->deleted || r_e->del_hlc > l_e->del_hlc)) {
                l_e->deleted = r_e->deleted;
                l_e->del_hlc = r_e->del_hlc;
                changed = 1;
            }
        }
    }
    raxStop(&ri);

    /* Merge PEL */
    raxStart(&ri, remote->pel);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri)) {
        crdtPelEntry *r_pel = ri.data;
        if (r_pel->acked) {
            if (crdtStreamAck(local, r_pel->id, r_pel->ack_hlc)) {
                changed = 1;
            }
        }
    }
    raxStop(&ri);

    return changed;
}
