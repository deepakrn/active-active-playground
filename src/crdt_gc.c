/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Anti-Entropy & Stability Horizon Tombstone Garbage Collection Implementation
 */

#include "crdt_gc.h"
#include "server.h"
#include "zmalloc.h"
#include <string.h>
#include <strings.h>

static void dictEntryDestructorPeerValue(void *entry) {
    dictEntry *de = entry;
    if (de && de->v.val) {
        zfree(de->v.val);
        de->v.val = NULL;
    }
    zfree(de);
}

dictType crdtPeerDictType = {
    .entryGetKey = dictEntryGetKey,
    .entryDestructor = dictEntryDestructorPeerValue,
};

void crdtGcInit(void) {
    if (!server.crdt_peers) {
        server.crdt_peers = dictCreate(&crdtPeerDictType);
    }
    memset(&server.crdt_gc_stats, 0, sizeof(server.crdt_gc_stats));
}

void crdtGcReset(void) {
    if (server.crdt_peers) {
        dictEmpty(server.crdt_peers, NULL);
    }
    memset(&server.crdt_gc_stats, 0, sizeof(server.crdt_gc_stats));
}

int crdtPeerTrackAck(uint32_t peer_id, uint64_t acked_hlc, mstime_t now) {
    if (peer_id == 0 || peer_id == server.origin_id) return 0;
    if (!server.crdt_peers) crdtGcInit();

    dictEntry *de = dictFind(server.crdt_peers, (void *)(uintptr_t)peer_id);
    crdtPeerState *peer = NULL;
    if (de) {
        peer = dictGetVal(de);
    } else {
        peer = zmalloc(sizeof(*peer));
        peer->node_id = peer_id;
        peer->last_acked_hlc = acked_hlc;
        peer->last_heartbeat_time = now;
        peer->is_ejected = 0;
        peer->is_active = 1;
        dictAdd(server.crdt_peers, (void *)(uintptr_t)peer_id, peer);
        return 1;
    }

    if (acked_hlc > peer->last_acked_hlc) {
        peer->last_acked_hlc = acked_hlc;
    }
    peer->last_heartbeat_time = now;
    peer->is_ejected = 0;
    peer->is_active = 1;
    return 1;
}

int crdtPeerHeartbeat(uint32_t peer_id, mstime_t now) {
    if (peer_id == 0 || peer_id == server.origin_id) return 0;
    if (!server.crdt_peers) crdtGcInit();

    dictEntry *de = dictFind(server.crdt_peers, (void *)(uintptr_t)peer_id);
    crdtPeerState *peer = NULL;
    if (de) {
        peer = dictGetVal(de);
    } else {
        peer = zmalloc(sizeof(*peer));
        peer->node_id = peer_id;
        peer->last_acked_hlc = 0;
        peer->last_heartbeat_time = now;
        peer->is_ejected = 0;
        peer->is_active = 1;
        dictAdd(server.crdt_peers, (void *)(uintptr_t)peer_id, peer);
        return 1;
    }

    peer->last_heartbeat_time = now;
    peer->is_ejected = 0;
    peer->is_active = 1;
    return 1;
}

int crdtPeerRegister(uint32_t peer_id, uint64_t initial_hlc) {
    if (peer_id == 0 || peer_id == server.origin_id) return 0;
    if (!server.crdt_peers) crdtGcInit();

    dictEntry *de = dictFind(server.crdt_peers, (void *)(uintptr_t)peer_id);
    crdtPeerState *peer = NULL;
    if (de) {
        peer = dictGetVal(de);
        peer->last_acked_hlc = initial_hlc;
        peer->last_heartbeat_time = mstime();
        peer->is_ejected = 0;
        peer->is_active = 1;
        return 1;
    }

    peer = zmalloc(sizeof(*peer));
    peer->node_id = peer_id;
    peer->last_acked_hlc = initial_hlc;
    peer->last_heartbeat_time = mstime();
    peer->is_ejected = 0;
    peer->is_active = 1;
    dictAdd(server.crdt_peers, (void *)(uintptr_t)peer_id, peer);
    return 1;
}

int crdtPeerUnregister(uint32_t peer_id) {
    if (!server.crdt_peers) return 0;
    return dictDelete(server.crdt_peers, (void *)(uintptr_t)peer_id) == DICT_OK;
}

int crdtPeerEject(uint32_t peer_id) {
    if (!server.crdt_peers) return 0;
    dictEntry *de = dictFind(server.crdt_peers, (void *)(uintptr_t)peer_id);
    if (!de) return 0;
    crdtPeerState *peer = dictGetVal(de);
    if (!peer->is_ejected) {
        peer->is_ejected = 1;
        server.crdt_gc_stats.straggler_ejections++;
    }
    return 1;
}

crdtPeerState *crdtPeerFind(uint32_t peer_id) {
    if (!server.crdt_peers) return NULL;
    dictEntry *de = dictFind(server.crdt_peers, (void *)(uintptr_t)peer_id);
    return de ? dictGetVal(de) : NULL;
}

uint64_t crdtComputeStabilityHorizon(void) {
    mstime_t now = mstime();
    uint64_t min_hlc = UINT64_MAX;
    int active_peer_count = 0;
    uint64_t local_hlc = crdtHlcCompose(server.crdt_clock.last_physical_ms, server.crdt_clock.logical_counter);

    if (server.crdt_peers && dictSize(server.crdt_peers) > 0) {
        dictIterator *di = dictGetIterator(server.crdt_peers);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
            crdtPeerState *peer = dictGetVal(de);
            if (!peer || !peer->is_active) continue;

            /* Check straggler lease eviction:
             * If peer has not sent heartbeat/ack for > aa_gc_lease_ms, eject it */
            if (peer->last_heartbeat_time > 0 &&
                (now - peer->last_heartbeat_time > server.aa_gc_lease_ms)) {
                if (!peer->is_ejected) {
                    peer->is_ejected = 1;
                    server.crdt_gc_stats.straggler_ejections++;
                    serverLog(LL_VERBOSE, "CRDT: Peer %u ejected from GC quorum (lease expired %lld ms ago)",
                              peer->node_id, (long long)(now - peer->last_heartbeat_time));
                }
            }

            if (!peer->is_ejected) {
                if (peer->last_acked_hlc < min_hlc) {
                    min_hlc = peer->last_acked_hlc;
                }
                active_peer_count++;
            }
        }
        dictReleaseIterator(di);
    }

    /* If no active peer is tracked in quorum, stability horizon is local HLC */
    if (active_peer_count == 0) {
        return local_hlc;
    }

    /* Also compare with local HLC */
    if (local_hlc < min_hlc) {
        min_hlc = local_hlc;
    }

    return min_hlc;
}

size_t crdtGetTrackedPeerCount(void) {
    return server.crdt_peers ? dictSize(server.crdt_peers) : 0;
}

size_t crdtGetActiveQuorumCount(void) {
    if (!server.crdt_peers) return 0;
    size_t count = 0;
    mstime_t now = mstime();
    dictIterator *di = dictGetIterator(server.crdt_peers);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        crdtPeerState *peer = dictGetVal(de);
        if (peer && peer->is_active) {
            if (peer->last_heartbeat_time > 0 && (now - peer->last_heartbeat_time > server.aa_gc_lease_ms)) {
                /* lease expired */
            } else if (!peer->is_ejected) {
                count++;
            }
        }
    }
    dictReleaseIterator(di);
    return count;
}

size_t crdtGetEjectedPeerCount(void) {
    if (!server.crdt_peers) return 0;
    size_t count = 0;
    mstime_t now = mstime();
    dictIterator *di = dictGetIterator(server.crdt_peers);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        crdtPeerState *peer = dictGetVal(de);
        if (peer && peer->is_active) {
            if (peer->is_ejected || (peer->last_heartbeat_time > 0 && (now - peer->last_heartbeat_time > server.aa_gc_lease_ms))) {
                count++;
            }
        }
    }
    dictReleaseIterator(di);
    return count;
}

size_t crdtDatabaseGCSweep(int dbid, uint64_t h_stable, size_t max_keys) {
    if (!server.active_active_enabled) return 0;
    if (dbid < 0 || dbid >= server.dbnum) return 0;
    serverDb *db = server.db[dbid];
    if (!db || kvstoreSize(db->keys) == 0) return 0;

    size_t pruned_total = 0;
    size_t keys_checked = 0;

    kvstoreIterator *kvs_it = kvstoreIteratorInit(db->keys, HASHTABLE_ITER_SAFE);
    void *next;

    while (kvstoreIteratorNext(kvs_it, &next)) {
        robj *val = next;
        if (val && val->type == OBJ_LIST && objectGetEncoding(val) == OBJ_ENCODING_CRDT_LIST) {
            crdtList *cl = objectGetVal(val);
            if (cl && cl->tombstone_count > 0) {
                size_t pruned = crdtListPruneTombstones(cl, h_stable);
                pruned_total += pruned;
            }
        }
        keys_checked++;
        if (max_keys > 0 && keys_checked >= max_keys) break;
    }

    kvstoreIteratorRelease(kvs_it);
    return pruned_total;
}

size_t crdtAllDatabasesGCSweep(uint64_t h_stable) {
    size_t total = 0;
    for (int j = 0; j < server.dbnum; j++) {
        total += crdtDatabaseGCSweep(j, h_stable, 0);
    }
    server.crdt_gc_stats.tombstones_collected += total;
    server.crdt_gc_stats.gc_sweeps++;
    server.crdt_gc_stats.last_sweep_time = mstime();
    server.crdt_gc_stats.last_h_stable = h_stable;
    return total;
}

void crdtCronGCSweep(void) {
    if (!server.active_active_enabled) return;
    uint64_t h_stable = crdtComputeStabilityHorizon();
    if (h_stable == 0) return;

    /* Incremental sweep over databases */
    static int current_db = 0;
    size_t pruned = crdtDatabaseGCSweep(current_db % server.dbnum, h_stable, 100);
    current_db++;

    server.crdt_gc_stats.tombstones_collected += pruned;
    server.crdt_gc_stats.gc_sweeps++;
    server.crdt_gc_stats.last_sweep_time = mstime();
    server.crdt_gc_stats.last_h_stable = h_stable;
}

void crdtGcCommand(client *c) {
    uint64_t h_stable = 0;
    size_t pruned = 0;

    if (c->argc == 1) {
        /* CRDT.GC -> compute current H_stable, sweep all DBs */
        h_stable = crdtComputeStabilityHorizon();
        pruned = crdtAllDatabasesGCSweep(h_stable);
        addReplyLongLong(c, pruned);
        return;
    }

    sds first_arg = objectGetVal(c->argv[1]);

    if (strcasecmp(first_arg, "ALL") == 0) {
        if (c->argc >= 3) {
            unsigned long long h;
            if (string2ull(objectGetVal(c->argv[2]), sdslen(objectGetVal(c->argv[2])), &h) == 0) {
                addReplyError(c, "Invalid h_stable timestamp");
                return;
            }
            h_stable = (uint64_t)h;
        } else {
            h_stable = crdtComputeStabilityHorizon();
        }
        pruned = crdtAllDatabasesGCSweep(h_stable);
        addReplyLongLong(c, pruned);
        return;
    }

    /* Key-specific GC: CRDT.GC <key> [h_stable] */
    if (c->argc >= 3) {
        unsigned long long h;
        if (string2ull(objectGetVal(c->argv[2]), sdslen(objectGetVal(c->argv[2])), &h) == 0) {
            addReplyError(c, "Invalid h_stable timestamp");
            return;
        }
        h_stable = (uint64_t)h;
    } else {
        h_stable = crdtComputeStabilityHorizon();
    }

    robj *lobj = lookupKeyWrite(c->db, c->argv[1]);
    if (lobj == NULL || lobj->type != OBJ_LIST || objectGetEncoding(lobj) != OBJ_ENCODING_CRDT_LIST) {
        addReplyLongLong(c, 0);
        return;
    }

    crdtList *cl = objectGetVal(lobj);
    pruned = crdtListPruneTombstones(cl, h_stable);

    if (cl->total_vertices == 0 && cl->length == 0) {
        dbDelete(c->db, c->argv[1]);
    }

    if (pruned > 0) {
        server.crdt_gc_stats.tombstones_collected += pruned;
        server.dirty += pruned;
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST, "crdt.gc", c->argv[1], c->db->id);
    }

    addReplyLongLong(c, pruned);
}

void crdtStabilityCommand(client *c) {
    if (c->argc == 1 || (c->argc == 2 && strcasecmp(objectGetVal(c->argv[1]), "INFO") == 0) ||
        (c->argc == 2 && strcasecmp(objectGetVal(c->argv[1]), "GET") == 0)) {
        uint64_t h_stable = crdtComputeStabilityHorizon();
        mstime_t now = mstime();
        uint64_t local_hlc = crdtHlcCompose(server.crdt_clock.last_physical_ms, server.crdt_clock.logical_counter);

        if (c->argc == 2 && strcasecmp(objectGetVal(c->argv[1]), "GET") == 0) {
            addReplyLongLong(c, h_stable);
            return;
        }

        /* Return array/map of stability info */
        size_t tracked_count = crdtGetTrackedPeerCount();
        size_t active_count = crdtGetActiveQuorumCount();
        size_t ejected_count = crdtGetEjectedPeerCount();

        void *reply_len = addReplyDeferredLen(c);
        int numfields = 0;

        addReplyBulkCString(c, "h_stable");
        addReplyLongLong(c, h_stable);
        numfields++;

        addReplyBulkCString(c, "local_hlc");
        addReplyLongLong(c, local_hlc);
        numfields++;

        addReplyBulkCString(c, "gc_lease_ms");
        addReplyLongLong(c, server.aa_gc_lease_ms);
        numfields++;

        addReplyBulkCString(c, "tracked_peers");
        addReplyLongLong(c, tracked_count);
        numfields++;

        addReplyBulkCString(c, "active_peers");
        addReplyLongLong(c, active_count);
        numfields++;

        addReplyBulkCString(c, "ejected_peers");
        addReplyLongLong(c, ejected_count);
        numfields++;

        addReplyBulkCString(c, "tombstones_collected");
        addReplyLongLong(c, server.crdt_gc_stats.tombstones_collected);
        numfields++;

        addReplyBulkCString(c, "gc_sweeps");
        addReplyLongLong(c, server.crdt_gc_stats.gc_sweeps);
        numfields++;

        addReplyBulkCString(c, "straggler_ejections");
        addReplyLongLong(c, server.crdt_gc_stats.straggler_ejections);
        numfields++;

        /* Peers array */
        addReplyBulkCString(c, "peers");
        numfields++;
        if (server.crdt_peers && dictSize(server.crdt_peers) > 0) {
            void *peers_len = addReplyDeferredLen(c);
            int peer_items = 0;
            dictIterator *di = dictGetIterator(server.crdt_peers);
            dictEntry *de;
            while ((de = dictNext(di)) != NULL) {
                crdtPeerState *peer = dictGetVal(de);
                if (!peer) continue;
                void *sub_len = addReplyDeferredLen(c);
                int sub_fields = 0;

                addReplyBulkCString(c, "node_id");
                addReplyLongLong(c, peer->node_id);
                sub_fields++;

                addReplyBulkCString(c, "last_acked_hlc");
                addReplyLongLong(c, peer->last_acked_hlc);
                sub_fields++;

                addReplyBulkCString(c, "last_heartbeat_ago_ms");
                addReplyLongLong(c, peer->last_heartbeat_time > 0 ? (now - peer->last_heartbeat_time) : -1);
                sub_fields++;

                addReplyBulkCString(c, "is_ejected");
                addReplyLongLong(c, peer->is_ejected);
                sub_fields++;

                setDeferredMapLen(c, sub_len, sub_fields);
                peer_items++;
            }
            dictReleaseIterator(di);
            setDeferredArrayLen(c, peers_len, peer_items);
        } else {
            addReplyArrayLen(c, 0);
        }

        setDeferredMapLen(c, reply_len, numfields);
        return;
    }

    sds subcmd = objectGetVal(c->argv[1]);

    if (strcasecmp(subcmd, "PEER_ACK") == 0 || strcasecmp(subcmd, "ACK") == 0) {
        if (c->argc < 4) {
            addReplyError(c, "ERR syntax error, usage: CRDT.STABILITY PEER_ACK <peer_id> <acked_hlc>");
            return;
        }
        long long peer_id_ll;
        unsigned long long acked_hlc_ull;
        if (string2ll(objectGetVal(c->argv[2]), sdslen(objectGetVal(c->argv[2])), &peer_id_ll) == 0 || peer_id_ll <= 0) {
            addReplyError(c, "Invalid peer_id");
            return;
        }
        if (string2ull(objectGetVal(c->argv[3]), sdslen(objectGetVal(c->argv[3])), &acked_hlc_ull) == 0) {
            addReplyError(c, "Invalid acked_hlc");
            return;
        }
        crdtPeerTrackAck((uint32_t)peer_id_ll, (uint64_t)acked_hlc_ull, mstime());
        addReply(c, shared.ok);
        return;
    }

    if (strcasecmp(subcmd, "PEER_HEARTBEAT") == 0 || strcasecmp(subcmd, "HEARTBEAT") == 0) {
        if (c->argc < 3) {
            addReplyError(c, "ERR syntax error, usage: CRDT.STABILITY PEER_HEARTBEAT <peer_id>");
            return;
        }
        long long peer_id_ll;
        if (string2ll(objectGetVal(c->argv[2]), sdslen(objectGetVal(c->argv[2])), &peer_id_ll) == 0 || peer_id_ll <= 0) {
            addReplyError(c, "Invalid peer_id");
            return;
        }
        crdtPeerHeartbeat((uint32_t)peer_id_ll, mstime());
        addReply(c, shared.ok);
        return;
    }

    if (strcasecmp(subcmd, "PEER_ADD") == 0) {
        if (c->argc < 3) {
            addReplyError(c, "ERR syntax error, usage: CRDT.STABILITY PEER_ADD <peer_id> [initial_hlc]");
            return;
        }
        long long peer_id_ll;
        unsigned long long init_hlc = 0;
        if (string2ll(objectGetVal(c->argv[2]), sdslen(objectGetVal(c->argv[2])), &peer_id_ll) == 0 || peer_id_ll <= 0) {
            addReplyError(c, "Invalid peer_id");
            return;
        }
        if (c->argc >= 4) {
            if (string2ull(objectGetVal(c->argv[3]), sdslen(objectGetVal(c->argv[3])), &init_hlc) == 0) {
                addReplyError(c, "Invalid initial_hlc");
                return;
            }
        }
        crdtPeerRegister((uint32_t)peer_id_ll, (uint64_t)init_hlc);
        addReply(c, shared.ok);
        return;
    }

    if (strcasecmp(subcmd, "PEER_DEL") == 0) {
        if (c->argc < 3) {
            addReplyError(c, "ERR syntax error, usage: CRDT.STABILITY PEER_DEL <peer_id>");
            return;
        }
        long long peer_id_ll;
        if (string2ll(objectGetVal(c->argv[2]), sdslen(objectGetVal(c->argv[2])), &peer_id_ll) == 0 || peer_id_ll <= 0) {
            addReplyError(c, "Invalid peer_id");
            return;
        }
        crdtPeerUnregister((uint32_t)peer_id_ll);
        addReply(c, shared.ok);
        return;
    }

    if (strcasecmp(subcmd, "PEER_EJECT") == 0) {
        if (c->argc < 3) {
            addReplyError(c, "ERR syntax error, usage: CRDT.STABILITY PEER_EJECT <peer_id>");
            return;
        }
        long long peer_id_ll;
        if (string2ll(objectGetVal(c->argv[2]), sdslen(objectGetVal(c->argv[2])), &peer_id_ll) == 0 || peer_id_ll <= 0) {
            addReplyError(c, "Invalid peer_id");
            return;
        }
        crdtPeerEject((uint32_t)peer_id_ll);
        addReply(c, shared.ok);
        return;
    }

    if (strcasecmp(subcmd, "RESET") == 0) {
        crdtGcReset();
        addReply(c, shared.ok);
        return;
    }

    addReplyErrorFormat(c, "Unknown CRDT.STABILITY subcommand '%s'", subcmd);
}

sds genCrdtInfoString(sds info) {
    uint64_t h_stable = crdtComputeStabilityHorizon();
    uint64_t local_hlc = crdtHlcCompose(server.crdt_clock.last_physical_ms, server.crdt_clock.logical_counter);

    info = sdscatprintf(info,
                        "# CRDT\r\n"
                        "crdt_active_active_enabled:%d\r\n"
                        "crdt_origin_id:%u\r\n"
                        "crdt_local_hlc:%llu\r\n"
                        "crdt_h_stable:%llu\r\n"
                        "crdt_gc_lease_ms:%lld\r\n"
                        "crdt_tracked_peers:%zu\r\n"
                        "crdt_active_peers:%zu\r\n"
                        "crdt_ejected_peers:%zu\r\n"
                        "crdt_tombstones_collected:%llu\r\n"
                        "crdt_gc_sweeps:%llu\r\n"
                        "crdt_straggler_ejections:%llu\r\n",
                        server.active_active_enabled,
                        server.origin_id,
                        (unsigned long long)local_hlc,
                        (unsigned long long)h_stable,
                        (long long)server.aa_gc_lease_ms,
                        crdtGetTrackedPeerCount(),
                        crdtGetActiveQuorumCount(),
                        crdtGetEjectedPeerCount(),
                        server.crdt_gc_stats.tombstones_collected,
                        server.crdt_gc_stats.gc_sweeps,
                        server.crdt_gc_stats.straggler_ejections);
    return info;
}
