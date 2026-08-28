/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Unified Operation Envelope Wire Protocol Subsystem
 */

#include "crdt_envelope.h"
#include "crdt_string.h"
#include "crdt_set.h"
#include "crdt_counter.h"
#include "crdt_stream.h"
#include "crdt_list.h"
#include "crdt_tombstone.h"
#include "crdt_gc.h"
#include "server.h"

/* Propagate a generic CRDT.OP replication frame */
void crdtPropagateOp(client *c, robj *key, uint32_t type_id, uint32_t op_code, crdtId id, int extra_argc, robj **extra_argv) {
    int total_argc = 6 + extra_argc;
    robj **argv = zmalloc(total_argc * sizeof(robj*));

    argv[0] = createStringObject("CRDT.OP", 7);
    argv[1] = createStringObjectFromLongLong(id.origin_id);
    argv[2] = createStringObjectFromLongLong(id.hlc);
    argv[3] = createStringObjectFromLongLong(type_id);
    argv[4] = createStringObjectFromLongLong(op_code);
    argv[5] = key;
    incrRefCount(key);

    for (int i = 0; i < extra_argc; i++) {
        argv[6 + i] = extra_argv[i];
        incrRefCount(argv[6 + i]);
    }

    alsoPropagate(c->db->id, argv, total_argc, PROPAGATE_REPL | PROPAGATE_AOF, c->slot);

    for (int i = 0; i < total_argc; i++) {
        decrRefCount(argv[i]);
    }
    zfree(argv);
}

/* Handler for incoming CRDT.OP wire frames:
 * CRDT.OP <origin_id> <hlc> <type_id> <op_code> <key> [args...] */
void crdtOpCommand(client *c) {
    if (c->argc < 6) {
        addReplyErrorArity(c);
        return;
    }

    long long origin_id_ll, type_id_ll, op_code_ll;
    unsigned long long hlc_ull;

    sds arg1 = objectGetVal(c->argv[1]);
    sds arg2 = objectGetVal(c->argv[2]);
    sds arg3 = objectGetVal(c->argv[3]);
    sds arg4 = objectGetVal(c->argv[4]);

    if (string2ll(arg1, sdslen(arg1), &origin_id_ll) == 0 || origin_id_ll < 0 ||
        string2ull(arg2, sdslen(arg2), &hlc_ull) == 0 ||
        string2ll(arg3, sdslen(arg3), &type_id_ll) == 0 ||
        string2ll(arg4, sdslen(arg4), &op_code_ll) == 0) {
        addReplyError(c, "Invalid CRDT.OP header arguments");
        return;
    }

    uint32_t origin_id = (uint32_t)origin_id_ll;
    uint64_t hlc = (uint64_t)hlc_ull;
    uint32_t type_id = (uint32_t)type_id_ll;
    uint32_t op_code = (uint32_t)op_code_ll;
    robj *key = c->argv[5];
    crdtId id = crdtIdMake(hlc, origin_id);

    /* Echo suppression */
    if (origin_id == server.origin_id) {
        addReply(c, shared.ok);
        return;
    }

    /* Advance local logical clock */
    hlc_recv(&server.crdt_clock, 0, id);

    /* Track peer activity and ack */
    crdtPeerTrackAck(origin_id, hlc, mstime());

    /* Check hierarchical key tombstone */
    sds key_sds = objectGetVal(key);
    if (c->db->crdt_key_tombstones && !crdtKeyCanMutate(c->db->crdt_key_tombstones, key_sds, id)) {
        addReply(c, shared.ok);
        return;
    }

    switch (type_id) {
        case CRDT_TYPE_STRING: {
            robj *o = lookupKeyWrite(c->db, key);
            sds val = (c->argc >= 7) ? (sds)objectGetVal(c->argv[6]) : sdsempty();

            if (op_code == OP_STR_SET) {
                if (o == NULL) {
                    o = createCrdtStringObject(id, val);
                    dbAdd(c->db, key, &o);
                } else if (objectGetEncoding(o) == OBJ_ENCODING_CRDT_STRING) {
                    crdtStringUpdate(objectGetVal(o), id, val);
                }
            } else if (op_code == OP_STR_APPEND) {
                if (o == NULL) {
                    o = createCrdtStringObject(id, val);
                    dbAdd(c->db, key, &o);
                } else if (objectGetEncoding(o) == OBJ_ENCODING_CRDT_STRING) {
                    crdtStringAppend(objectGetVal(o), id, val);
                }
            }
            break;
        }

        case CRDT_TYPE_SET: {
            if (c->argc < 7) break;
            sds member = objectGetVal(c->argv[6]);
            robj *o = lookupKeyWrite(c->db, key);
            if (o == NULL) {
                o = createCrdtSetObject();
                dbAdd(c->db, key, &o);
            }

            if (objectGetEncoding(o) == OBJ_ENCODING_CRDT_SET) {
                crdtSet *cs = objectGetVal(o);
                if (op_code == OP_SET_ADD) {
                    crdtSetAdd(cs, member, id);
                } else if (op_code == OP_SET_REM) {
                    crdtSetRemove(cs, member, id);
                }
            }
            break;
        }

        case CRDT_TYPE_COUNTER: {
            if (c->argc < 7) break;
            robj *o = lookupKeyWrite(c->db, key);

            if (op_code == OP_CNT_INCR_INT) {
                long long delta;
                sds arg_val = objectGetVal(c->argv[6]);
                if (string2ll(arg_val, sdslen(arg_val), &delta)) {
                    if (o == NULL) {
                        o = createCrdtCounterObject(0);
                        dbAdd(c->db, key, &o);
                    }
                    if (objectGetEncoding(o) == OBJ_ENCODING_CRDT_COUNTER) {
                        crdtCounterIncr(objectGetVal(o), origin_id, (int64_t)delta);
                    }
                }
            } else if (op_code == OP_CNT_INCR_FLOAT) {
                double delta;
                sds arg_val = objectGetVal(c->argv[6]);
                if (string2d(arg_val, sdslen(arg_val), &delta)) {
                    if (o == NULL) {
                        o = createCrdtCounterObject(1);
                        dbAdd(c->db, key, &o);
                    }
                    if (objectGetEncoding(o) == OBJ_ENCODING_CRDT_COUNTER) {
                        crdtCounterIncrFloat(objectGetVal(o), origin_id, delta);
                    }
                }
            }
            break;
        }

        case CRDT_TYPE_STREAM: {
            robj *o = lookupKeyWrite(c->db, key);
            if (op_code == OP_STRM_ADD) {
                if (c->argc < 7) break;
                crdtStreamID strm_id;
                sds id_str = objectGetVal(c->argv[6]);
                if (crdtStringToStreamID(id_str, sdslen(id_str), &strm_id)) {
                    int num_fields = (c->argc - 7) / 2;
                    sds *fields = (num_fields > 0) ? zmalloc(num_fields * sizeof(sds)) : NULL;
                    sds *values = (num_fields > 0) ? zmalloc(num_fields * sizeof(sds)) : NULL;

                    for (int i = 0; i < num_fields; i++) {
                        fields[i] = objectGetVal(c->argv[7 + i * 2]);
                        values[i] = objectGetVal(c->argv[7 + i * 2 + 1]);
                    }

                    if (o == NULL) {
                        o = createCrdtStreamObject();
                        dbAdd(c->db, key, &o);
                    }

                    if (objectGetEncoding(o) == OBJ_ENCODING_CRDT_STREAM) {
                        crdtStreamAppend(objectGetVal(o), strm_id, num_fields, (const sds*)fields, (const sds*)values);
                    }

                    if (fields) zfree(fields);
                    if (values) zfree(values);
                }
            } else if (op_code == OP_STRM_ACK) {
                if (c->argc < 7) break;
                crdtStreamID strm_id;
                sds id_str = objectGetVal(c->argv[6]);
                if (crdtStringToStreamID(id_str, sdslen(id_str), &strm_id) && o != NULL) {
                    if (objectGetEncoding(o) == OBJ_ENCODING_CRDT_STREAM) {
                        crdtStreamAck(objectGetVal(o), strm_id, hlc);
                    }
                }
            }
            break;
        }

        case CRDT_TYPE_LIST: {
            robj *o = lookupKeyWrite(c->db, key);
            if (op_code == OP_LIST_INSERT) {
                if (c->argc < 9) break;
                unsigned long long parent_hlc;
                long long parent_origin;
                sds arg_phlc = objectGetVal(c->argv[6]);
                sds arg_porig = objectGetVal(c->argv[7]);
                string2ull(arg_phlc, sdslen(arg_phlc), &parent_hlc);
                string2ll(arg_porig, sdslen(arg_porig), &parent_origin);
                crdtId parent_id = crdtIdMake((uint64_t)parent_hlc, (uint32_t)parent_origin);
                sds val = objectGetVal(c->argv[8]);

                if (o == NULL) {
                    o = createCrdtListObject();
                    dbAdd(c->db, key, &o);
                }
                if (objectGetEncoding(o) == OBJ_ENCODING_CRDT_LIST) {
                    crdtListInsertAfter(objectGetVal(o), parent_id, id, val);
                }
            } else if (op_code == OP_LIST_DELETE) {
                if (o != NULL && objectGetEncoding(o) == OBJ_ENCODING_CRDT_LIST) {
                    crdtListDeleteVertex(objectGetVal(o), id, hlc, origin_id);
                }
            }
            break;
        }

        case CRDT_TYPE_KEYSPACE: {
            if (op_code == OP_KEY_DEL) {
                if (c->db->crdt_key_tombstones) {
                    crdtKeyTombstoneRecord(c->db->crdt_key_tombstones, key_sds, id, mstime());
                }
                dbDelete(c->db, key);
            } else if (op_code == OP_KEY_EXPIREAT) {
                if (c->argc >= 7) {
                    long long expiretime;
                    sds arg_exp = objectGetVal(c->argv[6]);
                    if (string2ll(arg_exp, sdslen(arg_exp), &expiretime)) {
                        setExpire(c, c->db, key, expiretime);
                    }
                }
            }
            break;
        }
    }

    server.dirty++;
    signalModifiedKey(c, c->db, key);
    addReply(c, shared.ok);
}
