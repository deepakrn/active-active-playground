/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Unified Operation Envelope Wire Protocol Subsystem
 */

#ifndef CRDT_ENVELOPE_H
#define CRDT_ENVELOPE_H

#include "crdt_clock.h"
#include "sds.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
struct client;
struct serverObject;
typedef struct serverObject robj;
typedef struct client client;

/* Unified CRDT Type Identifiers */
#define CRDT_TYPE_STRING    1
#define CRDT_TYPE_SET       2
#define CRDT_TYPE_COUNTER   3
#define CRDT_TYPE_STREAM    4
#define CRDT_TYPE_LIST      5
#define CRDT_TYPE_KEYSPACE  6

/* Operation Codes per Type */
#define OP_STR_SET          1
#define OP_STR_APPEND       2

#define OP_SET_ADD          1
#define OP_SET_REM          2

#define OP_CNT_INCR_INT     1
#define OP_CNT_INCR_FLOAT   2

#define OP_STRM_ADD         1
#define OP_STRM_ACK         2

#define OP_LIST_INSERT      1
#define OP_LIST_DELETE      2

#define OP_KEY_DEL          1
#define OP_KEY_EXPIREAT     2

/* Propagate a generic CRDT.OP replication frame */
void crdtPropagateOp(client *c, robj *key, uint32_t type_id, uint32_t op_code, crdtId id, int extra_argc, robj **extra_argv);

/* Handler for incoming CRDT.OP wire frames */
void crdtOpCommand(client *c);

#ifdef __cplusplus
}
#endif

#endif /* CRDT_ENVELOPE_H */
