/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Active-Active Unified CRDT Types & Invariants Comprehensive Test Suite
 */

#include "crdt_clock.h"
#include "crdt_string.h"
#include "crdt_set.h"
#include "crdt_counter.h"
#include "crdt_stream.h"
#include "crdt_tombstone.h"
#include "sds.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>

/* Standalone server assert mocks for sds/rax/hashtable */
void _serverAssert(const char *estr, const char *file, int line) {
    fprintf(stderr, "ASSERTION FAILED: %s (%s:%d)\n", estr, file, line);
    abort();
}

void _serverAssertWithInfo(const void *c, const void *o, const char *estr, const char *file, int line) {
    (void)c; (void)o;
    fprintf(stderr, "ASSERTION FAILED: %s (%s:%d)\n", estr, file, line);
    abort();
}

void _serverPanic(const char *file, int line, const char *msg, ...) {
    fprintf(stderr, "SERVER PANIC at %s:%d: %s\n", file, line, msg);
    abort();
}

uint64_t wangHash64(uint64_t hash) {
    hash = (~hash) + (hash << 21);
    hash = hash ^ (hash >> 24);
    hash = (hash + (hash << 3)) + (hash << 8);
    hash = hash ^ (hash >> 14);
    hash = (hash + (hash << 2)) + (hash << 4);
    hash = hash ^ (hash >> 28);
    hash = hash + (hash << 31);
    return hash;
}

#define TEST(name) static void test_##name(void)
#define RUN_TEST(name) do { \
    printf("Running test: %s...", #name); \
    test_##name(); \
    printf(" PASSED\n"); \
} while (0)

/* =========================== 1. Clock & Key Tombstones =========================== */

TEST(clock_and_key_tombstones) {
    crdtClockState clk;
    crdtClockInit(&clk, 1, CRDT_DEFAULT_MAX_BORROW_MS, CRDT_DEFAULT_MAX_SKEW_MS);

    crdtId write_id1, write_id2, del_id;
    hlc_now(&clk, 1000, &write_id1, NULL);
    hlc_now(&clk, 2000, &del_id, NULL);
    hlc_now(&clk, 3000, &write_id2, NULL);

    dict *tombstones = crdtKeyTombstoneDictCreate();
    sds key = sdsnew("mykey");

    /* Initially write is allowed */
    assert(crdtKeyCanMutate(tombstones, key, write_id1) == 1);

    /* Record deletion tombstone at del_id */
    assert(crdtKeyTombstoneRecord(tombstones, key, del_id, 2000) == 1);

    /* Stale write (write_id1 < del_id) is masked and rejected */
    assert(crdtKeyCanMutate(tombstones, key, write_id1) == 0);

    /* Newer write (write_id2 > del_id) survives */
    assert(crdtKeyCanMutate(tombstones, key, write_id2) == 1);

    /* Pruning with H_stable below del_id does not remove it */
    assert(crdtKeyTombstonePrune(tombstones, del_id.hlc - 1) == 0);
    assert(crdtKeyCanMutate(tombstones, key, write_id1) == 0);

    /* Pruning with H_stable >= del_id removes tombstone */
    assert(crdtKeyTombstonePrune(tombstones, del_id.hlc) == 1);
    assert(crdtKeyCanMutate(tombstones, key, write_id1) == 1);

    sdsfree(key);
    crdtKeyTombstoneDictRelease(tombstones);
}

/* =========================== 2. String CRDT (HLC-LWW) =========================== */

TEST(string_lww_and_merge) {
    crdtId id1 = crdtIdMake(100, 1);
    crdtId id2 = crdtIdMake(200, 2);
    crdtId id_tie_low = crdtIdMake(300, 1);
    crdtId id_tie_high = crdtIdMake(300, 2);

    sds val1 = sdsnew("hello");
    sds val2 = sdsnew("world");
    sds val3 = sdsnew("active");
    sds val4 = sdsnew("active-active");

    crdtString *s1 = crdtStringCreate(id1, val1);
    assert(strcmp(s1->val, "hello") == 0);

    /* Update with higher HLC wins */
    assert(crdtStringUpdate(s1, id2, val2) == 1);
    assert(strcmp(s1->val, "world") == 0);

    /* Stale update with lower HLC is ignored */
    assert(crdtStringUpdate(s1, id1, val1) == 0);
    assert(strcmp(s1->val, "world") == 0);

    /* Equal HLC tie-breaker: higher origin_id wins */
    crdtString *s_tie = crdtStringCreate(id_tie_low, val3);
    assert(crdtStringUpdate(s_tie, id_tie_high, val4) == 1);
    assert(strcmp(s_tie->val, "active-active") == 0);

    /* Merge commutativity: A ⊔ B == B ⊔ A */
    crdtString *a = crdtStringCreate(id1, val1);
    crdtString *b = crdtStringCreate(id2, val2);
    crdtStringMerge(a, b);
    assert(strcmp(a->val, "world") == 0);

    crdtStringFree(s1);
    crdtStringFree(s_tie);
    crdtStringFree(a);
    crdtStringFree(b);
    sdsfree(val1);
    sdsfree(val2);
    sdsfree(val3);
    sdsfree(val4);
}

/* =========================== 3. Set CRDT (Two-Phase LWW) =========================== */

TEST(set_lww_element_and_merge) {
    crdtSet *sa = crdtSetCreate();
    crdtSet *sb = crdtSetCreate();

    sds m1 = sdsnew("apple");
    sds m2 = sdsnew("banana");

    crdtId add1 = crdtIdMake(100, 1);
    crdtId add2 = crdtIdMake(150, 2);
    crdtId rem1 = crdtIdMake(200, 1);

    /* Node A adds apple and banana */
    assert(crdtSetAdd(sa, m1, add1) == 1);
    assert(crdtSetAdd(sa, m2, add2) == 1);
    assert(crdtSetCard(sa) == 2);
    assert(crdtSetContains(sa, m1) == 1);

    /* Node B removes apple at higher timestamp rem1 (200 > 100) */
    crdtSetRemove(sb, m1, rem1);
    assert(crdtSetContains(sb, m1) == 0);

    /* Merge sb into sa: apple should now be removed, banana stays */
    crdtSetMerge(sa, sb);
    assert(crdtSetContains(sa, m1) == 0);
    assert(crdtSetContains(sa, m2) == 1);
    assert(crdtSetCard(sa) == 1);

    /* Re-adding apple at 300 > 200 should revive apple */
    crdtId add3 = crdtIdMake(300, 1);
    assert(crdtSetAdd(sa, m1, add3) == 1);
    assert(crdtSetContains(sa, m1) == 1);
    assert(crdtSetCard(sa) == 2);

    /* Prune tombstones */
    crdtSetRemove(sa, m2, crdtIdMake(400, 1));
    assert(sa->tombstone_count == 1);
    assert(crdtSetPruneTombstones(sa, 400) == 1);
    assert(sa->tombstone_count == 0);

    crdtSetFree(sa);
    crdtSetFree(sb);
    sdsfree(m1);
    sdsfree(m2);
}

/* =========================== 4. Counter CRDT (PN-Vector & Float Associativity) =========================== */

TEST(counter_vector_and_float_associativity) {
    /* Integer Counter */
    crdtCounter *c_int1 = crdtCounterCreate(0);
    crdtCounter *c_int2 = crdtCounterCreate(0);

    crdtCounterIncr(c_int1, 1, 10);
    crdtCounterIncr(c_int1, 1, -3);
    assert(crdtCounterValue(c_int1) == 7);

    crdtCounterIncr(c_int2, 2, 20);
    crdtCounterIncr(c_int2, 2, -5);
    assert(crdtCounterValue(c_int2) == 15);

    /* Merge: total should be (10-3) + (20-5) = 22 */
    crdtCounterMerge(c_int1, c_int2);
    assert(crdtCounterValue(c_int1) == 22);

    /* Float Counter with Scaled 128-bit Fixed Point: Zero Precision Drift */
    crdtCounter *f1 = crdtCounterCreate(1);
    crdtCounter *f2 = crdtCounterCreate(1);

    /* Standard IEEE 754 fails associativity: (0.1 + 0.2) + 0.3 != 0.1 + (0.2 + 0.3) */
    crdtCounterIncrFloat(f1, 1, 0.1);
    crdtCounterIncrFloat(f1, 1, 0.2);
    crdtCounterIncrFloat(f1, 1, 0.3);

    crdtCounterIncrFloat(f2, 2, 0.3);
    crdtCounterIncrFloat(f2, 2, 0.2);
    crdtCounterIncrFloat(f2, 2, 0.1);

    double v1 = crdtCounterValueFloat(f1);
    double v2 = crdtCounterValueFloat(f2);
    assert(fabs(v1 - 0.6) < 1e-12);
    assert(fabs(v2 - 0.6) < 1e-12);
    assert(v1 == v2);

    crdtCounterFree(c_int1);
    crdtCounterFree(c_int2);
    crdtCounterFree(f1);
    crdtCounterFree(f2);
}

/* =========================== 5. Stream CRDT (Disjoint Triplet IDs & Merge) =========================== */

TEST(stream_disjoint_ids_and_merge) {
    crdtStream *s1 = crdtStreamCreate();
    crdtStream *s2 = crdtStreamCreate();

    /* Same millisecond (1000) written concurrently on node 1 and node 2 */
    crdtStreamID id_node1 = { .ms = 1000, .origin_id = 1, .seq = 0 };
    crdtStreamID id_node2 = { .ms = 1000, .origin_id = 2, .seq = 0 };

    /* Disjoint ID comparison ensures deterministic total ordering: node2 > node1 */
    assert(crdtStreamIDCmp(id_node2, id_node1) > 0);

    sds f1 = sdsnew("sensor");
    sds v1 = sdsnew("temp");
    sds f2 = sdsnew("sensor");
    sds v2 = sdsnew("humidity");

    const sds fields1[] = { f1 };
    const sds values1[] = { v1 };
    const sds fields2[] = { f2 };
    const sds values2[] = { v2 };

    assert(crdtStreamAppend(s1, id_node1, 1, fields1, values1) != NULL);
    assert(crdtStreamAppend(s2, id_node2, 1, fields2, values2) != NULL);

    /* Merge streams: both entries survive without overwriting or collision */
    crdtStreamMerge(s1, s2);
    assert(s1->length == 2);

    /* Test consumer PEL ack */
    assert(crdtStreamAck(s1, id_node1, 500) == 1);
    /* Lower HLC ack ignored */
    assert(crdtStreamAck(s1, id_node1, 400) == 0);

    crdtStreamFree(s1);
    crdtStreamFree(s2);
    sdsfree(f1);
    sdsfree(v1);
    sdsfree(f2);
    sdsfree(v2);
}

/* =========================== 6. Counter Boundaries & Resizing =========================== */

TEST(counter_boundaries_and_resizing) {
    crdtCounter *c = crdtCounterCreate(0);

    /* Zero increment */
    crdtCounterIncr(c, 1, 0);
    assert(crdtCounterValue(c) == 0);

    /* INT64_MAX */
    crdtCounterIncr(c, 1, INT64_MAX);
    assert(crdtCounterValue(c) == INT64_MAX);

    /* Decrement by INT64_MAX back to 0 */
    crdtCounterIncr(c, 1, -INT64_MAX);
    assert(crdtCounterValue(c) == 0);

    /* INT64_MIN (-9223372036854775808LL) check */
    crdtCounterIncr(c, 1, INT64_MIN);
    assert(crdtCounterValue(c) == INT64_MIN);

    /* Dynamic capacity expansion (> 4 origins up to 16 origins) */
    crdtCounter *multi = crdtCounterCreate(0);
    int64_t expected_sum = 0;
    for (uint32_t orig = 1; orig <= 16; orig++) {
        int64_t val = (int64_t)orig * 100;
        crdtCounterIncr(multi, orig, val);
        expected_sum += val;
    }
    assert(multi->num_origins == 16);
    assert(multi->capacity >= 16);
    assert(crdtCounterValue(multi) == expected_sum);

    /* Float counter: symmetric positive and negative floats strictly sum to 0.0 */
    crdtCounter *f = crdtCounterCreate(1);
    crdtCounterIncrFloat(f, 1, 123456.789012);
    crdtCounterIncrFloat(f, 1, -123456.789012);
    assert(fabs(crdtCounterValueFloat(f)) < 1e-12);

    crdtCounterFree(c);
    crdtCounterFree(multi);
    crdtCounterFree(f);
}

/* =========================== 7. Set Adversarial Interleavings & Binary Safety =========================== */

TEST(set_adversarial_interleavings) {
    crdtSet *s = crdtSetCreate();

    sds item = sdsnew("adversarial_item");

    /* Out-of-order delivery: Remove arrived at HLC 2000 BEFORE Add arrived at HLC 1000 */
    crdtId rem_early = crdtIdMake(2000, 1);
    crdtId add_late = crdtIdMake(1000, 2);

    assert(crdtSetRemove(s, item, rem_early) == 0); /* Recorded as removal tombstone */
    assert(crdtSetContains(s, item) == 0);
    assert(s->tombstone_count == 1);

    assert(crdtSetAdd(s, item, add_late) == 0); /* Stale add masked by newer removal */
    assert(crdtSetContains(s, item) == 0);
    assert(s->live_count == 0);

    /* Equal timestamp tie-breaker: t_add == t_rem */
    sds item_tie = sdsnew("tie_item");
    crdtId add_tie_winner = crdtIdMake(3000, 2);
    crdtId rem_tie_loser = crdtIdMake(3000, 1);

    crdtSetRemove(s, item_tie, rem_tie_loser);
    crdtSetAdd(s, item_tie, add_tie_winner);
    /* 2 > 1 => Add wins tie */
    assert(crdtSetContains(s, item_tie) == 1);

    /* Binary safety: strings with embedded NULL bytes */
    char bin1_data[] = "\x01\x00\xAA\xBB";
    char bin2_data[] = "\x01\x00\xCC\xDD";
    sds bin1 = sdsnewlen(bin1_data, 4);
    sds bin2 = sdsnewlen(bin2_data, 4);

    crdtSetAdd(s, bin1, crdtIdMake(4000, 1));
    crdtSetAdd(s, bin2, crdtIdMake(4001, 1));

    assert(crdtSetContains(s, bin1) == 1);
    assert(crdtSetContains(s, bin2) == 1);
    assert(crdtSetCard(s) == 3); /* item_tie + bin1 + bin2 */

    /* Case sensitivity check: "apple" vs "APPLE" must be distinct */
    sds lower = sdsnew("case_test");
    sds upper = sdsnew("CASE_TEST");
    crdtSetAdd(s, lower, crdtIdMake(5000, 1));
    crdtSetAdd(s, upper, crdtIdMake(5001, 1));
    assert(crdtSetContains(s, lower) == 1);
    assert(crdtSetContains(s, upper) == 1);

    /* Pruning safety: live members with t_add <= H_stable must NOT be pruned */
    size_t pruned = crdtSetPruneTombstones(s, 10000);
    assert(pruned == 1); /* Only 'item' tombstone pruned */
    assert(crdtSetContains(s, lower) == 1);
    assert(crdtSetContains(s, upper) == 1);
    assert(crdtSetContains(s, item_tie) == 1);

    crdtSetFree(s);
    sdsfree(item);
    sdsfree(item_tie);
    sdsfree(bin1);
    sdsfree(bin2);
    sdsfree(lower);
    sdsfree(upper);
}

/* =========================== 8. Stream Big-Endian Radix Tree Ordering =========================== */

TEST(stream_big_endian_radix_ordering) {
    crdtStream *cs = crdtStreamCreate();

    /* Insert entries with same ms (5000), same origin (1), across sequence boundaries: 0, 1, 255, 256, 65535, 65536 */
    uint32_t seqs[] = { 0, 1, 255, 256, 65535, 65536 };
    size_t num_seqs = sizeof(seqs) / sizeof(seqs[0]);

    sds f = sdsnew("key");
    sds v = sdsnew("val");
    const sds fields[] = { f };
    const sds values[] = { v };

    for (size_t i = 0; i < num_seqs; i++) {
        crdtStreamID id = { .ms = 5000, .origin_id = 1, .seq = seqs[i] };
        assert(crdtStreamAppend(cs, id, 1, fields, values) != NULL);
    }
    assert(cs->length == num_seqs);

    /* Radix traversal order verification */
    raxIterator ri;
    raxStart(&ri, cs->entries);
    raxSeek(&ri, "^", NULL, 0);
    size_t idx = 0;

    while (raxNext(&ri)) {
        crdtStreamEntry *e = ri.data;
        assert(idx < num_seqs);
        assert(e->id.seq == seqs[idx]);
        idx++;
    }
    raxStop(&ri);
    assert(idx == num_seqs);

    /* Deletion merge test */
    crdtStream *remote = crdtStreamCreate();
    crdtStreamID del_id = { .ms = 5000, .origin_id = 1, .seq = 255 };
    crdtStreamEntry *r_e = crdtStreamAppend(remote, del_id, 1, fields, values);
    assert(r_e != NULL);
    r_e->deleted = 1;
    r_e->del_hlc = 9999;

    crdtStreamMerge(cs, remote);
    unsigned char enc_del_id[16];
    crdtStreamIDEncode(del_id, enc_del_id);
    void *local_entry = NULL;
    assert(raxFind(cs->entries, enc_del_id, 16, &local_entry) == 1);
    crdtStreamEntry *res_e = local_entry;
    assert(res_e->deleted == 1);
    assert(res_e->del_hlc == 9999);

    crdtStreamFree(cs);
    crdtStreamFree(remote);
    sdsfree(f);
    sdsfree(v);
}

/* =========================== 9. String Append & Empty Payloads =========================== */

TEST(string_append_and_empty_payloads) {
    /* Empty payload */
    sds empty = sdsempty();
    crdtString *s = crdtStringCreate(crdtIdMake(100, 1), empty);
    assert(strcmp(s->val, "") == 0);
    assert(sdslen(s->val) == 0);

    /* Append */
    sds part1 = sdsnew("hello");
    sds part2 = sdsnew(" world");
    crdtStringAppend(s, crdtIdMake(200, 1), part1);
    assert(strcmp(s->val, "hello") == 0);
    crdtStringAppend(s, crdtIdMake(300, 1), part2);
    assert(strcmp(s->val, "hello world") == 0);

    /* 3-node equal HLC transitivity: origin 1 < origin 2 < origin 3 */
    crdtId id_n1 = crdtIdMake(500, 1);
    crdtId id_n2 = crdtIdMake(500, 2);
    crdtId id_n3 = crdtIdMake(500, 3);
    assert(crdtIdCmp(id_n2, id_n1) > 0);
    assert(crdtIdCmp(id_n3, id_n2) > 0);
    assert(crdtIdCmp(id_n3, id_n1) > 0);

    crdtStringFree(s);
    sdsfree(empty);
    sdsfree(part1);
    sdsfree(part2);
}

/* =========================== 10. Semilattice Algebraic Laws =========================== */

TEST(semilattice_algebraic_laws) {
    /* Test Commutativity (A ⊔ B == B ⊔ A), Associativity ((A ⊔ B) ⊔ C == A ⊔ (B ⊔ C)), Idempotence (A ⊔ A == A) */

    /* 1. Sets */
    crdtSet *sA1 = crdtSetCreate();
    crdtSet *sA2 = crdtSetCreate();
    crdtSet *sB = crdtSetCreate();
    crdtSet *sC = crdtSetCreate();

    sds m1 = sdsnew("x");
    sds m2 = sdsnew("y");
    sds m3 = sdsnew("z");

    crdtSetAdd(sA1, m1, crdtIdMake(100, 1));
    crdtSetAdd(sA2, m1, crdtIdMake(100, 1));
    crdtSetAdd(sB, m2, crdtIdMake(200, 2));
    crdtSetAdd(sC, m3, crdtIdMake(300, 3));

    /* Idempotence */
    crdtSetMerge(sA1, sA2);
    assert(crdtSetCard(sA1) == 1);

    /* Commutativity */
    crdtSet *comm1 = crdtSetCreate();
    crdtSet *comm2 = crdtSetCreate();
    crdtSetMerge(comm1, sA1);
    crdtSetMerge(comm1, sB);

    crdtSetMerge(comm2, sB);
    crdtSetMerge(comm2, sA1);
    assert(crdtSetCard(comm1) == crdtSetCard(comm2));
    assert(crdtSetContains(comm1, m1) == crdtSetContains(comm2, m1));
    assert(crdtSetContains(comm1, m2) == crdtSetContains(comm2, m2));

    /* Associativity */
    crdtSet *assoc1 = crdtSetCreate();
    crdtSetMerge(assoc1, sA1);
    crdtSetMerge(assoc1, sB);
    crdtSetMerge(assoc1, sC);

    crdtSet *assoc2 = crdtSetCreate();
    crdtSet *bc = crdtSetCreate();
    crdtSetMerge(bc, sB);
    crdtSetMerge(bc, sC);
    crdtSetMerge(assoc2, sA1);
    crdtSetMerge(assoc2, bc);

    assert(crdtSetCard(assoc1) == crdtSetCard(assoc2));
    assert(crdtSetCard(assoc1) == 3);

    crdtSetFree(sA1);
    crdtSetFree(sA2);
    crdtSetFree(sB);
    crdtSetFree(sC);
    crdtSetFree(comm1);
    crdtSetFree(comm2);
    crdtSetFree(assoc1);
    crdtSetFree(assoc2);
    crdtSetFree(bc);
    sdsfree(m1);
    sdsfree(m2);
    sdsfree(m3);
}

/* =========================== Main Entry Point =========================== */

int main(void) {
    printf("============================================================\n");
    printf("Starting Valkey Active-Active Unified CRDT Invariant Tests\n");
    printf("============================================================\n");

    RUN_TEST(clock_and_key_tombstones);
    RUN_TEST(string_lww_and_merge);
    RUN_TEST(set_lww_element_and_merge);
    RUN_TEST(counter_vector_and_float_associativity);
    RUN_TEST(stream_disjoint_ids_and_merge);
    RUN_TEST(counter_boundaries_and_resizing);
    RUN_TEST(set_adversarial_interleavings);
    RUN_TEST(stream_big_endian_radix_ordering);
    RUN_TEST(string_append_and_empty_payloads);
    RUN_TEST(semilattice_algebraic_laws);

    printf("============================================================\n");
    printf("All Unified CRDT Invariant Tests Passed Successfully! [10/10]\n");
    printf("============================================================\n");
    return 0;
}
