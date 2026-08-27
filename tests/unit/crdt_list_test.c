/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT List Engine Unit Test Suite
 *
 * Verifies:
 * - HLC Clock generation, counter saturation borrowing, backpressure, and skew clamping.
 * - Basic push/pop/delete/range operations and index lookups.
 * - Mathematical CRDT properties: Commutativity, Associativity, Idempotence.
 * - Concurrent delete & insert resilience on tombstoned parent anchors.
 * - Multi-replica random fuzzing convergence.
 */

#include "../../src/crdt_clock.h"
#include "../../src/crdt_list.h"
#include "../../src/sds.h"
#include "../../src/zmalloc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <inttypes.h>

/* Standalone server assert mocks for sds/rax */
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

static int tests_run = 0;
static int tests_passed = 0;
static int assertions_run = 0;

#define ANSI_GREEN "\033[0;32m"
#define ANSI_RED   "\033[0;31m"
#define ANSI_BLUE  "\033[0;34m"
#define ANSI_BOLD  "\033[1m"
#define ANSI_RESET "\033[0m"

#define TEST_ASSERT(expr) do { \
    assertions_run++; \
    if (!(expr)) { \
        printf(ANSI_RED "  [FAIL] Assertion failed at %s:%d: %s\n" ANSI_RESET, __FILE__, __LINE__, #expr); \
        return 0; \
    } \
} while (0)

#define TEST_ASSERT_EQUAL(a, b) do { \
    assertions_run++; \
    if ((a) != (b)) { \
        printf(ANSI_RED "  [FAIL] Equality failed at %s:%d: %s (%ld) != %s (%ld)\n" ANSI_RESET, \
               __FILE__, __LINE__, #a, (long)(a), #b, (long)(b)); \
        return 0; \
    } \
} while (0)

#define TEST_ASSERT_STR_EQUAL(a, b) do { \
    assertions_run++; \
    if (strcmp((a), (b)) != 0) { \
        printf(ANSI_RED "  [FAIL] String equality failed at %s:%d: \"%s\" != \"%s\"\n" ANSI_RESET, \
               __FILE__, __LINE__, (a), (b)); \
        return 0; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    tests_run++; \
    printf(ANSI_BLUE "==> Running %s...\n" ANSI_RESET, #fn); \
    if (fn()) { \
        tests_passed++; \
        printf(ANSI_GREEN "    PASS: %s\n" ANSI_RESET, #fn); \
    } else { \
        printf(ANSI_RED "    FAIL: %s\n" ANSI_RESET, #fn); \
    } \
} while (0)

/* Helper: Print list contents for diagnostics */
static void __attribute__((unused)) debug_dump_list(const char *label, crdtList *list) {
    printf("--- List Dump [%s] (len=%zu, tombstones=%zu, total=%zu) ---\n",
           label, list->length, list->tombstone_count, list->total_vertices);
    crdtListVertex *v = list->head->next;
    int idx = 0;
    while (v != NULL) {
        printf("  [%d] id=(hlc=%" PRIu64 ", origin=%u) parent=(hlc=%" PRIu64 ", origin=%u) deleted=%d val=\"%s\"\n",
               idx++, v->id.hlc, v->id.origin_id, v->parent_id.hlc, v->parent_id.origin_id,
               v->deleted, v->val ? v->val : "<null>");
        v = v->next;
    }
}

/* =========================================================================
 * 1. Hybrid Logical Clock (HLC) & CRDT ID Test Suite
 * ========================================================================= */

static int test_hlc_monotonicity(void) {
    crdtClock clock;
    crdtClockInit(&clock, 101, 5000, 10000);

    crdtId prev_id, cur_id;
    int bp = 0;

    int rc = hlc_now(&clock, 1000, &prev_id, &bp);
    TEST_ASSERT_EQUAL(rc, CRDT_CLOCK_OK);
    TEST_ASSERT_EQUAL(bp, 0);
    TEST_ASSERT_EQUAL(crdtHlcPhysical(prev_id.hlc), 1000);
    TEST_ASSERT_EQUAL(crdtHlcLogical(prev_id.hlc), 0);
    TEST_ASSERT_EQUAL(prev_id.origin_id, 101);

    /* Generate sequential timestamps in same physical millisecond */
    for (int i = 1; i <= 100; i++) {
        rc = hlc_now(&clock, 1000, &cur_id, &bp);
        TEST_ASSERT_EQUAL(rc, CRDT_CLOCK_OK);
        TEST_ASSERT_EQUAL(bp, 0);
        TEST_ASSERT_EQUAL(crdtHlcPhysical(cur_id.hlc), 1000);
        TEST_ASSERT_EQUAL(crdtHlcLogical(cur_id.hlc), i);
        TEST_ASSERT(crdtIdCmp(cur_id, prev_id) > 0);
        prev_id = cur_id;
    }

    /* Physical clock advances */
    rc = hlc_now(&clock, 1005, &cur_id, &bp);
    TEST_ASSERT_EQUAL(rc, CRDT_CLOCK_OK);
    TEST_ASSERT_EQUAL(crdtHlcPhysical(cur_id.hlc), 1005);
    TEST_ASSERT_EQUAL(crdtHlcLogical(cur_id.hlc), 0);
    TEST_ASSERT(crdtIdCmp(cur_id, prev_id) > 0);

    return 1;
}

static int test_hlc_counter_saturation_and_borrowing(void) {
    crdtClock clock;
    crdtClockInit(&clock, 102, 10, 5000); /* max_borrow_ms = 10 ms */

    /* Set clock state right before counter saturation */
    clock.last_physical_ms = 2000;
    clock.logical_counter = 0xFFFE;

    crdtId id;
    int bp = 0;

    /* Hit 0xFFFF */
    int rc = hlc_now(&clock, 2000, &id, &bp);
    TEST_ASSERT_EQUAL(rc, CRDT_CLOCK_OK);
    TEST_ASSERT_EQUAL(bp, 0);
    TEST_ASSERT_EQUAL(crdtHlcPhysical(id.hlc), 2000);
    TEST_ASSERT_EQUAL(crdtHlcLogical(id.hlc), 0xFFFF);

    /* Next operation triggers counter saturation -> forward physical borrow (+1 ms) */
    rc = hlc_now(&clock, 2000, &id, &bp);
    TEST_ASSERT_EQUAL(rc, CRDT_CLOCK_OK);
    TEST_ASSERT_EQUAL(bp, 0);
    TEST_ASSERT_EQUAL(crdtHlcPhysical(id.hlc), 2001);
    TEST_ASSERT_EQUAL(crdtHlcLogical(id.hlc), 0);

    /* Borrow beyond max_borrow_ms (10 ms) -> triggers backpressure notification */
    for (int i = 0; i < 11; i++) {
        clock.logical_counter = 0xFFFF;
        rc = hlc_now(&clock, 2000, &id, &bp);
    }
    TEST_ASSERT_EQUAL(rc, CRDT_CLOCK_BACKPRESSURE);
    TEST_ASSERT_EQUAL(bp, 1);
    TEST_ASSERT(crdtHlcPhysical(id.hlc) >= 2011);

    return 1;
}

static int test_hlc_recv_and_skew_clamping(void) {
    crdtClock clock;
    crdtClockInit(&clock, 201, 5000, 100); /* max_skew_ms = 100 ms */

    crdtId local_id;
    hlc_now(&clock, 1000, &local_id, NULL);

    /* 1. Receive remote timestamp with acceptable skew (1050 ms <= 1000 + 100) */
    crdtId valid_remote = crdtIdMake(crdtHlcCompose(1050, 5), 202);
    int rc = hlc_recv(&clock, 1000, valid_remote);
    TEST_ASSERT_EQUAL(rc, CRDT_CLOCK_OK);
    TEST_ASSERT_EQUAL(clock.last_physical_ms, 1050);
    TEST_ASSERT_EQUAL(clock.logical_counter, 6);

    /* 2. Receive remote timestamp exceeding max_skew_ms (1200 ms > 1000 + 100) -> Rejected */
    crdtId skewed_remote = crdtIdMake(crdtHlcCompose(1200, 0), 203);
    rc = hlc_recv(&clock, 1000, skewed_remote);
    TEST_ASSERT_EQUAL(rc, CRDT_CLOCK_ERR_SKEW);
    /* Local clock must not advance on rejected message */
    TEST_ASSERT_EQUAL(clock.last_physical_ms, 1050);

    return 1;
}

static int test_crdtid_encoding_decoding_and_cmp(void) {
    crdtId id1 = crdtIdMake(0x0102030405060708ULL, 0x0A0B0C0D);
    unsigned char buf[12];
    crdtIdEncode(id1, buf);

    /* Verify big-endian byte layout */
    TEST_ASSERT_EQUAL(buf[0], 0x01);
    TEST_ASSERT_EQUAL(buf[1], 0x02);
    TEST_ASSERT_EQUAL(buf[7], 0x08);
    TEST_ASSERT_EQUAL(buf[8], 0x0A);
    TEST_ASSERT_EQUAL(buf[11], 0x0D);

    crdtId decoded = crdtIdDecode(buf);
    TEST_ASSERT_EQUAL(decoded.hlc, id1.hlc);
    TEST_ASSERT_EQUAL(decoded.origin_id, id1.origin_id);
    TEST_ASSERT_EQUAL(crdtIdEqual(id1, decoded), 1);

    /* Test total order comparison */
    crdtId a = crdtIdMake(100, 1);
    crdtId b = crdtIdMake(100, 2);
    crdtId c = crdtIdMake(101, 1);

    TEST_ASSERT(crdtIdCmp(a, a) == 0);
    TEST_ASSERT(crdtIdCmp(a, b) < 0);
    TEST_ASSERT(crdtIdCmp(b, a) > 0);
    TEST_ASSERT(crdtIdCmp(b, c) < 0);
    TEST_ASSERT(crdtIdCmp(c, b) > 0);

    return 1;
}

/* =========================================================================
 * 2. Basic List Operations Test Suite
 * ========================================================================= */

static int test_basic_push_pop_range_delete(void) {
    crdtList *list = crdtListCreate();
    TEST_ASSERT(list != NULL);
    TEST_ASSERT_EQUAL(crdtListLength(list), 0);
    TEST_ASSERT_EQUAL(list->tombstone_count, 0);

    crdtClock clock;
    crdtClockInit(&clock, 1, 5000, 10000);

    /* Push tail elements */
    sds s1 = sdsnew("apple");
    sds s2 = sdsnew("banana");
    sds s3 = sdsnew("cherry");

    crdtListVertex *v1 = crdtListPushTail(list, &clock, s1);
    crdtListVertex *v2 = crdtListPushTail(list, &clock, s2);
    crdtListVertex *v3 = crdtListPushTail(list, &clock, s3);

    sdsfree(s1);
    sdsfree(s2);
    sdsfree(s3);

    TEST_ASSERT(v1 != NULL && v2 != NULL && v3 != NULL);
    TEST_ASSERT_EQUAL(crdtListLength(list), 3);

    /* Visible index checks */
    crdtListVertex *idx0 = crdtListGetVisibleIndex(list, 0);
    crdtListVertex *idx1 = crdtListGetVisibleIndex(list, 1);
    crdtListVertex *idx2 = crdtListGetVisibleIndex(list, 2);
    crdtListVertex *idx_last = crdtListGetVisibleIndex(list, -1);
    crdtListVertex *idx_prev = crdtListGetVisibleIndex(list, -2);

    TEST_ASSERT_STR_EQUAL(idx0->val, "apple");
    TEST_ASSERT_STR_EQUAL(idx1->val, "banana");
    TEST_ASSERT_STR_EQUAL(idx2->val, "cherry");
    TEST_ASSERT_STR_EQUAL(idx_last->val, "cherry");
    TEST_ASSERT_STR_EQUAL(idx_prev->val, "banana");

    /* Out of bounds visible index checks */
    TEST_ASSERT(crdtListGetVisibleIndex(list, 3) == NULL);
    TEST_ASSERT(crdtListGetVisibleIndex(list, -4) == NULL);

    /* Range query [0, -1] */
    size_t range_len = 0;
    sds *range = crdtListGetVisibleRange(list, 0, -1, &range_len);
    TEST_ASSERT_EQUAL(range_len, 3);
    TEST_ASSERT_STR_EQUAL(range[0], "apple");
    TEST_ASSERT_STR_EQUAL(range[1], "banana");
    TEST_ASSERT_STR_EQUAL(range[2], "cherry");
    crdtListFreeRange(range, range_len);

    /* Range query slice [1, 2] */
    range = crdtListGetVisibleRange(list, 1, 2, &range_len);
    TEST_ASSERT_EQUAL(range_len, 2);
    TEST_ASSERT_STR_EQUAL(range[0], "banana");
    TEST_ASSERT_STR_EQUAL(range[1], "cherry");
    crdtListFreeRange(range, range_len);

    /* Delete middle element ("banana") */
    crdtId del_id;
    hlc_now(&clock, 0, &del_id, NULL);
    int del_rc = crdtListDeleteVertex(list, v2->id, del_id.hlc, clock.origin_id);
    TEST_ASSERT_EQUAL(del_rc, 1);
    TEST_ASSERT_EQUAL(crdtListLength(list), 2);
    TEST_ASSERT_EQUAL(list->tombstone_count, 1);

    /* Idempotent delete on already deleted vertex */
    del_rc = crdtListDeleteVertex(list, v2->id, del_id.hlc, clock.origin_id);
    TEST_ASSERT_EQUAL(del_rc, 2);
    TEST_ASSERT_EQUAL(crdtListLength(list), 2);

    /* Visible range after deletion should now be ["apple", "cherry"] */
    range = crdtListGetVisibleRange(list, 0, -1, &range_len);
    TEST_ASSERT_EQUAL(range_len, 2);
    TEST_ASSERT_STR_EQUAL(range[0], "apple");
    TEST_ASSERT_STR_EQUAL(range[1], "cherry");
    crdtListFreeRange(range, range_len);

    /* Pop head -> removes "apple" */
    sds popped = NULL;
    int pop_rc = crdtListPopHead(list, &clock, &popped);
    TEST_ASSERT_EQUAL(pop_rc, 1);
    TEST_ASSERT_STR_EQUAL(popped, "apple");
    sdsfree(popped);
    TEST_ASSERT_EQUAL(crdtListLength(list), 1);
    TEST_ASSERT_EQUAL(list->tombstone_count, 2);

    /* Pop tail -> removes "cherry" */
    popped = NULL;
    pop_rc = crdtListPopTail(list, &clock, &popped);
    TEST_ASSERT_EQUAL(pop_rc, 1);
    TEST_ASSERT_STR_EQUAL(popped, "cherry");
    sdsfree(popped);
    TEST_ASSERT_EQUAL(crdtListLength(list), 0);
    TEST_ASSERT_EQUAL(list->tombstone_count, 3);

    /* Pop on empty list */
    pop_rc = crdtListPopHead(list, &clock, &popped);
    TEST_ASSERT_EQUAL(pop_rc, 0);

    crdtListRelease(list);
    return 1;
}

/* =========================================================================
 * 3. Commutativity Test Suite
 * ========================================================================= */

static int test_commutativity_concurrent_inserts(void) {
    /* Scenario:
     * Base list has root element "root".
     * Node 1 creates OpA ("alpha", hlc=100, origin=1).
     * Node 2 creates OpB ("beta",  hlc=150, origin=2).
     * Node 3 creates OpC ("gamma", hlc=150, origin=3).
     *
     * Node 1 applies: OpA, then OpB, then OpC.
     * Node 2 applies: OpB, then OpC, then OpA.
     * Node 3 applies: OpC, then OpA, then OpB.
     *
     * All 3 nodes must reach the exact same vertex sequence:
     * Sibling order (descending HLC, tie-break by origin_id):
     * OpC (hlc=150, origin=3) > OpB (hlc=150, origin=2) > OpA (hlc=100, origin=1)
     */
    crdtId root_id = crdtIdMake(10, 1);
    crdtId idA = crdtIdMake(100, 1);
    crdtId idB = crdtIdMake(150, 2);
    crdtId idC = crdtIdMake(150, 3);

    sds s_root = sdsnew("root");
    sds s_a = sdsnew("alpha");
    sds s_b = sdsnew("beta");
    sds s_c = sdsnew("gamma");

    /* Replica 1 */
    crdtList *l1 = crdtListCreate();
    crdtListInsertAfter(l1, (crdtId){0,0}, root_id, s_root);
    crdtListInsertAfter(l1, root_id, idA, s_a);
    crdtListInsertAfter(l1, root_id, idB, s_b);
    crdtListInsertAfter(l1, root_id, idC, s_c);

    /* Replica 2 */
    crdtList *l2 = crdtListCreate();
    crdtListInsertAfter(l2, (crdtId){0,0}, root_id, s_root);
    crdtListInsertAfter(l2, root_id, idB, s_b);
    crdtListInsertAfter(l2, root_id, idC, s_c);
    crdtListInsertAfter(l2, root_id, idA, s_a);

    /* Replica 3 */
    crdtList *l3 = crdtListCreate();
    crdtListInsertAfter(l3, (crdtId){0,0}, root_id, s_root);
    crdtListInsertAfter(l3, root_id, idC, s_c);
    crdtListInsertAfter(l3, root_id, idA, s_a);
    crdtListInsertAfter(l3, root_id, idB, s_b);

    /* Verify all 3 replicas are pairwise identical */
    TEST_ASSERT_EQUAL(crdtListEquals(l1, l2), 1);
    TEST_ASSERT_EQUAL(l1->length, 4);
    TEST_ASSERT_EQUAL(crdtListEquals(l2, l3), 1);

    /* Verify exact sequence: ["root", "gamma", "beta", "alpha"] */
    size_t len = 0;
    sds *res = crdtListGetVisibleRange(l1, 0, -1, &len);
    TEST_ASSERT_EQUAL(len, 4);
    TEST_ASSERT_STR_EQUAL(res[0], "root");
    TEST_ASSERT_STR_EQUAL(res[1], "gamma");
    TEST_ASSERT_STR_EQUAL(res[2], "beta");
    TEST_ASSERT_STR_EQUAL(res[3], "alpha");
    crdtListFreeRange(res, len);

    sdsfree(s_root);
    sdsfree(s_a);
    sdsfree(s_b);
    sdsfree(s_c);

    crdtListRelease(l1);
    crdtListRelease(l2);
    crdtListRelease(l3);
    return 1;
}

static int test_commutativity_complex_tree_permutations(void) {
    /* Scenario with tree branching and descendants:
     * Node 1 creates chain: A -> B -> C
     * Node 2 creates competing subtree: A -> D -> E
     * Node 3 creates sibling: A -> F
     * Apply in 6 distinct arrival permutations across 6 independent lists.
     */
    crdtId idA = crdtIdMake(10, 1);
    crdtId idB = crdtIdMake(20, 1);
    crdtId idC = crdtIdMake(30, 1);
    crdtId idD = crdtIdMake(25, 2);
    crdtId idE = crdtIdMake(35, 2);
    crdtId idF = crdtIdMake(22, 3);

    sds sA = sdsnew("A");
    sds sB = sdsnew("B");
    sds sC = sdsnew("C");
    sds sD = sdsnew("D");
    sds sE = sdsnew("E");
    sds sF = sdsnew("F");

    crdtList *lists[6];
    for (int i = 0; i < 6; i++) {
        lists[i] = crdtListCreate();
    }

    /* Permutation 0: A, B, C, D, E, F */
    crdtListInsertAfter(lists[0], (crdtId){0,0}, idA, sA);
    crdtListInsertAfter(lists[0], idA, idB, sB);
    crdtListInsertAfter(lists[0], idB, idC, sC);
    crdtListInsertAfter(lists[0], idA, idD, sD);
    crdtListInsertAfter(lists[0], idD, idE, sE);
    crdtListInsertAfter(lists[0], idA, idF, sF);

    /* Permutation 1: A, D, E, B, C, F */
    crdtListInsertAfter(lists[1], (crdtId){0,0}, idA, sA);
    crdtListInsertAfter(lists[1], idA, idD, sD);
    crdtListInsertAfter(lists[1], idD, idE, sE);
    crdtListInsertAfter(lists[1], idA, idB, sB);
    crdtListInsertAfter(lists[1], idB, idC, sC);
    crdtListInsertAfter(lists[1], idA, idF, sF);

    /* Permutation 2: A, F, D, B, E, C */
    crdtListInsertAfter(lists[2], (crdtId){0,0}, idA, sA);
    crdtListInsertAfter(lists[2], idA, idF, sF);
    crdtListInsertAfter(lists[2], idA, idD, sD);
    crdtListInsertAfter(lists[2], idA, idB, sB);
    crdtListInsertAfter(lists[2], idD, idE, sE);
    crdtListInsertAfter(lists[2], idB, idC, sC);

    /* Permutation 3: A, D, F, B, C, E */
    crdtListInsertAfter(lists[3], (crdtId){0,0}, idA, sA);
    crdtListInsertAfter(lists[3], idA, idD, sD);
    crdtListInsertAfter(lists[3], idA, idF, sF);
    crdtListInsertAfter(lists[3], idA, idB, sB);
    crdtListInsertAfter(lists[3], idB, idC, sC);
    crdtListInsertAfter(lists[3], idD, idE, sE);

    /* Permutation 4: A, B, D, F, C, E */
    crdtListInsertAfter(lists[4], (crdtId){0,0}, idA, sA);
    crdtListInsertAfter(lists[4], idA, idB, sB);
    crdtListInsertAfter(lists[4], idA, idD, sD);
    crdtListInsertAfter(lists[4], idA, idF, sF);
    crdtListInsertAfter(lists[4], idB, idC, sC);
    crdtListInsertAfter(lists[4], idD, idE, sE);

    /* Permutation 5: A, F, B, C, D, E */
    crdtListInsertAfter(lists[5], (crdtId){0,0}, idA, sA);
    crdtListInsertAfter(lists[5], idA, idF, sF);
    crdtListInsertAfter(lists[5], idA, idB, sB);
    crdtListInsertAfter(lists[5], idB, idC, sC);
    crdtListInsertAfter(lists[5], idA, idD, sD);
    crdtListInsertAfter(lists[5], idD, idE, sE);

    /* Check that all 6 permutations converged to the identical list */
    for (int i = 1; i < 6; i++) {
        TEST_ASSERT_EQUAL(crdtListEquals(lists[0], lists[i]), 1);
    }

    /* Expected order:
     * Children of A: D (hlc=25), F (hlc=22), B (hlc=20)
     * D's subtree: D -> E
     * F's subtree: F
     * B's subtree: B -> C
     * Complete sequence: A -> D -> E -> F -> B -> C
     */
    size_t len = 0;
    sds *res = crdtListGetVisibleRange(lists[0], 0, -1, &len);
    TEST_ASSERT_EQUAL(len, 6);
    TEST_ASSERT_STR_EQUAL(res[0], "A");
    TEST_ASSERT_STR_EQUAL(res[1], "D");
    TEST_ASSERT_STR_EQUAL(res[2], "E");
    TEST_ASSERT_STR_EQUAL(res[3], "F");
    TEST_ASSERT_STR_EQUAL(res[4], "B");
    TEST_ASSERT_STR_EQUAL(res[5], "C");
    crdtListFreeRange(res, len);

    sdsfree(sA);
    sdsfree(sB);
    sdsfree(sC);
    sdsfree(sD);
    sdsfree(sE);
    sdsfree(sF);

    for (int i = 0; i < 6; i++) {
        crdtListRelease(lists[i]);
    }
    return 1;
}

/* =========================================================================
 * 4. Associativity Test Suite
 * ========================================================================= */

static int test_associativity_join_semilattice(void) {
    /* Test: (A ⊔ B) ⊔ C == A ⊔ (B ⊔ C) */
    crdtClock clockA, clockB, clockC;
    crdtClockInit(&clockA, 1, 5000, 10000);
    crdtClockInit(&clockB, 2, 5000, 10000);
    crdtClockInit(&clockC, 3, 5000, 10000);

    /* Initial shared state */
    crdtList *shared = crdtListCreate();
    sds init_sds = sdsnew("initial");
    crdtListPushTail(shared, &clockA, init_sds);
    sdsfree(init_sds);

    /* Fork into replicas A, B, C */
    crdtList *repA = crdtListClone(shared);
    crdtList *repB = crdtListClone(shared);
    crdtList *repC = crdtListClone(shared);
    crdtListRelease(shared);

    /* Mutate A */
    sds sa1 = sdsnew("a_first");
    sds sa2 = sdsnew("a_second");
    crdtListPushTail(repA, &clockA, sa1);
    crdtListPushHead(repA, &clockA, sa2);
    sdsfree(sa1);
    sdsfree(sa2);

    /* Mutate B */
    sds sb1 = sdsnew("b_mid");
    crdtListPushTail(repB, &clockB, sb1);
    sdsfree(sb1);
    crdtListPopHead(repB, &clockB, NULL);

    /* Mutate C */
    sds sc1 = sdsnew("c_end");
    crdtListPushTail(repC, &clockC, sc1);
    sdsfree(sc1);

    /* Compute LHS: (A ⊔ B) ⊔ C */
    crdtList *lhs = crdtListClone(repA);
    crdtListMerge(lhs, repB);
    crdtListMerge(lhs, repC);

    /* Compute RHS: A ⊔ (B ⊔ C) */
    crdtList *b_cup_c = crdtListClone(repB);
    crdtListMerge(b_cup_c, repC);

    crdtList *rhs = crdtListClone(repA);
    crdtListMerge(rhs, b_cup_c);

    /* Assert exact structural and semantic equality */
    TEST_ASSERT_EQUAL(crdtListEquals(lhs, rhs), 1);

    crdtListRelease(repA);
    crdtListRelease(repB);
    crdtListRelease(repC);
    crdtListRelease(lhs);
    crdtListRelease(rhs);
    crdtListRelease(b_cup_c);
    return 1;
}

/* =========================================================================
 * 5. Idempotence Test Suite
 * ========================================================================= */

static int test_idempotence_join_semilattice(void) {
    /* Test: A ⊔ A == A and A ⊔ A ⊔ A == A */
    crdtClock clock;
    crdtClockInit(&clock, 1, 5000, 10000);

    crdtList *listA = crdtListCreate();
    sds s1 = sdsnew("item1");
    sds s2 = sdsnew("item2");
    sds s3 = sdsnew("item3");

    crdtListPushTail(listA, &clock, s1);
    crdtListPushTail(listA, &clock, s2);
    crdtListPushTail(listA, &clock, s3);
    crdtListPopHead(listA, &clock, NULL);

    sdsfree(s1);
    sdsfree(s2);
    sdsfree(s3);

    crdtList *clone = crdtListClone(listA);
    TEST_ASSERT_EQUAL(crdtListEquals(listA, clone), 1);

    /* Merge listA into itself */
    crdtListMerge(listA, clone);
    TEST_ASSERT_EQUAL(crdtListEquals(listA, clone), 1);

    /* Multiple merges */
    crdtListMerge(listA, clone);
    crdtListMerge(listA, clone);
    TEST_ASSERT_EQUAL(crdtListEquals(listA, clone), 1);

    crdtListRelease(listA);
    crdtListRelease(clone);
    return 1;
}

/* =========================================================================
 * 6. Concurrent Delete & Insert Test Suite
 * ========================================================================= */

static int test_concurrent_delete_and_insert(void) {
    /* Scenario 1:
     * Node 1 has item X.
     * Concurrently:
     * - Node 1 deletes item X (creates tombstone for X).
     * - Node 2 inserts item Y after item X (anchored to X).
     *
     * Merge:
     * - Node 1 receives insert(Y, parent=X). Since X exists in index as tombstone,
     *   Y is inserted after X.
     * - Node 2 receives delete(X). X becomes tombstone, Y remains visible.
     * - Both nodes reach: [X (tombstone), Y (visible)]. Visible list: ["Y"].
     */
    crdtClock clock1, clock2;
    crdtClockInit(&clock1, 1, 5000, 10000);
    crdtClockInit(&clock2, 2, 5000, 10000);

    crdtId idX = crdtIdMake(100, 1);
    crdtId idY = crdtIdMake(200, 2);

    sds sX = sdsnew("item_X");
    sds sY = sdsnew("item_Y");

    crdtList *n1 = crdtListCreate();
    crdtListInsertAfter(n1, (crdtId){0,0}, idX, sX);

    crdtList *n2 = crdtListClone(n1);

    /* Node 1 deletes X */
    crdtListDeleteVertex(n1, idX, 300, 1);
    TEST_ASSERT_EQUAL(crdtListLength(n1), 0);
    TEST_ASSERT_EQUAL(n1->tombstone_count, 1);

    /* Node 2 inserts Y after X */
    crdtListInsertAfter(n2, idX, idY, sY);
    TEST_ASSERT_EQUAL(crdtListLength(n2), 2);

    /* Merge N1 and N2 */
    crdtListMerge(n1, n2);
    crdtListMerge(n2, n1);

    TEST_ASSERT_EQUAL(crdtListEquals(n1, n2), 1);
    TEST_ASSERT_EQUAL(crdtListLength(n1), 1);
    TEST_ASSERT_EQUAL(n1->tombstone_count, 1);

    size_t len = 0;
    sds *vis = crdtListGetVisibleRange(n1, 0, -1, &len);
    TEST_ASSERT_EQUAL(len, 1);
    TEST_ASSERT_STR_EQUAL(vis[0], "item_Y");
    crdtListFreeRange(vis, len);

    sdsfree(sX);
    sdsfree(sY);
    crdtListRelease(n1);
    crdtListRelease(n2);

    /* Scenario 2:
     * Node 1 deletes X and inserts Z after X.
     * Node 2 inserts Y after X.
     * Merge -> X is tombstone, Y and Z ordered by HLC.
     */
    crdtId idZ = crdtIdMake(250, 1);
    sds sZ = sdsnew("item_Z");
    sX = sdsnew("item_X");
    sY = sdsnew("item_Y");

    n1 = crdtListCreate();
    crdtListInsertAfter(n1, (crdtId){0,0}, idX, sX);
    n2 = crdtListClone(n1);

    crdtListDeleteVertex(n1, idX, 300, 1);
    crdtListInsertAfter(n1, idX, idZ, sZ);

    crdtListInsertAfter(n2, idX, idY, sY);

    crdtListMerge(n1, n2);
    crdtListMerge(n2, n1);

    TEST_ASSERT_EQUAL(crdtListEquals(n1, n2), 1);
    TEST_ASSERT_EQUAL(crdtListLength(n1), 2);
    TEST_ASSERT_EQUAL(n1->tombstone_count, 1);

    vis = crdtListGetVisibleRange(n1, 0, -1, &len);
    TEST_ASSERT_EQUAL(len, 2);
    /* idZ (hlc=250) > idY (hlc=200) -> Z before Y */
    TEST_ASSERT_STR_EQUAL(vis[0], "item_Z");
    TEST_ASSERT_STR_EQUAL(vis[1], "item_Y");
    crdtListFreeRange(vis, len);

    sdsfree(sX);
    sdsfree(sY);
    sdsfree(sZ);
    crdtListRelease(n1);
    crdtListRelease(n2);
    return 1;
}

/* =========================================================================
 * 7. Multi-Replica Convergence Random Fuzzing Test Suite
 * ========================================================================= */

static int test_multi_replica_fuzzing_convergence(void) {
    const int NUM_REPLICAS = 5;
    const int OPS_PER_REPLICA = 100;

    crdtList *replicas[NUM_REPLICAS];
    crdtClock clocks[NUM_REPLICAS];

    for (int i = 0; i < NUM_REPLICAS; i++) {
        replicas[i] = crdtListCreate();
        crdtClockInit(&clocks[i], i + 1, 5000, 10000);
    }

    /* Seed shared root */
    sds root_val = sdsnew("origin_root");
    crdtListPushTail(replicas[0], &clocks[0], root_val);
    sdsfree(root_val);

    for (int i = 1; i < NUM_REPLICAS; i++) {
        crdtListMerge(replicas[i], replicas[0]);
    }

    srand(42);

    /* Run concurrent random mutations across replicas */
    for (int op = 0; op < OPS_PER_REPLICA; op++) {
        for (int r = 0; r < NUM_REPLICAS; r++) {
            int action = rand() % 3;
            if (action == 0 || replicas[r]->length == 0) {
                /* Insert after a random existing vertex */
                int pick = (replicas[r]->total_vertices > 0) ? (rand() % (int)replicas[r]->total_vertices) : 0;
                crdtListVertex *p = replicas[r]->head;
                for (int s = 0; s < pick && p->next != NULL; s++) {
                    p = p->next;
                }
                char buf[64];
                snprintf(buf, sizeof(buf), "val_r%d_op%d", r, op);
                sds val = sdsnew(buf);
                crdtId new_id;
                hlc_now(&clocks[r], 0, &new_id, NULL);
                crdtListInsertAfter(replicas[r], p->id, new_id, val);
                sdsfree(val);
            } else if (action == 1) {
                /* Push Head */
                char buf[64];
                snprintf(buf, sizeof(buf), "head_r%d_op%d", r, op);
                sds val = sdsnew(buf);
                crdtListPushHead(replicas[r], &clocks[r], val);
                sdsfree(val);
            } else {
                /* Delete a random visible item */
                if (replicas[r]->length > 0) {
                    long idx = rand() % (long)replicas[r]->length;
                    crdtListVertex *v = crdtListGetVisibleIndex(replicas[r], idx);
                    if (v) {
                        crdtId del_id;
                        hlc_now(&clocks[r], 0, &del_id, NULL);
                        crdtListDeleteVertex(replicas[r], v->id, del_id.hlc, clocks[r].origin_id);
                    }
                }
            }
        }
    }

    /* Full N-way cross-gossip merge */
    for (int iter = 0; iter < 2; iter++) {
        for (int i = 0; i < NUM_REPLICAS; i++) {
            for (int j = 0; j < NUM_REPLICAS; j++) {
                if (i != j) {
                    crdtListMerge(replicas[i], replicas[j]);
                }
            }
        }
    }

    /* Assert that ALL replicas achieved 100% bit-for-bit structural equality */
    for (int i = 1; i < NUM_REPLICAS; i++) {
        TEST_ASSERT_EQUAL(crdtListEquals(replicas[0], replicas[i]), 1);
    }

    printf(ANSI_GREEN "    [Convergence Verified] %d Replicas converged to identical list of len=%zu (tombstones=%zu, total=%zu)\n" ANSI_RESET,
           NUM_REPLICAS, replicas[0]->length, replicas[0]->tombstone_count, replicas[0]->total_vertices);

    for (int i = 0; i < NUM_REPLICAS; i++) {
        crdtListRelease(replicas[i]);
    }
    return 1;
}

/* =========================================================================
 * Suite 8: Dynamic Stability Horizon & Tombstone Garbage Collection
 * ========================================================================= */

static int test_tombstone_pruning_stability_horizon(void) {
    crdtList *list = crdtListCreate();
    TEST_ASSERT(list != NULL);

    crdtClock clock;
    crdtClockInit(&clock, 1, CRDT_DEFAULT_MAX_BORROW_MS, CRDT_DEFAULT_MAX_SKEW_MS);

    /* Insert 4 elements: V1 (100:1, "A"), V2 (200:1, "B"), V3 (300:1, "C"), V4 (400:1, "D") */
    crdtId id1 = crdtIdMake(100, 1);
    crdtId id2 = crdtIdMake(200, 1);
    crdtId id3 = crdtIdMake(300, 1);
    crdtId id4 = crdtIdMake(400, 1);

    crdtListInsertAfter(list, (crdtId){0,0}, id1, "A");
    crdtListInsertAfter(list, id1, id2, "B");
    crdtListInsertAfter(list, id2, id3, "C");
    crdtListInsertAfter(list, id3, id4, "D");

    TEST_ASSERT(list->length == 4);
    TEST_ASSERT(list->total_vertices == 4);
    TEST_ASSERT(list->tombstone_count == 0);

    /* Delete V2 at del_hlc=250 and V4 at del_hlc=450 */
    TEST_ASSERT(crdtListDeleteVertex(list, id2, 250, 1) == 1);
    TEST_ASSERT(crdtListDeleteVertex(list, id4, 450, 1) == 1);

    TEST_ASSERT(list->length == 2);
    TEST_ASSERT(list->tombstone_count == 2);
    TEST_ASSERT(list->total_vertices == 4);

    /* Note: V3 depends on V2 (parent_id = id2), but V3 is ALIVE!
     * So V2 must NOT be pruned even if H_stable >= 250 as long as V3 is alive! */
    size_t pruned = crdtListPruneTombstones(list, 300);
    /* V4 is a leaf (V4 has no children), del_hlc=450 > 300 -> V4 not pruned.
     * V2 has active child V3 -> V2 not pruned.
     * Total pruned = 0 */
    TEST_ASSERT(pruned == 0);
    TEST_ASSERT(list->tombstone_count == 2);
    TEST_ASSERT(list->total_vertices == 4);

    /* Prune with H_stable = 500:
     * V4 (del_hlc=450 <= 500) has NO children -> V4 is safely pruned!
     * V2 (del_hlc=250 <= 500) still has active child V3 -> V2 is preserved! */
    pruned = crdtListPruneTombstones(list, 500);
    TEST_ASSERT(pruned == 1);
    TEST_ASSERT(list->tombstone_count == 1);
    TEST_ASSERT(list->total_vertices == 3);
    TEST_ASSERT(list->length == 2);
    TEST_ASSERT(crdtListFindVertex(list, id4) == NULL); /* V4 removed from rax */
    TEST_ASSERT(crdtListFindVertex(list, id2) != NULL); /* V2 retained */

    /* Now delete V3 at del_hlc=600 */
    TEST_ASSERT(crdtListDeleteVertex(list, id3, 600, 1) == 1);
    TEST_ASSERT(list->length == 1);
    TEST_ASSERT(list->tombstone_count == 2); /* V2 and V3 */

    /* Prune with H_stable = 550:
     * V3 has del_hlc=600 > 550 -> V3 cannot be pruned.
     * V2 is parent of unpruned V3 -> V2 cannot be pruned either. */
    pruned = crdtListPruneTombstones(list, 550);
    TEST_ASSERT(pruned == 0);

    /* Prune with H_stable = 650:
     * V3 (del_hlc=600 <= 650) and V2 (del_hlc=250 <= 650) have no active descendants -> both pruned! */
    pruned = crdtListPruneTombstones(list, 650);
    TEST_ASSERT(pruned == 2);
    TEST_ASSERT(list->tombstone_count == 0);
    TEST_ASSERT(list->total_vertices == 1);
    TEST_ASSERT(list->length == 1);
    TEST_ASSERT(crdtListFindVertex(list, id2) == NULL);
    TEST_ASSERT(crdtListFindVertex(list, id3) == NULL);
    TEST_ASSERT(crdtListFindVertex(list, id1) != NULL);

    /* Verify visible element is still "A" */
    crdtListVertex *v1 = crdtListGetVisibleIndex(list, 0);
    TEST_ASSERT(v1 != NULL);
    TEST_ASSERT(strcmp(v1->val, "A") == 0);

    crdtListRelease(list);
    return 1;
}

static int test_tombstone_descendant_anchor_safety(void) {
    crdtList *list = crdtListCreate();
    TEST_ASSERT(list != NULL);

    /* Chain: Root -> V1 -> V2 -> V3 -> V4 */
    crdtId id1 = crdtIdMake(100, 1);
    crdtId id2 = crdtIdMake(200, 1);
    crdtId id3 = crdtIdMake(300, 1);
    crdtId id4 = crdtIdMake(400, 1);

    crdtListInsertAfter(list, (crdtId){0,0}, id1, "1");
    crdtListInsertAfter(list, id1, id2, "2");
    crdtListInsertAfter(list, id2, id3, "3");
    crdtListInsertAfter(list, id3, id4, "4");

    /* Delete V1, V2, V3 at del_hlc=500. V4 is active. */
    crdtListDeleteVertex(list, id1, 500, 1);
    crdtListDeleteVertex(list, id2, 500, 1);
    crdtListDeleteVertex(list, id3, 500, 1);

    TEST_ASSERT(list->length == 1);
    TEST_ASSERT(list->tombstone_count == 3);

    /* Attempt to prune with H_stable = 1000:
     * Even though V1, V2, V3 are deleted <= 1000, V4 is active!
     * Since V4 -> V3 -> V2 -> V1, all ancestors of V4 must be preserved. */
    size_t pruned = crdtListPruneTombstones(list, 1000);
    TEST_ASSERT(pruned == 0);
    TEST_ASSERT(list->tombstone_count == 3);
    TEST_ASSERT(list->total_vertices == 4);

    /* Now delete V4 at del_hlc=800 */
    crdtListDeleteVertex(list, id4, 800, 1);
    TEST_ASSERT(list->length == 0);
    TEST_ASSERT(list->tombstone_count == 4);

    /* Prune with H_stable = 600:
     * V4 has del_hlc=800 > 600 -> V4 cannot be pruned.
     * Ancestors V1, V2, V3 cannot be pruned either. */
    pruned = crdtListPruneTombstones(list, 600);
    TEST_ASSERT(pruned == 0);

    /* Prune with H_stable = 800:
     * All vertices are deleted with del_hlc <= 800 -> entire chain is pruned! */
    pruned = crdtListPruneTombstones(list, 800);
    TEST_ASSERT(pruned == 4);
    TEST_ASSERT(list->total_vertices == 0);
    TEST_ASSERT(list->tombstone_count == 0);
    TEST_ASSERT(list->length == 0);
    TEST_ASSERT(list->tail == list->head);
    TEST_ASSERT(list->head->next == NULL);

    crdtListRelease(list);
    return 1;
}

static int test_multi_branch_tombstone_pruning(void) {
    crdtList *list = crdtListCreate();
    TEST_ASSERT(list != NULL);

    /* Tree structure:
     * Head
     *   ├── V1 (100:1)
     *   │     ├── V1_child1 (200:1)
     *   │     └── V1_child2 (300:1)
     *   └── V2 (150:2)
     *         └── V2_child1 (250:2)
     */
    crdtId v1 = crdtIdMake(100, 1);
    crdtId v1_c1 = crdtIdMake(200, 1);
    crdtId v1_c2 = crdtIdMake(300, 1);
    crdtId v2 = crdtIdMake(150, 2);
    crdtId v2_c1 = crdtIdMake(250, 2);

    crdtListInsertAfter(list, (crdtId){0,0}, v1, "v1");
    crdtListInsertAfter(list, v1, v1_c1, "v1_c1");
    crdtListInsertAfter(list, v1, v1_c2, "v1_c2");
    crdtListInsertAfter(list, (crdtId){0,0}, v2, "v2");
    crdtListInsertAfter(list, v2, v2_c1, "v2_c1");

    TEST_ASSERT(list->length == 5);
    TEST_ASSERT(list->total_vertices == 5);

    /* Delete V1 and V1_child1 at del_hlc=400.
     * V1_child2 is ALIVE.
     * Delete V2 and V2_child1 at del_hlc=350.
     */
    crdtListDeleteVertex(list, v1, 400, 1);
    crdtListDeleteVertex(list, v1_c1, 400, 1);
    crdtListDeleteVertex(list, v2, 350, 2);
    crdtListDeleteVertex(list, v2_c1, 350, 2);

    TEST_ASSERT(list->length == 1); /* Only V1_child2 is alive */
    TEST_ASSERT(list->tombstone_count == 4);

    /* Prune with H_stable = 380:
     * V1 branch: V1 (del_hlc=400 > 380) not pruned; V1_child1 (del_hlc=400 > 380) not pruned; V1_child2 alive.
     * V2 branch: V2 (350 <= 380), V2_child1 (350 <= 380) -> Both pruned! */
    size_t pruned = crdtListPruneTombstones(list, 380);
    TEST_ASSERT(pruned == 2);
    TEST_ASSERT(list->tombstone_count == 2); /* V1 and V1_child1 */
    TEST_ASSERT(list->total_vertices == 3); /* V1, V1_c1, V1_c2 */
    TEST_ASSERT(crdtListFindVertex(list, v2) == NULL);
    TEST_ASSERT(crdtListFindVertex(list, v2_c1) == NULL);

    /* Prune with H_stable = 450:
     * V1_c1 (del_hlc=400 <= 450, no children) -> Pruned!
     * V1 (del_hlc=400 <= 450, has live child V1_c2) -> Preserved! */
    pruned = crdtListPruneTombstones(list, 450);
    TEST_ASSERT(pruned == 1);
    TEST_ASSERT(list->tombstone_count == 1); /* V1 */
    TEST_ASSERT(list->total_vertices == 2); /* V1 and V1_c2 */
    TEST_ASSERT(crdtListFindVertex(list, v1_c1) == NULL);
    TEST_ASSERT(crdtListFindVertex(list, v1) != NULL);

    /* Finally delete V1_c2 at del_hlc=500 */
    crdtListDeleteVertex(list, v1_c2, 500, 1);
    TEST_ASSERT(list->length == 0);
    TEST_ASSERT(list->tombstone_count == 2);

    /* Prune with H_stable = 500 -> both V1 and V1_c2 pruned! */
    pruned = crdtListPruneTombstones(list, 500);
    TEST_ASSERT(pruned == 2);
    TEST_ASSERT(list->total_vertices == 0);
    TEST_ASSERT(list->tombstone_count == 0);

    crdtListRelease(list);
    return 1;
}

/* =========================================================================
 * Main Runner
 * ========================================================================= */

int main(void) {
    printf(ANSI_BOLD "=========================================================\n" ANSI_RESET);
    printf(ANSI_BOLD "  Valkey Active-Active CRDT List Engine Unit Test Suite  \n" ANSI_RESET);
    printf(ANSI_BOLD "=========================================================\n\n" ANSI_RESET);

    /* Suite 1: Clock and HLC */
    printf(ANSI_BOLD "[Suite 1: Hybrid Logical Clock & crdtId]\n" ANSI_RESET);
    RUN_TEST(test_hlc_monotonicity);
    RUN_TEST(test_hlc_counter_saturation_and_borrowing);
    RUN_TEST(test_hlc_recv_and_skew_clamping);
    RUN_TEST(test_crdtid_encoding_decoding_and_cmp);
    printf("\n");

    /* Suite 2: Basic List Operations */
    printf(ANSI_BOLD "[Suite 2: CRDT List Basic Operations]\n" ANSI_RESET);
    RUN_TEST(test_basic_push_pop_range_delete);
    printf("\n");

    /* Suite 3: Commutativity */
    printf(ANSI_BOLD "[Suite 3: Commutativity of Concurrent Inserts]\n" ANSI_RESET);
    RUN_TEST(test_commutativity_concurrent_inserts);
    RUN_TEST(test_commutativity_complex_tree_permutations);
    printf("\n");

    /* Suite 4: Associativity */
    printf(ANSI_BOLD "[Suite 4: Join-Semilattice Associativity (A ⊔ B) ⊔ C == A ⊔ (B ⊔ C)]\n" ANSI_RESET);
    RUN_TEST(test_associativity_join_semilattice);
    printf("\n");

    /* Suite 5: Idempotence */
    printf(ANSI_BOLD "[Suite 5: Join-Semilattice Idempotence A ⊔ A == A]\n" ANSI_RESET);
    RUN_TEST(test_idempotence_join_semilattice);
    printf("\n");

    /* Suite 6: Concurrent Delete and Insert */
    printf(ANSI_BOLD "[Suite 6: Concurrent Delete and Insert on Tombstoned Anchors]\n" ANSI_RESET);
    RUN_TEST(test_concurrent_delete_and_insert);
    printf("\n");

    /* Suite 7: Fuzzing Convergence */
    printf(ANSI_BOLD "[Suite 7: 5-Node Multi-Replica Fuzzing Convergence]\n" ANSI_RESET);
    RUN_TEST(test_multi_replica_fuzzing_convergence);
    printf("\n");

    /* Suite 8: Tombstone Garbage Collection & Dynamic Stability Horizon */
    printf(ANSI_BOLD "[Suite 8: Stability Horizon Tombstone Garbage Collection]\n" ANSI_RESET);
    RUN_TEST(test_tombstone_pruning_stability_horizon);
    RUN_TEST(test_tombstone_descendant_anchor_safety);
    RUN_TEST(test_multi_branch_tombstone_pruning);
    printf("\n");

    printf(ANSI_BOLD "=========================================================\n" ANSI_RESET);
    printf("  Test Summary: %d/%d tests passed (%d assertions evaluated)\n",
           tests_passed, tests_run, assertions_run);
    printf(ANSI_BOLD "=========================================================\n" ANSI_RESET);

    if (tests_passed == tests_run) {
        printf(ANSI_GREEN ANSI_BOLD "  ALL TESTS PASSED SUCCESSFULLY (100%%)\n\n" ANSI_RESET);
        return 0;
    } else {
        printf(ANSI_RED ANSI_BOLD "  TEST FAILURES DETECTED\n\n" ANSI_RESET);
        return 1;
    }
}

