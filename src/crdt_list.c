/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * CRDT Replicated Growable Array (RGA) List Engine Implementation
 */

#include "crdt_list.h"
#include "zmalloc.h"
#include <string.h>
#include <stdlib.h>

crdtList *crdtListCreate(void) {
    crdtList *list = zmalloc(sizeof(*list));
    if (!list) return NULL;

    list->head = zmalloc(sizeof(*list->head));
    if (!list->head) {
        zfree(list);
        return NULL;
    }

    list->head->id = (crdtId){0, 0};
    list->head->parent_id = (crdtId){0, 0};
    list->head->val = NULL;
    list->head->deleted = 1; /* Sentinel is deleted/invisible */
    list->head->del_hlc = 0;
    list->head->del_origin = 0;
    list->head->prev = NULL;
    list->head->next = NULL;
    list->head->parent = NULL;
    list->head->gc_keep = 1;

    list->tail = list->head;
    list->index = raxNew();
    if (!list->index) {
        zfree(list->head);
        zfree(list);
        return NULL;
    }

    unsigned char sentinel_key[12];
    crdtIdEncode(list->head->id, sentinel_key);
    raxInsert(list->index, sentinel_key, 12, list->head, NULL);

    list->length = 0;
    list->tombstone_count = 0;
    list->total_vertices = 0;
    return list;
}

void crdtListRelease(crdtList *list) {
    if (!list) return;

    crdtListVertex *curr = list->head;
    while (curr != NULL) {
        crdtListVertex *next = curr->next;
        if (curr->val != NULL) {
            sdsfree(curr->val);
            curr->val = NULL;
        }
        zfree(curr);
        curr = next;
    }

    if (list->index != NULL) {
        raxFree(list->index);
        list->index = NULL;
    }

    zfree(list);
}

crdtListVertex *crdtListFindVertex(crdtList *list, crdtId id) {
    if (!list || !list->index) return NULL;
    unsigned char key[12];
    crdtIdEncode(id, key);
    void *val = NULL;
    if (raxFind(list->index, key, 12, &val) == 1) {
        return (crdtListVertex *)val;
    }
    return NULL;
}

/* Helper: Find the direct child of parent_vertex that is an ancestor of (or is equal to) curr.
 * If curr is not in parent_vertex's subtree, returns NULL. */
static inline crdtListVertex *crdtListFindChildAncestor(crdtListVertex *parent_vertex, crdtListVertex *curr) {
    crdtListVertex *p = curr;
    while (p != NULL && p->parent != NULL) {
        if (p->parent == parent_vertex) {
            return p;
        }
        p = p->parent;
    }
    return NULL;
}

crdtListVertex *crdtListInsertAfter(crdtList *list, crdtId parent_id, crdtId new_id, sds val) {
    if (!list) return NULL;

    /* Idempotence check: return existing vertex if already present */
    crdtListVertex *existing = crdtListFindVertex(list, new_id);
    if (existing != NULL) {
        return existing;
    }

    /* Locate parent vertex */
    crdtListVertex *parent_vertex = NULL;
    if (parent_id.hlc == 0 && parent_id.origin_id == 0) {
        parent_vertex = list->head;
    } else {
        parent_vertex = crdtListFindVertex(list, parent_id);
        if (parent_vertex == NULL) {
            /* Fallback to head if parent vertex is missing to avoid dropped writes */
            parent_vertex = list->head;
        }
    }

    /* Allocate and initialize new vertex */
    crdtListVertex *v = zmalloc(sizeof(*v));
    if (!v) return NULL;

    v->id = new_id;
    v->parent_id = parent_vertex->id;
    v->parent = parent_vertex;
    v->val = (val != NULL) ? sdsnew(val) : sdsnewlen("", 0);
    v->deleted = 0;
    v->del_hlc = 0;
    v->del_origin = 0;
    v->prev = NULL;
    v->next = NULL;
    v->gc_keep = 0;

    /* Deterministic RGA Sibling Traversal:
     * Scan vertices following parent_vertex.
     * We skip children/descendants of parent_vertex that have higher priority (crdtIdCmp > 0)
     * than new_id. As soon as we find a child whose crdtId is smaller than new_id, or
     * when we reach a vertex that has exited parent_vertex's subtree, we insert v right before it.
     */
    crdtListVertex *curr = parent_vertex->next;
    crdtListVertex *insert_before = NULL;

    while (curr != NULL) {
        crdtListVertex *child = crdtListFindChildAncestor(parent_vertex, curr);
        if (child == NULL) {
            /* curr is no longer inside parent_vertex's subtree */
            insert_before = curr;
            break;
        }

        int cmp = crdtIdCmp(new_id, child->id);
        if (cmp > 0) {
            /* new_id has higher priority than child->id.
             * v must be positioned before child and child's entire subtree. */
            insert_before = child;
            break;
        }

        /* new_id has lower priority than child->id.
         * Advance to next node to skip child and its descendants. */
        curr = curr->next;
    }

    /* Insert into doubly-linked chain */
    if (insert_before != NULL) {
        v->next = insert_before;
        v->prev = insert_before->prev;
        if (insert_before->prev) {
            insert_before->prev->next = v;
        }
        insert_before->prev = v;
    } else {
        /* Append at tail */
        v->prev = list->tail;
        v->next = NULL;
        list->tail->next = v;
        list->tail = v;
    }

    /* Add to rax index */
    unsigned char key[12];
    crdtIdEncode(new_id, key);
    raxInsert(list->index, key, 12, v, NULL);

    list->length++;
    list->total_vertices++;

    return v;
}

int crdtListDeleteVertex(crdtList *list, crdtId id, uint64_t del_hlc, uint32_t del_origin) {
    if (!list) return 0;
    crdtListVertex *v = crdtListFindVertex(list, id);
    if (!v || v == list->head) return 0;

    if (!v->deleted) {
        v->deleted = 1;
        v->del_hlc = del_hlc;
        v->del_origin = del_origin;
        if (list->length > 0) list->length--;
        list->tombstone_count++;
        return 1;
    } else {
        /* LWW metadata resolution for concurrent tombstones */
        if (del_hlc > v->del_hlc || (del_hlc == v->del_hlc && del_origin > v->del_origin)) {
            v->del_hlc = del_hlc;
            v->del_origin = del_origin;
        }
        return 2;
    }
}

size_t crdtListLength(crdtList *list) {
    return list ? list->length : 0;
}

crdtListVertex *crdtListGetVisibleIndex(crdtList *list, long index) {
    if (!list || list->length == 0) return NULL;

    if (index < 0) {
        index = (long)list->length + index;
    }
    if (index < 0 || (size_t)index >= list->length) return NULL;

    if ((size_t)index < (list->length / 2)) {
        long count = 0;
        crdtListVertex *curr = list->head->next;
        while (curr != NULL) {
            if (!curr->deleted) {
                if (count == index) return curr;
                count++;
            }
            curr = curr->next;
        }
    } else {
        long target_from_back = (long)list->length - 1 - index;
        long count = 0;
        crdtListVertex *curr = list->tail;
        while (curr != NULL && curr != list->head) {
            if (!curr->deleted) {
                if (count == target_from_back) return curr;
                count++;
            }
            curr = curr->prev;
        }
    }
    return NULL;
}

sds *crdtListGetVisibleRange(crdtList *list, long start, long end, size_t *out_len) {
    if (out_len) *out_len = 0;
    if (!list || list->length == 0) return NULL;

    if (start < 0) start = (long)list->length + start;
    if (end < 0) end = (long)list->length + end;

    if (start < 0) start = 0;
    if (end < 0) end = 0;

    if ((size_t)start >= list->length || start > end) {
        return NULL;
    }
    if ((size_t)end >= list->length) {
        end = (long)list->length - 1;
    }

    size_t count = (size_t)(end - start + 1);
    sds *result = zmalloc(sizeof(sds) * count);
    if (!result) return NULL;

    long cur_idx = 0;
    size_t out_idx = 0;
    crdtListVertex *curr = list->head->next;
    while (curr != NULL && out_idx < count) {
        if (!curr->deleted) {
            if (cur_idx >= start && cur_idx <= end) {
                result[out_idx++] = sdsdup(curr->val);
            }
            cur_idx++;
        }
        curr = curr->next;
    }

    if (out_len) *out_len = out_idx;
    return result;
}

void crdtListFreeRange(sds *items, size_t len) {
    if (!items) return;
    for (size_t i = 0; i < len; i++) {
        if (items[i] != NULL) {
            sdsfree(items[i]);
        }
    }
    zfree(items);
}

int crdtListMerge(crdtList *local, crdtList *remote) {
    if (!local || !remote) return -1;

    /* Remote linked list is traversed in list order (topological order) */
    crdtListVertex *rv = remote->head->next;
    while (rv != NULL) {
        crdtListVertex *lv = crdtListFindVertex(local, rv->id);
        if (lv == NULL) {
            lv = crdtListInsertAfter(local, rv->parent_id, rv->id, rv->val);
            if (lv && rv->deleted) {
                crdtListDeleteVertex(local, rv->id, rv->del_hlc, rv->del_origin);
            }
        } else {
            if (rv->deleted) {
                crdtListDeleteVertex(local, rv->id, rv->del_hlc, rv->del_origin);
            }
        }
        rv = rv->next;
    }
    return 0;
}

crdtListVertex *crdtListPushHead(crdtList *list, crdtClock *clock, sds val) {
    if (!list || !clock) return NULL;
    crdtId new_id;
    hlc_now(clock, 0, &new_id, NULL);
    return crdtListInsertAfter(list, (crdtId){0, 0}, new_id, val);
}

crdtListVertex *crdtListPushTail(crdtList *list, crdtClock *clock, sds val) {
    if (!list || !clock) return NULL;
    crdtId new_id;
    hlc_now(clock, 0, &new_id, NULL);
    crdtId parent_id = list->tail->id;
    return crdtListInsertAfter(list, parent_id, new_id, val);
}

int crdtListPopHead(crdtList *list, crdtClock *clock, sds *out_val) {
    if (!list || !clock || list->length == 0) return 0;
    crdtListVertex *v = crdtListGetVisibleIndex(list, 0);
    if (!v) return 0;

    crdtId del_id;
    hlc_now(clock, 0, &del_id, NULL);

    if (out_val) {
        *out_val = sdsdup(v->val);
    }
    crdtListDeleteVertex(list, v->id, del_id.hlc, clock->origin_id);
    return 1;
}

int crdtListPopTail(crdtList *list, crdtClock *clock, sds *out_val) {
    if (!list || !clock || list->length == 0) return 0;
    crdtListVertex *v = crdtListGetVisibleIndex(list, -1);
    if (!v) return 0;

    crdtId del_id;
    hlc_now(clock, 0, &del_id, NULL);

    if (out_val) {
        *out_val = sdsdup(v->val);
    }
    crdtListDeleteVertex(list, v->id, del_id.hlc, clock->origin_id);
    return 1;
}

crdtList *crdtListClone(crdtList *list) {
    if (!list) return NULL;
    crdtList *clone = crdtListCreate();
    if (!clone) return NULL;
    crdtListMerge(clone, list);
    return clone;
}

int crdtListEquals(crdtList *a, crdtList *b) {
    if (a == b) return 1;
    if (!a || !b) return 0;
    if (a->length != b->length) return 0;
    if (a->total_vertices != b->total_vertices) return 0;
    if (a->tombstone_count != b->tombstone_count) return 0;

    crdtListVertex *va = a->head->next;
    crdtListVertex *vb = b->head->next;
    while (va != NULL && vb != NULL) {
        if (!crdtIdEqual(va->id, vb->id)) return 0;
        if (!crdtIdEqual(va->parent_id, vb->parent_id)) return 0;
        if (va->deleted != vb->deleted) return 0;
        if (va->deleted) {
            if (va->del_hlc != vb->del_hlc) return 0;
            if (va->del_origin != vb->del_origin) return 0;
        } else {
            if (sdscmp(va->val, vb->val) != 0) return 0;
        }
        va = va->next;
        vb = vb->next;
    }
    return (va == NULL && vb == NULL);
}

size_t crdtListPruneTombstones(crdtList *list, uint64_t h_stable) {
    if (!list || list->tombstone_count == 0) return 0;

    /* Pass 1: Clear gc_keep for all non-sentinel vertices */
    crdtListVertex *curr = list->head->next;
    while (curr != NULL) {
        curr->gc_keep = 0;
        curr = curr->next;
    }
    list->head->gc_keep = 1; /* Sentinel head is always preserved */

    /* Pass 2: Mark vertices that must be preserved:
     * 1. Any vertex that is NOT deleted (live/visible).
     * 2. Any deleted vertex with del_hlc > h_stable (not yet stable).
     * For every preserved vertex, traverse up the parent chain and mark
     * all ancestor vertices as preserved (gc_keep = 1).
     */
    curr = list->head->next;
    while (curr != NULL) {
        if (!curr->deleted || curr->del_hlc > h_stable) {
            curr->gc_keep = 1;
            crdtListVertex *p = curr->parent;
            while (p != NULL && p != list->head && !p->gc_keep) {
                p->gc_keep = 1;
                p = p->parent;
            }
        }
        curr = curr->next;
    }

    /* Pass 3: Prune all vertices that have gc_keep == 0 */
    size_t pruned = 0;
    curr = list->head->next;
    while (curr != NULL) {
        crdtListVertex *next = curr->next;
        if (!curr->gc_keep) {
            /* Detach from doubly-linked list */
            if (curr->prev) curr->prev->next = curr->next;
            if (curr->next) curr->next->prev = curr->prev;
            if (list->tail == curr) list->tail = curr->prev;

            /* Remove from rax index */
            unsigned char key[12];
            crdtIdEncode(curr->id, key);
            raxRemove(list->index, key, 12, NULL);

            /* Update counters */
            if (curr->deleted) {
                if (list->tombstone_count > 0) list->tombstone_count--;
            } else {
                if (list->length > 0) list->length--;
            }
            if (list->total_vertices > 0) list->total_vertices--;

            /* Free vertex payload and struct */
            if (curr->val != NULL) {
                sdsfree(curr->val);
                curr->val = NULL;
            }
            zfree(curr);
            pruned++;
        }
        curr = next;
    }

    return pruned;
}
