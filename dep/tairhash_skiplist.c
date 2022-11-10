#include "tairhash_skiplist.h"

#include <assert.h>
#include <stdio.h>
/* Create a tairhashskiplist node with specified number of levelss.*/
tairhash_zskiplistNode *tairhash_zslCreateNode(int level, Slab *slab, long long expire, RedisModuleString *key) {
    tairhash_zskiplistNode *zn = (tairhash_zskiplistNode *)RedisModule_Alloc(sizeof(*zn) + level * sizeof(struct tairhash_zskiplistLevel));
    zn->slab = slab;
    zn->expire_min = expire;
    zn->key_min = key;
    zn->backward = NULL;
    return zn;
}

/* Create a new tairhashskiplist. */
tairhash_zskiplist *tairhash_zslCreate(void) {
    tairhash_zskiplist *zsl;

    zsl = (tairhash_zskiplist *)RedisModule_Alloc(sizeof(*zsl));
    zsl->length = 0;
    zsl->level = 1;
    zsl->header = tairhash_zslCreateNode(TAIRHASH_ZSKIPLIST_MAXLEVEL, NULL, 0, NULL);
    for (int j = 0; j < TAIRHASH_ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;
    zsl->tail = NULL;

    return zsl;
}

void tairhash_zslFreeNode(tairhash_zskiplistNode *node) {
    RedisModule_Free(node);
}

/* Free a whole skiplist. */
void tairhash_zslFree(tairhash_zskiplist *zsl) {
    if (zsl == NULL)
        return;
    tairhash_zskiplistNode *node = zsl->header->level[0].forward, *next;

    RedisModule_Free(zsl->header);
    while (node) {
        next = node->level[0].forward;
        if (node->slab != NULL)  // free slab
            slab_delete(node->slab);

        tairhash_zslFreeNode(node);
        node = next;
    }
    RedisModule_Free(zsl);
}

int tairhash_zslRandomLevel(void) {
    int level = 1;
    while ((random() & 0xFFFF) < (TAIRHASH_ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level < TAIRHASH_ZSKIPLIST_MAXLEVEL) ? level : TAIRHASH_ZSKIPLIST_MAXLEVEL;
}

tairhash_zskiplistNode *tairhash_zslGetNode(tairhash_zskiplist *zsl, RedisModuleString *key_min, long long expire_min) {
    tairhash_zskiplistNode *x;

    x = zsl->header;
    for (int i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (x->level[i].forward->expire_min < expire_min || (x->level[i].forward->expire_min == expire_min && RedisModule_StringCompare(x->level[i].forward->key_min, key_min) <= 0))) {
            x = x->level[i].forward;
        }
    }
    return x;
}

tairhash_zskiplistNode *tairhash_zslInsertNode(tairhash_zskiplist *zsl, Slab *slab, RedisModuleString *key_min, long long expire_min) {
    tairhash_zskiplistNode *update[TAIRHASH_ZSKIPLIST_MAXLEVEL], *x;
    uint32_t rank[TAIRHASH_ZSKIPLIST_MAXLEVEL];
    int i, level;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        rank[i] = i == (zsl->level - 1) ? 0 : rank[i + 1];
        while (x->level[i].forward && (x->level[i].forward->expire_min < expire_min || (x->level[i].forward->expire_min == expire_min && RedisModule_StringCompare(x->level[i].forward->key_min, key_min) < 0))) {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    level = tairhash_zslRandomLevel();
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
        }
        zsl->level = level;
    }

    x = tairhash_zslCreateNode(level, slab, expire_min, key_min);

    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }
    /* increment span for untouched levels */
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        zsl->tail = x;
    zsl->length++;
    return x;
}

void tairhash_zslDeleteNode(tairhash_zskiplist *zsl, tairhash_zskiplistNode *x, tairhash_zskiplistNode **update) {
    for (int i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }
    while (zsl->level > 1 && zsl->header->level[zsl->level - 1].forward == NULL)
        zsl->level--;
    zsl->length--;
}

int tairhash_zslDelete(tairhash_zskiplist *zsl, RedisModuleString *key, long long expire) {
    tairhash_zskiplistNode *update[TAIRHASH_ZSKIPLIST_MAXLEVEL], *x;

    x = zsl->header;
    for (int i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (x->level[i].forward->expire_min < expire || (x->level[i].forward->expire_min == expire && RedisModule_StringCompare(x->level[i].forward->key_min, key) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    x = x->level[0].forward;

    if (x && expire == x->expire_min && RedisModule_StringCompare(x->key_min, key) == 0) {
        tairhash_zslDeleteNode(zsl, x, update);
        tairhash_zslFreeNode(x);
        return 1;
    }
    return 0; /* not found */
}

tairhash_zskiplistNode *tairhash_zslUpdateNode(tairhash_zskiplist *zsl, RedisModuleString *cur_key_min, long long cur_expire_min, RedisModuleString *new_key_min, long long new_expire_min) {
    tairhash_zskiplistNode *update[TAIRHASH_ZSKIPLIST_MAXLEVEL], *x, *newnode;
    /* We need to seek to element to update to start: this is useful anyway,
     * we'll have to update or remove it. */
    x = zsl->header;
    for (int i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (x->level[i].forward->expire_min < cur_expire_min || (x->level[i].forward->expire_min == cur_expire_min && RedisModule_StringCompare(x->level[i].forward->key_min, cur_key_min) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    x = x->level[0].forward;
    /* Jump to our element: note that this function assumes that the
     * element with the matching expire_min. */
    assert(x && cur_expire_min == x->expire_min && RedisModule_StringCompare(x->key_min, cur_key_min) == 0);

    /* If the node, after the expire_min update, would be still exactly
     * at the same position, we can just update the expire_min without
     * actually removing and re-inserting the element in the skiplist. */

    if ((x->backward == NULL || x->backward->expire_min < new_expire_min) && (x->level[0].forward == NULL || x->level[0].forward->expire_min > new_expire_min)) {
        x->key_min = new_key_min;
        x->expire_min = new_expire_min;
        return x;
    }

    /* No way to reuse the old node: we need to remove and insert a new
     * one at a different place. */
    tairhash_zslDeleteNode(zsl, x, update);
    newnode = tairhash_zslInsertNode(zsl, x->slab, new_key_min, new_expire_min);
    x->key_min = NULL;
    tairhash_zslFreeNode(x);

    return newnode;
}

/* Delete all the elements with rank between start and end from the tairhash_skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
unsigned int tairhash_zslDeleteRangeByRank(tairhash_zskiplist *zsl, unsigned int start, unsigned int end) {
    tairhash_zskiplistNode *update[TAIRHASH_ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;


    x = zsl->header;
    for (int i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    traversed++;
    x = x->level[0].forward;
    while (x && traversed <= end) {
        tairhash_zskiplistNode *next = x->level[0].forward;
        tairhash_zslDeleteNode(zsl, x, update);
        tairhash_zslFreeNode(x);
        removed++;
        traversed++;
        x = next;
    }
    return removed;
}