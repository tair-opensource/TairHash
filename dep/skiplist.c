#include "skiplist.h"

#include <assert.h>
#include <math.h>
#include <stdlib.h>

#include "util.h"

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'ele' is referenced by the node after the call. */
m_zskiplistNode *m_zslCreateNode(int level, long long score, RedisModuleString *key, RedisModuleString *field) {
    m_zskiplistNode *zn = RedisModule_Alloc(sizeof(*zn) + level * sizeof(struct zskiplistLevel));
    zn->score = score;
    zn->key = key;
    zn->field = field;
    return zn;
}

/* Create a new skiplist. */
m_zskiplist *m_zslCreate(void) {
    int j;
    m_zskiplist *zsl;

    zsl = RedisModule_Alloc(sizeof(*zsl));
    zsl->level = 1;
    zsl->length = 0;
    zsl->header = m_zslCreateNode(ZSKIPLIST_MAXLEVEL, 0, NULL, NULL);
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    return zsl;
}

/* Free the specified skiplist node. The referenced SDS string representation
 * of the element is freed too, unless node->ele is set to NULL before calling
 * this function. */
void m_zslFreeNode(m_zskiplistNode *node) {
    // if (node->key)
    //     RedisModule_FreeString(NULL, node->key);
    if (node->field)
        RedisModule_FreeString(NULL, node->field);
    RedisModule_Free(node);
}

/* Free a whole skiplist. */
void m_zslFree(m_zskiplist *zsl) {
    m_zskiplistNode *node = zsl->header->level[0].forward, *next;

    RedisModule_Free(zsl->header);
    while (node) {
        next = node->level[0].forward;
        m_zslFreeNode(node);
        node = next;
    }
    RedisModule_Free(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
int m_zslRandomLevel(void) {
    int level = 1;
    while ((random() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level < ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'. */
m_zskiplistNode *m_zslInsert(m_zskiplist *zsl, long long score, RedisModuleString *key, RedisModuleString *field) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        rank[i] = i == (zsl->level - 1) ? 0 : rank[i + 1];
        while (x->level[i].forward && 
            (x->level[i].forward->score < score || 
            (x->level[i].forward->score == score && RedisModule_StringCompare(x->level[i].forward->key, key) < 0) || 
            (x->level[i].forward->score == score && RedisModule_StringCompare(x->level[i].forward->key, key) == 0 && RedisModule_StringCompare(x->level[i].forward->field, field) < 0))) {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* we assume the element is not already inside, since we allow duplicated
     * scores, reinserting the same element should never happen since the
     * caller of m_zslInsert() should test in the hash table if the element is
     * already inside or not. */
    level = m_zslRandomLevel();
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
        }
        zsl->level = level;
    }
    x = m_zslCreateNode(level, score, key, field);
    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
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

/* Internal function used by m_zslDelete, zslDeleteByScore and zslDeleteByRank */
void m_zslDeleteNode(m_zskiplist *zsl, m_zskiplistNode *x, m_zskiplistNode **update) {
    int i;
    for (i = 0; i < zsl->level; i++) {
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

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by m_zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele). */
int m_zslDelete(m_zskiplist *zsl, long long score, RedisModuleString *key, RedisModuleString *field, m_zskiplistNode **node) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && 
        (x->level[i].forward->score < score || 
        (x->level[i].forward->score == score && RedisModule_StringCompare(x->level[i].forward->key, key) < 0) || 
        (x->level[i].forward->score == score && RedisModule_StringCompare(x->level[i].forward->key, key) == 0 && RedisModule_StringCompare(x->level[i].forward->field, field) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    x = x->level[0].forward;
    if (x && score == x->score && RedisModule_StringCompare(x->key, key) == 0 && RedisModule_StringCompare(x->field, field) == 0) {
        m_zslDeleteNode(zsl, x, update);
        if (!node)
            m_zslFreeNode(x);
        else
            *node = x;
        return 1;
    }
    return 0; /* not found */
}

int m_zslDeleteWholeKey(m_zskiplist *zsl, RedisModuleString *key) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    if (key == NULL) {
        printf("woca\n");
        return 0;
    }

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && 
            (x->level[i].forward->key == NULL || 
            (x->level[i].forward->key != NULL && RedisModule_StringCompare(x->level[i].forward->key, key) != 0)))
            x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    while (x) {
        m_zskiplistNode *next = x->level[0].forward;
        if (RedisModule_StringCompare(x->key, key) == 0) {
            m_zslDeleteNode(zsl, x, update);
            m_zslFreeNode(x); /* Here is where x->ele is actually released. */
            removed++;
        }
        x = next;
    }
    return removed;
}

/* Update the score of an elmenent inside the sorted set skiplist.
 * Note that the element must exist and must match 'score'.
 * This function does not update the score in the hash table side, the
 * caller should take care of it.
 *
 * Note that this function attempts to just update the node, in case after
 * the score update, the node would be exactly at the same position.
 * Otherwise the skiplist is modified by removing and re-adding a new
 * element, which is more costly.
 *
 * The function returns the updated element skiplist node pointer. */
m_zskiplistNode *m_zslUpdateScore(m_zskiplist *zsl, long long  curscore, RedisModuleString *key, RedisModuleString *field, long long  newscore) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    /* We need to seek to element to update to start: this is useful anyway,
     * we'll have to update or remove it. */
    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && 
            (x->level[i].forward->score < curscore || 
            (x->level[i].forward->score == curscore && RedisModule_StringCompare(x->level[i].forward->key, key) < 0) || 
            (x->level[i].forward->score == curscore && RedisModule_StringCompare(x->level[i].forward->key, key) == 0 && RedisModule_StringCompare(x->level[i].forward->field, field) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* Jump to our element: note that this function assumes that the
     * element with the matching score exists. */
    x = x->level[0].forward;
    assert(x && curscore == x->score && RedisModule_StringCompare(x->key, key) == 0 && RedisModule_StringCompare(x->field, field) == 0);

    /* If the node, after the score update, would be still exactly
     * at the same position, we can just update the score without
     * actually removing and re-inserting the element in the skiplist. */
    if ((x->backward == NULL || x->backward->score < newscore) && (x->level[0].forward == NULL || x->level[0].forward->score > newscore)) {
        x->score = newscore;
        return x;
    }

    /* No way to reuse the old node: we need to remove and insert a new
     * one at a different place. */
    m_zslDeleteNode(zsl, x, update);
    m_zskiplistNode *newnode = m_zslInsert(zsl, newscore, x->key, x->field);
    /* We reused the old node x->ele SDS string, free the node now
     * since m_zslInsert created a new one. */
    x->key = NULL;
    x->field = NULL;
    m_zslFreeNode(x);
    return newnode;
}

int m_zslValueGteMin(long long value, m_zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

int m_zslValueLteMax(long long value, m_zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range. */
int zslIsInRange(m_zskiplist *zsl, m_zrangespec *range) {
    m_zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    if (range->min > range->max || (range->min == range->max && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !m_zslValueGteMin(x->score, range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !m_zslValueLteMax(x->score, range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
m_zskiplistNode *m_zslFirstInRange(m_zskiplist *zsl, m_zrangespec *range) {
    m_zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl, range))
        return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward && !m_zslValueGteMin(x->level[i].forward->score, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    assert(x != NULL);

    /* Check if score <= max. */
    if (!m_zslValueLteMax(x->score, range))
        return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
m_zskiplistNode *m_zslLastInRange(m_zskiplist *zsl, m_zrangespec *range) {
    m_zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl, range))
        return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward && m_zslValueLteMax(x->level[i].forward->score, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    assert(x != NULL);

    /* Check if score >= min. */
    if (!m_zslValueGteMin(x->score, range))
        return NULL;
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 * Min and max are inclusive, so a score >= min || score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */
unsigned long m_zslDeleteRangeByScore(m_zskiplist *zsl, m_zrangespec *range) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (range->minex ? x->level[i].forward->score <= range->min : x->level[i].forward->score < range->min))
            x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    while (x && (range->maxex ? x->score < range->max : x->score <= range->max)) {
        m_zskiplistNode *next = x->level[0].forward;
        m_zslDeleteNode(zsl, x, update);
        m_zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }
    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
unsigned long m_zslDeleteRangeByRank(m_zskiplist *zsl, unsigned int start, unsigned int end) {
    m_zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    traversed++;
    x = x->level[0].forward;
    while (x && traversed <= end) {
        m_zskiplistNode *next = x->level[0].forward;
        m_zslDeleteNode(zsl, x, update);
        m_zslFreeNode(x);
        removed++;
        traversed++;
        x = next;
    }
    return removed;
}

m_zskiplistNode* m_zslGetElementByRank(m_zskiplist *zsl, unsigned long rank) {
    m_zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}