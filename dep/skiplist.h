#pragma once

#include "../src/redismodule.h"

#define ZSKIPLIST_MAXLEVEL 64 /* Should be enough for 2^64 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

typedef struct {
    long long min, max;
    int minex, maxex; /* are min or max exclusive? */
} m_zrangespec;

typedef struct m_zskiplistNode {
    RedisModuleString *member; 
    long long score;
    struct m_zskiplistNode *backward;
    struct zskiplistLevel {
        struct m_zskiplistNode *forward;
        unsigned long span;
    } level[];
} m_zskiplistNode;

typedef struct m_zskiplist {
    struct m_zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} m_zskiplist;

m_zskiplist *m_zslCreate(void);
void m_zslFree(m_zskiplist *zsl);
m_zskiplistNode *m_zslInsert(m_zskiplist *zsl, long long score, RedisModuleString *member);
int m_zslDelete(m_zskiplist *zsl, long long score, RedisModuleString *member, m_zskiplistNode **node);
m_zskiplistNode *m_zslFirstInRange(m_zskiplist *zsl, m_zrangespec *range);
m_zskiplistNode *m_zslLastInRange(m_zskiplist *zsl, m_zrangespec *range);
int m_zslValueGteMin(long long value, m_zrangespec *spec);
int m_zslValueLteMax(long long value, m_zrangespec *spec);
void m_zslDeleteNode(m_zskiplist *zsl, m_zskiplistNode *x, m_zskiplistNode **update);
m_zskiplistNode *m_zslUpdateScore(m_zskiplist *zsl, long long  curscore, RedisModuleString *member, long long newscore);
m_zskiplistNode* m_zslGetElementByRank(m_zskiplist *zsl, unsigned long rank);
unsigned long m_zslDeleteRangeByRank(m_zskiplist *zsl, unsigned int start, unsigned int end);