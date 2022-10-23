#ifndef TAIRHASH_SKIPLIST_H
#define TAIRHASH_SKIPLIST_H

#include <stdint.h>

#include "redismodule.h"
#include "slab.h"

#define TAIRHASH_ZSKIPLIST_MAXLEVEL 64 /* Should be enough for 2^64 elements */
#define TAIRHASH_ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */
typedef struct tairhash_zskiplistNode {
    Slab *slab;
    long long expire_min;
    RedisModuleString *key_min;
    struct tairhash_zskiplistNode *backward;
    struct tairhash_zskiplistLevel {
        struct tairhash_zskiplistNode *forward;
        unsigned long span;
    } level[];
} tairhash_zskiplistNode;

typedef struct tairhash_zskiplist {
    struct tairhash_zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} tairhash_zskiplist;

tairhash_zskiplist *tairhash_zslCreate(void);
tairhash_zskiplistNode *tairhash_zslInsertNode(tairhash_zskiplist *zsl, Slab *slab, RedisModuleString *key_min, long long expire_min);
tairhash_zskiplistNode *tairhash_zslGetNode(tairhash_zskiplist *zsl, RedisModuleString *key_min, long long expire_min);
tairhash_zskiplistNode *tairhash_zslUpdateNode(tairhash_zskiplist *zsl, RedisModuleString *cur_key_min, long long cur_expire_min, RedisModuleString *new_key_min, long long new_expire_min);
int tairhash_zslDelete(tairhash_zskiplist *zsl, RedisModuleString *key, long long expire);
void tairhash_zslFree(tairhash_zskiplist *zsl);
unsigned int tairhash_zslDeleteRangeByRank(tairhash_zskiplist *zsl, unsigned int start, unsigned int end);
#endif