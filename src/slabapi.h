#ifndef SLABAPI_H
#define SLABAPI_H
#include "dict.h"
#include "tairhash_skiplist.h"

#define SLABMERGENUM 512

#ifdef __AVX2__
void slab_initShuffleMask();
#endif
void slab_expireInsert(tairhash_zskiplist *zsl, RedisModuleString *key, long long expire);
void slab_expireDelete(tairhash_zskiplist *zsl, RedisModuleString *key, long long expire);
void slab_expireUpdate(tairhash_zskiplist *zsl, RedisModuleString *cur_key, long long cur_expire, RedisModuleString *new_key, long long new_expire);
int slab_expireGet(tairhash_zskiplist *zsl, RedisModuleString *key, long long expire);
void slab_free(tairhash_zskiplist *zsl);
tairhash_zskiplist *slab_create();
int slab_getSlabTimeoutExpireIndex(tairhash_zskiplistNode *node, int *ontime_indices, int *timeout_indices);
void slab_deleteSlabExpire(tairhash_zskiplist *zsl, tairhash_zskiplistNode *zsl_node, int *effective_indexs, int effective_num);
unsigned int slab_deleteTairhashRangeByRank(tairhash_zskiplist *zsl, unsigned int start, unsigned int end);
#endif
