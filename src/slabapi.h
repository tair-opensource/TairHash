/*
 * Copyright 2021 Alibaba Tair Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
