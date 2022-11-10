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
#ifndef SLAB_H
#define SLAB_H

#include <stdint.h>
#include <stdlib.h>

#include "redismodule.h"

#define SLABMAXN 512 /* Skiplist P = 1/4 */
#define FALSE 0
#define TRUE 1
typedef struct Slab {
    long long expires[SLABMAXN];        // field_value_expire
    RedisModuleString *keys[SLABMAXN];  // field
    uint16_t num_keys;
} Slab;

Slab *slab_createNode(void);
int slab_insertNode(Slab *slab, RedisModuleString *key, long long expire);
int slab_getNode(Slab *slab, RedisModuleString *key, long long expire);
int slab_deleteIndexNode(Slab *slab, int index);
int slab_deleteNode(Slab *slab, RedisModuleString *key, long long expire);
int slab_updateNode(Slab *slab, RedisModuleString *cur_key, long long cur_expire, RedisModuleString *new_key, long long new_expire);
void slab_delete(Slab *slab);
int slab_minExpireTimeIndex(Slab *slab);
int slab_getExpiredKeyIndices(Slab *slab, long long target_ttl, int *out_indices);
void slab_swap(Slab *slab, int left, int right);
#endif