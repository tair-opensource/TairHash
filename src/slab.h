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