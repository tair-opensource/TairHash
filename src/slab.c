#include "slab.h"

#include <assert.h>
#include <stdio.h>


#include "util.h"

/* create slab */
Slab *slab_createNode(void) {
    Slab *slab = (Slab *)RedisModule_Alloc(sizeof(Slab));
    for (int i = 0; i < SLABMAXN; i++) {
        slab->keys[i] = NULL;
        slab->expires[i] = 0;
    }
    slab->num_keys = 0;

    return slab;
}

/*insert to the slab tail */
int slab_insertNode(Slab *slab, RedisModuleString *key, long long expire) {
    if (slab->num_keys >= SLABMAXN) {
        return FALSE;
    }
    int i = slab->num_keys;
    slab->expires[i] = expire, slab->keys[i] = key, slab->num_keys++;
    return TRUE;
}

/*   if return value  -1 is not found ,else the target position */
int slab_getNode(Slab *slab, RedisModuleString *key, long long expire) {
    if (slab == NULL) return -1;
    size_t key_len;

    int target_position = -1, num_keys = slab->num_keys;
    if (key == NULL) {  // key is null
        return target_position;
    }

    for (int i = 0; i < num_keys; i++) {
        if (slab->expires[i] == expire && RedisModule_StringCompare(key, slab->keys[i]) == 0) {
            target_position = i;
            break;
        }
    }
    return target_position;
}

/* slab delete index node*/
int slab_deleteIndexNode(Slab *slab, int index) {
    if (index < 0 || slab->num_keys <= 0 || index >= slab->num_keys) {
        return FALSE;
    }
    int end = slab->num_keys - 1;
    RedisModule_FreeString(NULL, slab->keys[index]);
    if (end != index) {
        slab->keys[index] = slab->keys[end], slab->expires[index] = slab->expires[end];
    }
    slab->expires[end] = 0, slab->keys[end] = NULL, slab->num_keys--;
    return TRUE;
}

int slab_deleteNode(Slab *slab, RedisModuleString *key, long long expire) {
    int target_position = slab_getNode(slab, key, expire);
    return slab_deleteIndexNode(slab, target_position);
}

int slab_updateNode(Slab *slab, RedisModuleString *cur_key, long long cur_expire, RedisModuleString *new_key, long long new_expire) {
    int target_position = slab_getNode(slab, cur_key, cur_expire);

    if (target_position < 0 || target_position >= SLABMAXN) {
        return FALSE;
    }

    RedisModule_FreeString(NULL, slab->keys[target_position]);
    slab->keys[target_position] = new_key, slab->expires[target_position] = new_expire;

    return TRUE;
}

/* free slab */
void slab_delete(Slab *slab) {
    if (slab == NULL) return;
    for (int i = 0; i < slab->num_keys; i++) {
        RedisModule_FreeString(NULL, slab->keys[i]);
    }
    RedisModule_Free(slab);
}

/* get the smallest element ssubscript */
int slab_minExpireTimeIndex(Slab *slab) {
    int min_subscript = 0, length = slab->num_keys;
    for (int i = 1; i < length; i++) {
        if (slab->expires[min_subscript] > slab->expires[i] || (slab->expires[min_subscript] == slab->expires[i] && RedisModule_StringCompare(slab->keys[min_subscript], slab->keys[i]) > 0)) {
            min_subscript = i;
        }
    }
    return min_subscript;
}

int slab_getExpiredKeyIndices(Slab *slab, long long target_ttl, int *out_indices) {
    if (slab == NULL || slab->num_keys == 0)
        return 0;

    int size_out = 0, size = slab->num_keys;
    for (int i = 0; i < size; i++) {
        out_indices[size_out] = i;
        size_out += (slab->expires[i] <= target_ttl);
    }
    return size_out;
}

inline void slab_swap(Slab *slab, int left, int right) {
    long long temp_expire = slab->expires[left];
    RedisModuleString *temp_key = slab->keys[left];
    slab->expires[left] = slab->expires[right], slab->keys[left] = slab->keys[right];
    slab->expires[right] = temp_expire, slab->keys[right] = temp_key;
    return;
}