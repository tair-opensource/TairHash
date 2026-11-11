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
#include "tairhash.h"

#if defined(SLAB_MODE)
extern ExpireAlgorithm g_expire_algorithm;
extern RedisModuleType *TairHashType;

int ontime_indices[SLABMAXN], timeout_indices[SLABMAXN];

void insert(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, long long expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(key);
    if (expire) {
        long long before_min_score = -1, after_min_score = -1;
        tairHashEnsureExpireIndex(o);
        if (o->expire_index->header->level[0].forward) {
            before_min_score = o->expire_index->header->level[0].forward->expire_min;
        }
        slab_expireInsert(o->expire_index, ownString(field), expire);
        after_min_score = o->expire_index->header->level[0].forward->expire_min;
        if (before_min_score > 0) {
            m_zslUpdateScore(dbExpireIndex(dbid), before_min_score, o->key, after_min_score);
        } else {
            m_zslInsert(dbExpireIndex(dbid), after_min_score, expireIndexMember(o));
        }
    }
}

void update(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, long long cur_expire, long long new_expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(key);
    if (cur_expire != new_expire) {
        long long before_min_score = -1, after_min_score = 1;
        tairhash_zskiplistNode *ln = o->expire_index->header->level[0].forward;
        Module_Assert(ln != NULL);
        before_min_score = ln->expire_min;
        RedisModuleString *new_field = ownString(field);
        slab_expireUpdate(o->expire_index, field, cur_expire, new_field, new_expire);
        after_min_score = o->expire_index->header->level[0].forward->expire_min;
        m_zslUpdateScore(dbExpireIndex(dbid), before_min_score, o->key, after_min_score);
    }
}

void delete(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, long long cur_expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(key);
    if (cur_expire != 0) {
        long long before_min_score = -1;
        tairhash_zskiplistNode *ln = o->expire_index->header->level[0].forward;
        Module_Assert(ln != NULL);
        before_min_score = ln->expire_min;
        slab_expireDelete(o->expire_index, field, cur_expire);
        if (o->expire_index->header->level[0].forward) {
            long long after_min_score = o->expire_index->header->level[0].forward->expire_min;
            m_zslUpdateScore(dbExpireIndex(dbid), before_min_score, key, after_min_score);
        } else {
            m_zslDelete(dbExpireIndex(dbid), before_min_score, key, NULL);
        }
    }
}

void activeExpire(RedisModuleCtx *ctx, int dbid, uint64_t keys_per_loop) {
    tairHashObj *tair_hash_obj = NULL;
    int start_index;
    tairhash_zskiplistNode *ln2 = NULL;
    RedisModuleString *key;
    RedisModuleKey *real_key;

    int expire_keys_per_loop = keys_per_loop;
    list *keys = popDueExpireIndexEntries(dbid, RedisModule_Milliseconds(), keys_per_loop);

    /* Delete expired fields. */
    m_listNode *node;
    int ontime_num = 0, timeout_num = 0, timeout_index = 0, delete_rank = 0, j;
    while ((node = listFirst(keys)) != NULL) {
        expiringKey *ek = listNodeValue(node);
        tair_hash_obj = openIndexedTairHash(ctx, ek, REDISMODULE_READ | REDISMODULE_WRITE | REDISMODULE_OPEN_KEY_NOTOUCH, &real_key);
        if (tair_hash_obj == NULL) {
            releaseExpiringKey(keys, node);
            continue;
        }
        key = ek->key;

        ln2 = tair_hash_obj->expire_index->header->level[0].forward;
        /* The split counts describe this key's first unfinished slab. When the per-loop budget is
         * already spent the loop below does not run, and counts left from the previous key would
         * compact this key's slab with that key's indices. */
        start_index = 0, delete_rank = 0, ontime_num = 0, timeout_num = 0;
        long long start_active_expire_timer = RedisModule_Milliseconds();
        while (ln2 && expire_keys_per_loop > 0) {
            if (ln2->level[0].forward != NULL && isExpire(ln2->level[0].forward->expire_min)) {
                timeout_num = ln2->slab->num_keys;
                ontime_num = 0;
            } else {
                timeout_num = slab_getSlabTimeoutExpireIndex(ln2, ontime_indices, timeout_indices);
                ontime_num = ln2->slab->num_keys - timeout_num;
                if (timeout_num <= 0)
                    break;
            }

            int kept = 0;
            for (j = 0; j < timeout_num; j++) {
                if (ontime_num == 0) {
                    timeout_index = j;
                } else {
                    timeout_index = timeout_indices[j];
                }
                if (fieldExpireIfNeeded(ctx, dbid, key, tair_hash_obj, ln2->slab->keys[timeout_index], 1)) {
                    g_expire_algorithm.stat_active_expired_field[dbid]++;
                    start_index++;
                    expire_keys_per_loop--;
                } else {
                    /* Still in the hash, so it stays in the slab. timeout_indices is read from j on,
                     * which leaves its start free to collect these. */
                    timeout_indices[kept++] = timeout_index;
                }
            }
            if (kept) {
                /* Merge them, from the back, into the ascending indices the slab keeps. */
                int from = ontime_num - 1, to = ontime_num + kept - 1;
                for (j = kept - 1; j >= 0; to--) {
                    if (from >= 0 && ontime_indices[from] > timeout_indices[j]) {
                        ontime_indices[to] = ontime_indices[from--];
                    } else {
                        ontime_indices[to] = timeout_indices[j--];
                    }
                }
                ontime_num += kept;
                timeout_num -= kept;
            }

            if (ontime_num == 0) {
                delete_rank++;
            } else {
                break;
            }
            ln2 = ln2->level[0].forward;
        }

        if (delete_rank) {
            slab_deleteTairhashRangeByRank(tair_hash_obj->expire_index, 1, delete_rank);
        }
        if (tair_hash_obj->expire_index->length > 0 && ontime_num > 0 && timeout_num > 0) {
            slab_deleteSlabExpire(tair_hash_obj->expire_index, tair_hash_obj->expire_index->header->level[0].forward, ontime_indices, ontime_num);
        }

        int deleted = start_index ? delEmptyTairHashIfNeeded(ctx, real_key, key, tair_hash_obj) : 0;

        /* The key left the global index above; hand its name back whenever fields remain to expire,
         * including when the per-loop budget ran out before any of them were processed. */
        if (!deleted && tair_hash_obj->expire_index->length > 0) {
            m_zslInsert(dbExpireIndex(dbid), tair_hash_obj->expire_index->header->level[0].forward->expire_min, key);
            ek->key = NULL;
        }
        /* Deleting the key closed it. Left open, it would outlive a later delete of the same key. */
        if (!deleted) {
            RedisModule_CloseKey(real_key);
        }
        releaseExpiringKey(keys, node);
    }
    m_listRelease(keys);
}

void passiveExpire(RedisModuleCtx *ctx, int dbid, RedisModuleString *key) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(dbid);
    REDISMODULE_NOT_USED(key);
    /* Not support. */
}

int deleteAndPropagate(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, long long expire, int is_timer) {
    RedisModuleString *key_dup = RedisModule_CreateStringFromString(NULL, key);
    /* field may be the expire index's own copy of the name, which deleting the index entry frees. */
    RedisModuleString *field_dup = RedisModule_CreateStringFromString(NULL, field);
    if (!is_timer) {
        long long before_min_score = o->expire_index->header->level[0].forward->expire_min;
        slab_expireDelete(o->expire_index, field_dup, expire);
        if (o->expire_index->header->level[0].forward != NULL) {
            long long after_min_score = o->expire_index->header->level[0].forward->expire_min;
            m_zslUpdateScore(dbExpireIndex(dbid), before_min_score, key, after_min_score);
        } else {
            m_zslDelete(dbExpireIndex(dbid), before_min_score, key, NULL);
        }
    }
    size_t field_len;
    const char *field_ptr = RedisModule_StringPtrLen(field_dup, &field_len);
    ftDelete(&o->hash, field_ptr, field_len);
    RedisModule_Replicate(ctx, "EXHDEL", "ss", key_dup, field_dup);
    notifyFieldSpaceEvent("expired", key_dup, field_dup, dbid);
    RedisModule_FreeString(NULL, key_dup);
    RedisModule_FreeString(NULL, field_dup);
    return 1;
}

#endif