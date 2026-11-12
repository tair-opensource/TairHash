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

#if defined(SORT_MODE)
extern ExpireAlgorithm g_expire_algorithm;
extern m_zskiplist *g_expire_index[DB_NUM];
extern RedisModuleType *TairHashType;

void insert(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, long long expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(key);
    if (expire) {
        long long before_min_score = -1, after_min_score = -1;
        tairHashEnsureExpireIndex(o);
        if (o->expire_index->header->level[0].forward) {
            before_min_score = o->expire_index->header->level[0].forward->score;
        }
        m_zslInsert(o->expire_index, expire, takeAndRef(field));
        after_min_score = o->expire_index->header->level[0].forward->score;
        if (before_min_score > 0) {
            m_zslUpdateScore(g_expire_index[dbid], before_min_score, o->key, after_min_score);
        } else {
            m_zslInsert(g_expire_index[dbid], after_min_score, expireIndexMember(o));
        }
    }
}

void update(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, long long cur_expire, long long new_expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(key);
    if (cur_expire != new_expire) {
        long long before_min_score = -1, after_min_score = 1;
        m_zskiplistNode *ln = o->expire_index->header->level[0].forward;
        Module_Assert(ln != NULL);
        before_min_score = ln->score;
        m_zslUpdateScore(o->expire_index, cur_expire, field, new_expire);
        after_min_score = o->expire_index->header->level[0].forward->score;
        m_zslUpdateScore(g_expire_index[dbid], before_min_score, o->key, after_min_score);
    }
}

void delete(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, long long cur_expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(key);
    if (cur_expire != 0) {
        long long before_min_score = -1;
        m_zskiplistNode *ln = o->expire_index->header->level[0].forward;
        Module_Assert(ln != NULL);
        before_min_score = ln->score;
        m_zslDelete(o->expire_index, cur_expire, field, NULL);
        if (o->expire_index->header->level[0].forward) {
            long long after_min_score = o->expire_index->header->level[0].forward->score;
            m_zslUpdateScore(g_expire_index[dbid], before_min_score, key, after_min_score);
        } else {
            m_zslDelete(g_expire_index[dbid], before_min_score, key, NULL);
        }
    }
}

void activeExpire(RedisModuleCtx *ctx, int dbid, uint64_t keys_per_loop) {
    int start_index;
    int expire_keys_per_loop = keys_per_loop;
    m_zskiplistNode *ln2 = NULL;
    RedisModuleString *key, *field;
    RedisModuleKey *real_key;
    tairHashObj *tair_hash_obj = NULL;

    list *keys = popDueExpireIndexEntries(dbid, RedisModule_Milliseconds(), keys_per_loop);

    /* Delete expired fields. */
    m_listNode *node;
    while ((node = listFirst(keys)) != NULL) {
        expiringKey *ek = listNodeValue(node);
        tair_hash_obj = openIndexedTairHash(ctx, ek, REDISMODULE_READ | REDISMODULE_WRITE | REDISMODULE_OPEN_KEY_NOTOUCH, &real_key);
        if (tair_hash_obj == NULL) {
            releaseExpiringKey(keys, node);
            continue;
        }
        key = ek->key;

        ln2 = tair_hash_obj->expire_index->header->level[0].forward;
        start_index = 0;
        while (ln2 && expire_keys_per_loop) {
            field = ln2->member;
            if (fieldExpireIfNeeded(ctx, dbid, key, tair_hash_obj, field, 1)) {
                g_expire_algorithm.stat_active_expired_field[dbid]++;
                start_index++;
                expire_keys_per_loop--;
            } else {
                break;
            }
            ln2 = ln2->level[0].forward;
        }

        int deleted = 0;
        if (start_index) {
            m_zslDeleteRangeByRank(tair_hash_obj->expire_index, 1, start_index);
            deleted = delEmptyTairHashIfNeeded(ctx, real_key, key, tair_hash_obj);
        }

        /* If there is still a field waiting to expire, hand the key name back to the global index. */
        if (!deleted && ln2) {
            m_zslInsert(g_expire_index[dbid], ln2->score, key);
            ek->key = NULL;
        }
        releaseExpiringKey(keys, node);
    }

    m_listRelease(keys);
}

void passiveExpire(RedisModuleCtx *ctx, int dbid, RedisModuleString *up_key) {
    REDISMODULE_NOT_USED(up_key);
    /* A replica sees expired fields but does not delete them; it gets the master's deletions.
     * Taking their entries out of the indexes here would leave those deletions nothing to remove. */
    if (isReadOnlyStatus(ctx)) {
        return;
    }
    int keys_per_loop = g_expire_algorithm.keys_per_passive_loop;
    int start_index = 0;
    m_zskiplistNode *ln = NULL;
    RedisModuleString *key, *field;
    RedisModuleKey *real_key;
    tairHashObj *tair_hash_obj = NULL;

    list *keys = popDueExpireIndexEntries(dbid, RedisModule_Milliseconds(), keys_per_loop);

    /* Delete expired fields. */
    m_listNode *node;
    while ((node = listFirst(keys)) != NULL) {
        expiringKey *ek = listNodeValue(node);
        /* Opening the key for writing can expire the key itself, which leaves nothing to do. */
        tair_hash_obj = openIndexedTairHash(ctx, ek, REDISMODULE_READ | REDISMODULE_WRITE, &real_key);
        if (tair_hash_obj == NULL) {
            releaseExpiringKey(keys, node);
            continue;
        }
        key = ek->key;

        start_index = 0;
        ln = tair_hash_obj->expire_index->header->level[0].forward;
        while (ln && keys_per_loop) {
            field = ln->member;
            if (fieldExpireIfNeeded(ctx, dbid, key, tair_hash_obj, field, 1)) {
                g_expire_algorithm.stat_passive_expired_field[dbid]++;
                start_index++;
                keys_per_loop--;
            } else {
                break;
            }
            ln = ln->level[0].forward;
        }

        int deleted = 0;
        if (start_index) {
            m_zslDeleteRangeByRank(tair_hash_obj->expire_index, 1, start_index);
            deleted = delEmptyTairHashIfNeeded(ctx, real_key, key, tair_hash_obj);
            if (!deleted) {
                RedisModule_CloseKey(real_key);
            }
        }

        /* If there is still a field waiting to expire, hand the key name back to the global index. */
        if (!deleted && ln) {
            m_zslInsert(g_expire_index[dbid], ln->score, key);
            ek->key = NULL;
        }
        releaseExpiringKey(keys, node);
    }

    m_listRelease(keys);
}

void deleteAndPropagate(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, long long expire, int is_timer) {
    RedisModuleString *key_dup = RedisModule_CreateStringFromString(NULL, key);
    RedisModuleString *field_dup = RedisModule_CreateStringFromString(NULL, field);
    if (!is_timer) {
        long long before_min_score = o->expire_index->header->level[0].forward->score;
        m_zslDelete(o->expire_index, expire, field_dup, NULL);
        if (o->expire_index->header->level[0].forward != NULL) {
            long long after_min_score = o->expire_index->header->level[0].forward->score;
            m_zslUpdateScore(g_expire_index[dbid], before_min_score, key, after_min_score);
        } else {
            m_zslDelete(g_expire_index[dbid], before_min_score, key, NULL);
        }
    }
    m_dictDelete(o->hash, field);
    RedisModule_Replicate(ctx, "EXHDEL", "ss", key_dup, field_dup);
    notifyFieldSpaceEvent("expired", key_dup, field_dup, dbid);
    RedisModule_FreeString(NULL, key_dup);
    RedisModule_FreeString(NULL, field_dup);
}

#endif