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

#if (!defined SORT_MODE) && (!defined SLAB_MODE)

extern ExpireAlgorithm g_expire_algorithm;
extern RedisModuleType *TairHashType;

void insert(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(dbid);
    REDISMODULE_NOT_USED(key);
    if (expire) {
        m_zslInsert(obj->expire_index, expire, takeAndRef(field));
    }
}

void update(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long cur_expire, long long new_expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(dbid);
    REDISMODULE_NOT_USED(key);
    if (cur_expire != new_expire) {
        m_zslUpdateScore(obj->expire_index, cur_expire, field, new_expire);
    }
}

void delete(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long cur_expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(dbid);
    REDISMODULE_NOT_USED(key);
    if (cur_expire != 0) {
        m_zslDelete(obj->expire_index, cur_expire, field, NULL);
    }
}

void activeExpire(RedisModuleCtx *ctx, int dbid, uint64_t keys_per_loop) {
    tairHashObj *tair_hash_obj = NULL;
    int start_index;
    m_zskiplistNode *ln = NULL;
    m_zskiplistNode *ln2 = NULL;

    RedisModuleString *key, *field;
    RedisModuleKey *real_key;

    long long when, now;
    unsigned long zsl_len;

    long long start = RedisModule_Milliseconds();

    list *keys = m_listCreate();
    /* Each db has its own cursor, but this value may be wrong when swapdb appears (because we do not have a callback notification),
     * But this will not cause serious problems. */
    static long long scan_cursor[DB_NUM] = {0};
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "SCAN", "lcl", scan_cursor[dbid], "COUNT", g_expire_algorithm.keys_per_active_loop);
    if (reply != NULL) {
        switch (RedisModule_CallReplyType(reply)) {
            case REDISMODULE_REPLY_ARRAY: {
                Module_Assert(RedisModule_CallReplyLength(reply) == 2);

                RedisModuleCallReply *cursor_reply = RedisModule_CallReplyArrayElement(reply, 0);
                Module_Assert(RedisModule_CallReplyType(cursor_reply) == REDISMODULE_REPLY_STRING);
                Module_Assert(RedisModule_StringToLongLong(RedisModule_CreateStringFromCallReply(cursor_reply), &scan_cursor[dbid]) == REDISMODULE_OK);

                RedisModuleCallReply *keys_reply = RedisModule_CallReplyArrayElement(reply, 1);
                Module_Assert(RedisModule_CallReplyType(keys_reply) == REDISMODULE_REPLY_ARRAY);
                size_t keynum = RedisModule_CallReplyLength(keys_reply);

                int j;
                for (j = 0; j < keynum; j++) {
                    RedisModuleCallReply *key_reply = RedisModule_CallReplyArrayElement(keys_reply, j);
                    Module_Assert(RedisModule_CallReplyType(key_reply) == REDISMODULE_REPLY_STRING);
                    key = RedisModule_CreateStringFromCallReply(key_reply);
                    real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ | REDISMODULE_OPEN_KEY_NOTOUCH);
                    /* Since RedisModule_KeyType does not deal with the stream type, it is possible to
                       return REDISMODULE_KEYTYPE_EMPTY here, so we must deal with it until after this
                       bugfix: https://github.com/redis/redis/commit/1833d008b3af8628835b5f082c5b4b1359557893 */
                    if (RedisModule_KeyType(real_key) == REDISMODULE_KEYTYPE_EMPTY) {
                        continue;
                    }
                    if (RedisModule_ModuleTypeGetType(real_key) == TairHashType) {
                        tair_hash_obj = RedisModule_ModuleTypeGetValue(real_key);
                        if (tair_hash_obj->expire_index->length > 0) {
                            m_listAddNodeTail(keys, key);
                        }
                    }
                    RedisModule_CloseKey(real_key);
                }
                break;
            }
            default:
                /* impossible */
                break;
        }
    }

    if (listLength(keys) == 0) {
        m_listRelease(keys);
        return;
    }

    /* 3. Delete expired field. */
    int expire_keys_per_loop = g_expire_algorithm.keys_per_active_loop;
    m_listNode *node;
    while ((node = listFirst(keys)) != NULL) {
        key = listNodeValue(node);
        real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ | REDISMODULE_WRITE | REDISMODULE_OPEN_KEY_NOTOUCH);
        int type = RedisModule_KeyType(real_key);
        if (type != REDISMODULE_KEYTYPE_EMPTY) {
            Module_Assert(RedisModule_ModuleTypeGetType(real_key) == TairHashType);
        } else {
            /* Note: redis scan may return dup key. */
            m_listDelNode(keys, node);
            continue;
        }

        tair_hash_obj = RedisModule_ModuleTypeGetValue(real_key);

        zsl_len = tair_hash_obj->expire_index->length;
        Module_Assert(zsl_len > 0);

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

        if (start_index) {
            m_zslDeleteRangeByRank(tair_hash_obj->expire_index, 1, start_index);
            delEmptyTairHashIfNeeded(ctx, real_key, key, tair_hash_obj);
        }
        m_listDelNode(keys, node);
    }
    m_listRelease(keys);
}

void passiveExpire(RedisModuleCtx *ctx, int dbid, RedisModuleString *key) {
    tairHashObj *tair_hash_obj = NULL;
    long long when, now;
    int start_index = 0;
    m_zskiplistNode *ln = NULL;

    RedisModuleString *field;
    RedisModuleKey *real_key;
    unsigned long zsl_len;
    /* 1. The current db does not have a key that needs to expire. */
    list *keys = m_listCreate();

    real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ | REDISMODULE_WRITE);
    if (RedisModule_KeyType(real_key) != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(real_key) == TairHashType) {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(real_key);
        if (tair_hash_obj->expire_index->length > 0) {
            m_listAddNodeTail(keys, key);
        }
    }
    RedisModule_CloseKey(real_key);

    if (listLength(keys) == 0) {
        m_listRelease(keys);
        return;
    }

    /* 3. Delete expired field. */
    int keys_per_loop = g_expire_algorithm.keys_per_passive_loop;
    m_listNode *node;
    while ((node = listFirst(keys)) != NULL) {
        key = listNodeValue(node);
        real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ | REDISMODULE_WRITE);
        int type = RedisModule_KeyType(real_key);

        Module_Assert(type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(real_key) == TairHashType);
        tair_hash_obj = RedisModule_ModuleTypeGetValue(real_key);

        zsl_len = tair_hash_obj->expire_index->length;
        Module_Assert(zsl_len > 0);

        start_index = 0;
        ln = tair_hash_obj->expire_index->header->level[0].forward;
        while (ln && keys_per_loop) {
            field = ln->member;
            if (fieldExpireIfNeeded(ctx, dbid, key, tair_hash_obj, field, 1)) {
                start_index++;
                keys_per_loop--;
            } else {
                break;
            }
            ln = ln->level[0].forward;
        }

        if (start_index) {
            m_zslDeleteRangeByRank(tair_hash_obj->expire_index, 1, start_index);
            if (!delEmptyTairHashIfNeeded(ctx, real_key, key, tair_hash_obj)) {
                RedisModule_CloseKey(real_key);
            }
        }
        m_listDelNode(keys, node);
    }

    m_listRelease(keys);
}

void deleteAndPropagate(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire, int is_timer) {
    if (is_timer) {
        /* See bugfix: https://github.com/redis/redis/pull/8617
                       https://github.com/redis/redis/pull/8097
                       https://github.com/redis/redis/pull/7037
        */
        RedisModuleCtx *ctx2 = RedisModule_GetThreadSafeContext(NULL);
        RedisModule_SelectDb(ctx2, dbid);
        notifyFieldSpaceEvent("expired", key, field, dbid);
        RedisModuleCallReply *reply = RedisModule_Call(ctx2, "EXHDELREPL", "ss!", key, field);
        if (reply != NULL) {
            RedisModule_FreeCallReply(reply);
        }
        RedisModule_FreeThreadSafeContext(ctx2);
    } else {
        RedisModuleString *key_dup = RedisModule_CreateStringFromString(NULL, key);
        RedisModuleString *field_dup = RedisModule_CreateStringFromString(NULL, field);
        m_zslDelete(obj->expire_index, expire, field_dup, NULL);
        m_dictDelete(obj->hash, field);
        RedisModule_Replicate(ctx, "EXHDEL", "ss", key_dup, field_dup);
        notifyFieldSpaceEvent("expired", key_dup, field_dup, dbid);
        RedisModule_FreeString(NULL, key_dup);
        RedisModule_FreeString(NULL, field_dup);
    }
}

#endif
