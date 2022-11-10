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
        if (o->expire_index->header->level[0].forward) {
            before_min_score = o->expire_index->header->level[0].forward->score;
        }
        m_zslInsert(o->expire_index, expire, takeAndRef(field));
        after_min_score = o->expire_index->header->level[0].forward->score;
        if (before_min_score > 0) {
            m_zslUpdateScore(g_expire_index[dbid], before_min_score, o->key, after_min_score);
        } else {
            m_zslInsert(g_expire_index[dbid], after_min_score, takeAndRef(o->key));
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
    long long when, now;
    unsigned long zsl_len;
    int expire_keys_per_loop = keys_per_loop;

    m_zskiplistNode *ln = NULL;
    m_zskiplistNode *ln2 = NULL;

    RedisModuleString *key, *field;
    RedisModuleKey *real_key;

    tairHashObj *tair_hash_obj = NULL;
    list *keys = m_listCreate();

    /* 1. The current db does not have a key that needs to expire. */
    zsl_len = g_expire_index[dbid]->length;
    if (zsl_len == 0) {
        m_listRelease(keys);
        return;
    }

    /* 2. Enumerates expired keys. */
    ln = g_expire_index[dbid]->header->level[0].forward;
    start_index = 0;
    while (ln && expire_keys_per_loop--) {
        key = ln->member;
        when = ln->score;
        now = RedisModule_Milliseconds();
        if (when > now) {
            break;
        }
        start_index++;
        m_listAddNodeTail(keys, key);
        ln = ln->level[0].forward;
    }

    if (start_index) {
        /* It is assumed that these keys will all be deleted. */
        m_zslDeleteRangeByRank(g_expire_index[dbid], 1, start_index);
    }

    if (listLength(keys) == 0) {
        m_listRelease(keys);
        return;
    }

    /* 3. Delete expired field. */
    expire_keys_per_loop = keys_per_loop;
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

        /* If there is still a field waiting to expire and delete, re-insert it to the global index. */
        if (ln2) {
            m_zslInsert(g_expire_index[dbid], ln2->score, takeAndRef(tair_hash_obj->key));
        }

        m_listDelNode(keys, node);
    }

    m_listRelease(keys);
}

void passiveExpire(RedisModuleCtx *ctx, int dbid, RedisModuleString *up_key) {
    REDISMODULE_NOT_USED(up_key);
    int keys_per_loop = g_expire_algorithm.keys_per_passive_loop;
    long long when, now;
    int start_index = 0;
    m_zskiplistNode *ln = NULL;

    RedisModuleString *key, *field;
    RedisModuleKey *real_key;
    unsigned long zsl_len;
    tairHashObj *tair_hash_obj = NULL;

    list *keys = m_listCreate();
    /* 1. The current db does not have a key that needs to expire. */
    zsl_len = g_expire_index[dbid]->length;
    if (zsl_len == 0) {
        m_listRelease(keys);
        return;
    }

    /* Reuse the current time for fields. */
    now = RedisModule_Milliseconds();

    /* 2. Enumerates expired keys */
    ln = g_expire_index[dbid]->header->level[0].forward;
    start_index = 0;
    while (ln && keys_per_loop--) {
        key = ln->member;
        when = ln->score;
        if (when > now) {
            break;
        }
        start_index++;
        m_listAddNodeTail(keys, key);
        ln = ln->level[0].forward;
    }

    if (start_index) {
        m_zslDeleteRangeByRank(g_expire_index[dbid], 1, start_index);
    }

    if (listLength(keys) == 0) {
        m_listRelease(keys);
        return;
    }

    /* 3. Delete expired field. */
    keys_per_loop = g_expire_algorithm.keys_per_passive_loop;
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
            if (fieldExpireIfNeeded(ctx, dbid, key, tair_hash_obj, field, 0)) {
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

        if (ln) {
            m_zslInsert(g_expire_index[dbid], ln->score, takeAndRef(tair_hash_obj->key));
        }

        m_listDelNode(keys, node);
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