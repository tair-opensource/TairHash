#include "tairhash.h"

#if defined(SLAB_MODE)
extern ExpireAlgorithm g_expire_algorithm;
extern m_zskiplist *g_expire_index[DB_NUM];
extern RedisModuleType *TairHashType;

int ontime_indices[SLABMAXN], timeout_indices[SLABMAXN];

void insert(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, long long expire) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(key);
    if (expire) {
        long long before_min_score = -1, after_min_score = -1;
        if (o->expire_index->header->level[0].forward) {
            before_min_score = o->expire_index->header->level[0].forward->expire_min;
        }
        slab_expireInsert(o->expire_index, takeAndRef(field), expire);
        after_min_score = o->expire_index->header->level[0].forward->expire_min;
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
        tairhash_zskiplistNode *ln = o->expire_index->header->level[0].forward;
        Module_Assert(ln != NULL);
        before_min_score = ln->expire_min;
        RedisModuleString *new_field = takeAndRef(field);
        slab_expireUpdate(o->expire_index, field, cur_expire, new_field, new_expire);
        after_min_score = o->expire_index->header->level[0].forward->expire_min;
        m_zslUpdateScore(g_expire_index[dbid], before_min_score, o->key, after_min_score);
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
            m_zslUpdateScore(g_expire_index[dbid], before_min_score, field, after_min_score);
        } else {
            m_zslDelete(g_expire_index[dbid], before_min_score, field, NULL);
        }
    }
}

void activeExpire(RedisModuleCtx *ctx, int dbid, uint64_t keys_per_loop) {
    tairHashObj *tair_hash_obj = NULL;
    int start_index;
    m_zskiplistNode *ln = NULL;
    tairhash_zskiplistNode *ln2 = NULL;
    RedisModuleString *key, *field;
    RedisModuleKey *real_key;

    long long when, now;
    unsigned long zsl_len;

    int expire_keys_per_loop = keys_per_loop;
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

    /* SLAB_MODE:3. Delete expired field. */
    expire_keys_per_loop = keys_per_loop;
    m_listNode *node;
    int ontime_num = 0, timeout_num = 0, timeout_index = 0, delete_rank = 0, j;
    while ((node = listFirst(keys)) != NULL) {
        key = listNodeValue(node);
        real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ | REDISMODULE_WRITE | REDISMODULE_OPEN_KEY_NOTOUCH);
        int type = RedisModule_KeyType(real_key);
        if (type != REDISMODULE_KEYTYPE_EMPTY) {
            Module_Assert(RedisModule_ModuleTypeGetType(real_key) == TairHashType);
        } else {
            m_listDelNode(keys, node);
            continue;
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(real_key);

        zsl_len = tair_hash_obj->expire_index->length;
        Module_Assert(zsl_len > 0);

        ln2 = tair_hash_obj->expire_index->header->level[0].forward;
        start_index = 0, delete_rank = 0;
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

            for (j = 0; j < timeout_num; j++) {
                if (ontime_num == 0) {
                    timeout_index = j;
                } else {
                    timeout_index = timeout_indices[j];
                }
                g_expire_algorithm.expireIfNeeded(ctx, dbid, key, tair_hash_obj, ln2->slab->keys[timeout_index], 1);
                g_expire_algorithm.stat_active_expired_field[dbid]++;
                start_index++;
                expire_keys_per_loop--;
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

        if (tair_hash_obj->expire_index->length > 0 && start_index) {
            m_zslInsert(g_expire_index[dbid], tair_hash_obj->expire_index->header->level[0].forward->expire_min, takeAndRef(tair_hash_obj->key));
        }
        if (start_index) {
            delEmptyTairHashIfNeeded(ctx, real_key, key, tair_hash_obj);
        }

        m_listDelNode(keys, node);
        start_index = 0;
    }
    m_listRelease(keys);
}

void passiveExpire(RedisModuleCtx *ctx, int dbid, RedisModuleString *key) {
    /* Not support. */
}

int expireIfNeeded(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, int is_timer) {
    TairHashVal *tair_hash_val = m_dictFetchValue(o->hash, field);
    if (tair_hash_val == NULL) {
        return 0;
    }
    long long when = tair_hash_val->expire;
    long long now;

    now = RedisModule_Milliseconds();

    if (when == 0) {
        return 0;
    }

    /* Slave only determines if it has timed out and does not perform a delete operation */
    if (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE) {
        return now > when;
    }

    if (now < when) {
        return 0;
    }

    RedisModuleString *key_dup = RedisModule_CreateStringFromString(NULL, key);
    RedisModuleString *field_dup = RedisModule_CreateStringFromString(NULL, field);
    if (!is_timer) {
        long long before_min_score = o->expire_index->header->level[0].forward->expire_min;
        //  printf("head_skiplist:%p\n",o->expire_index->header->level[0].forward);
        slab_expireDelete(o->expire_index, field_dup, when);
        if (o->expire_index->header->level[0].forward != NULL) {
            //    printf("delet_skiplist:%p\n",o->expire_index->header->level[0].forward);
            long long after_min_score = o->expire_index->header->level[0].forward->expire_min;
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
    return 1;
}

#endif