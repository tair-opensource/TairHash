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

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "scan_algorithm.h"
#include "slab_algorithm.h"
#include "sort_algorithm.h"

RedisModuleType *TairHashType;

static int redis_major_ver = 0;
static int redis_minor_ver = 0;
static int redis_patch_ver = 0;

#if defined(SORT_MODE) || defined(SLAB_MODE)
m_zskiplist **g_expire_index;

m_zskiplist *dbExpireIndex(int dbid) {
    if (g_expire_index[dbid] == NULL) {
        g_expire_index[dbid] = m_zslCreate();
    }
    return g_expire_index[dbid];
}
#endif

RedisModuleTimerID g_expire_timer_id;
ExpireAlgorithm g_expire_algorithm;

#if defined(SORT_MODE) || defined(SLAB_MODE)
unsigned long long expiringKeyCount(void) {
    unsigned long long keys = 0;
    for (int i = 0; i < g_expire_algorithm.db_num; i++) {
        if (g_expire_index[i]) {
            keys += g_expire_index[i]->length;
        }
    }
    return keys;
}
#endif

void _moduleAssert(const char *estr, const char *file, int line) {
    fprintf(stderr, "=== ASSERTION FAILED ===");
    fprintf(stderr, "==> %s:%d '%s' is not true", file, line, estr);
    *((char *)-1) = 'x';
}

/* A copy of str for a TairHash to keep. Client arguments are copied rather than retained: in a
 * transaction the client holds its arguments until EXEC returns, while a later command of the
 * transaction can free the hash on the lazyfree thread, and both would update the reference
 * count of a shared string at once. */
RedisModuleString *ownString(RedisModuleString *str) {
    return RedisModule_CreateStringFromString(NULL, str);
}

/* Members of g_expire_index are the index's own copies of key names, never a retained obj->key.
 * The server may free a TairHash, and with it obj->key, on a lazyfree thread, while the index is
 * used from the main thread; sharing the refcounted string between them races on its refcount. */
RedisModuleString *expireIndexMember(const tairHashObj *obj) {
    return RedisModule_CreateStringFromString(NULL, obj->key);
}

int isExpire(long long when) {
    if (when == 0) return 0;
    return RedisModule_Milliseconds() > when;
}

int delEmptyTairHashIfNeeded(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *raw_key, tairHashObj *obj) {
    if (!obj || (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE) || (ftSize(&obj->hash) != 0)) {
        return 0;
    }
    /* raw_key is still used after the key is deleted, so it must not be owned by obj. */
    Module_Assert(raw_key != obj->key);

    if (key == NULL) {
        key = RedisModule_OpenKey(ctx, raw_key, REDISMODULE_WRITE);
        if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
            RedisModule_CloseKey(key);
            return 0;
        }
    }

    if (redis_major_ver < 6 || (redis_major_ver == 6 && redis_minor_ver < 2)) {
        /* See bugfix: https://github.com/redis/redis/pull/8617
                       https://github.com/redis/redis/pull/8097
                       https://github.com/redis/redis/pull/7037
        */
        RedisModule_CloseKey(key);
        RedisModuleCtx *ctx2 = RedisModule_GetThreadSafeContext(NULL);
        RedisModule_SelectDb(ctx2, RedisModule_GetSelectedDb(ctx));
        RedisModuleCallReply *reply = RedisModule_Call(ctx2, "DEL", "s!", raw_key);
        if (reply != NULL) RedisModule_FreeCallReply(reply);
        RedisModule_FreeThreadSafeContext(ctx2);
    } else {
        RedisModule_DeleteKey(key);
        RedisModule_Replicate(ctx, "DEL", "s", raw_key);
        if (RedisModule_NotifyKeyspaceEvent) {
            RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", raw_key);
        }
        RedisModule_CloseKey(key);
    }
    return 1;
}

void notifyFieldSpaceEvent(char *event, RedisModuleString *key, RedisModuleString *field, int dbid) {
    size_t key_len, field_len;
    const char *key_ptr = RedisModule_StringPtrLen(key, &key_len);
    const char *field_ptr = RedisModule_StringPtrLen(field, &field_len);
    /* tairhash@<db>@<key>__:<event> <field> notifications. */
    RedisModuleString *channel = RedisModule_CreateStringPrintf(NULL, "tairhash@%d@%s__:%s", dbid, key_ptr, event);
    RedisModuleString *message = RedisModule_CreateStringFromString(NULL, field);

    if (RedisModule_PublishMessage) {
        RedisModule_PublishMessage(NULL, channel, message);
    } else {
        RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
        RedisModule_SelectDb(ctx, dbid);
        RedisModuleCallReply *reply = RedisModule_Call(ctx, "PUBLISH", "ss", channel, message);
        if (reply != NULL) RedisModule_FreeCallReply(reply);
        RedisModule_FreeThreadSafeContext(ctx);
    }

    RedisModule_FreeString(NULL, channel);
    RedisModule_FreeString(NULL, message);
}

static void tairhashScanCallback(void *privdata, TairHashField *f) {
    m_listAddNodeTail((list *)privdata, f);
}

static void tairHashTypeReleaseObject(struct tairHashObj *o) {
    expiringKeyReleased(o);
    ftClear(&o->hash);
    if (o->expire_index) {
#ifdef SLAB_MODE
        slab_free(o->expire_index);
#else
        m_zslFree(o->expire_index);
#endif
    }
    if (o->key) {
        RedisModule_FreeString(NULL, o->key);
    }
    RedisModule_Free(o);
}

/* The server treats a free effort of 0 as "always free asynchronously", which would hand
 * every emptied TairHash to a background thread. Count the object itself so small values
 * are freed synchronously. */
static size_t tairHashFreeEffort(const struct tairHashObj *o) {
    return 1 + ftSize(&o->hash) + TAIRHASH_EXPIRE_INDEX_LEN(o);
}

static struct tairHashObj *createTairHashTypeObject() {
    tairHashObj *o = RedisModule_Calloc(1, sizeof(*o));
    ftInit(&o->hash);
    return o;
}

/* ========================== Field access =============================*/

static TairHashField *fieldLookup(tairHashObj *o, RedisModuleString *name) {
    size_t len;
    const char *ptr = RedisModule_StringPtrLen(name, &len);
    return ftFind(&o->hash, ptr, len);
}

/* Adds a field with this name and value; version and expire start at 0. */
static TairHashField *fieldInsert(tairHashObj *o, RedisModuleString *name, RedisModuleString *value) {
    size_t name_len, value_len;
    const char *name_ptr = RedisModule_StringPtrLen(name, &name_len);
    const char *value_ptr = RedisModule_StringPtrLen(value, &value_len);
    TairHashField *f = fieldCreate(name_ptr, name_len, value_ptr, value_len);
    ftAdd(&o->hash, f);
    return f;
}

/* Returns the field, which may have moved: use the returned pointer. */
static TairHashField *fieldSetValue(tairHashObj *o, TairHashField *f, RedisModuleString *value) {
    size_t len;
    const char *ptr = RedisModule_StringPtrLen(value, &len);
    return ftSetValue(&o->hash, f, ptr, len);
}

static int fieldDelete(tairHashObj *o, RedisModuleString *name) {
    size_t len;
    const char *ptr = RedisModule_StringPtrLen(name, &len);
    return ftDelete(&o->hash, ptr, len);
}

static RedisModuleString *fieldNameString(RedisModuleCtx *ctx, const TairHashField *f) {
    return RedisModule_CreateString(ctx, fieldName(f), f->name_len);
}

static int replyWithFieldName(RedisModuleCtx *ctx, const TairHashField *f) {
    return RedisModule_ReplyWithStringBuffer(ctx, fieldName(f), f->name_len);
}

static int replyWithFieldValue(RedisModuleCtx *ctx, const TairHashField *f) {
    return RedisModule_ReplyWithStringBuffer(ctx, fieldValue(f), f->value_len);
}

void tairHashEnsureExpireIndex(tairHashObj *o) {
    if (o->expire_index) {
        return;
    }
#ifdef SLAB_MODE
    o->expire_index = slab_create();
#else
    o->expire_index = m_zslCreate();
#endif
}

/* What field expiry may do for the command or the timer that runs in ctx. */
enum {
    FIELD_EXPIRY_DELETE, /* an expired field is deleted and the deletion replicated */
    FIELD_EXPIRY_HIDE,   /* an expired field counts as absent and is left where it is */
    FIELD_EXPIRY_OFF,    /* no field counts as expired */
};

static int fieldExpiry(RedisModuleCtx *ctx) {
    int flags = RedisModule_GetContextFlags(ctx);

    /* A command read back from the AOF, or applied by a replica from its master, runs for the
     * second time. Where it first ran, each field was live or expired by the clock of that
     * moment, and every field that expired there is deleted by an EXHDEL or DEL of its own in
     * the same stream. Here the command has to find the fields it found there, whatever the time
     * is now. A server that does not report these flags (LOADING came with Redis 5.0.6,
     * REPLICATED with 5.0.4) goes on judging by its own clock. */
    if (flags & (REDISMODULE_CTX_FLAGS_LOADING | REDISMODULE_CTX_FLAGS_REPLICATED)) {
        return FIELD_EXPIRY_OFF;
    }

    if (flags & REDISMODULE_CTX_FLAGS_SLAVE || flags & REDISMODULE_CTX_FLAGS_READONLY) {
        return FIELD_EXPIRY_HIDE;
    }

    /* While clients are paused, as for a failover, the dataset has to stay as it is, and the
     * server asserts that nothing is propagated. Servers before Redis 6.0 cannot tell. */
    if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
        return FIELD_EXPIRY_HIDE;
    }

    return FIELD_EXPIRY_DELETE;
}

int isReadOnlyStatus(RedisModuleCtx *ctx) {
    return fieldExpiry(ctx) != FIELD_EXPIRY_DELETE;
}

/* ========================== Common  func =============================*/
void activeExpireTimerHandler(RedisModuleCtx *ctx, void *data) {
    REDISMODULE_NOT_USED(data);
    RedisModule_AutoMemory(ctx);
    static uint64_t loop_cnt = 0, total_expire_time = 0;
    static unsigned int current_db = 0;
    /* With fewer databases than a tick may visit, the tick would come back to the same one. */
    int dbs_per_call = g_expire_algorithm.dbs_per_active_loop;
    if (dbs_per_call > g_expire_algorithm.db_num) {
        dbs_per_call = g_expire_algorithm.db_num;
    }

    if (isReadOnlyStatus(ctx)) {
        goto restart;
    }

    long long start = RedisModule_Milliseconds();

    for (int i = 0; i < dbs_per_call; ++i) {
        current_db = current_db % g_expire_algorithm.db_num;
        if (RedisModule_SelectDb(ctx, current_db) != REDISMODULE_OK) {
            current_db++;
            continue;
        }

        if (RedisModule_DbSize && RedisModule_DbSize(ctx) == 0) {
            current_db++;
            continue;
        }

        /* Perform active expire algorithm. */
        g_expire_algorithm.activeExpire(ctx, current_db, g_expire_algorithm.keys_per_active_loop);
        current_db++;
    }

    g_expire_algorithm.stat_last_active_expire_time_msec = RedisModule_Milliseconds() - start;
    if (g_expire_algorithm.stat_max_active_expire_time_msec < g_expire_algorithm.stat_last_active_expire_time_msec) {
        g_expire_algorithm.stat_max_active_expire_time_msec = g_expire_algorithm.stat_last_active_expire_time_msec;
    }
    total_expire_time += g_expire_algorithm.stat_last_active_expire_time_msec;
    if (++loop_cnt % 10 == 0) {
        g_expire_algorithm.stat_avg_active_expire_time_msec = total_expire_time / loop_cnt;
        loop_cnt = 0;
        total_expire_time = 0;
    }

restart:
    if (g_expire_algorithm.enable_active_expire) {
        g_expire_timer_id = RedisModule_CreateTimer(ctx, g_expire_algorithm.active_expire_period, activeExpireTimerHandler, NULL);
    }
}

/* Expires `field` of o if its TTL has passed and returns whether it has: 1 for a field that was
 * deleted, and also for one that is only hidden because nothing may be deleted here (see
 * fieldExpiry). A caller that passes is_timer takes out of the expire index what it gets 1 for,
 * so for it 1 means deleted and nothing else. */
int fieldExpireIfNeeded(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, int is_timer) {
    TairHashField *f = fieldLookup(o, field);
    if (f == NULL) {
        return 0;
    }

    long long when = f->expire;
    if (when == 0) {
        return 0;
    }

    int expiry = fieldExpiry(ctx);
    if (expiry == FIELD_EXPIRY_OFF) {
        return 0;
    }

    long long now = RedisModule_Milliseconds();
    if (expiry == FIELD_EXPIRY_HIDE) {
        return is_timer ? 0 : now > when;
    }

    if (now < when) {
        return 0;
    }

    return g_expire_algorithm.deleteAndPropagate(ctx, dbid, key, o, field, when, is_timer);
}

#if defined(SORT_MODE) || defined(SLAB_MODE)
/* Whether o has a field with a TTL, and with it an entry in the db expire index. If it has,
 * *min_expire is set to the earliest field expire, which is the score of that entry. */
int tairHashMinExpire(const tairHashObj *o, long long *min_expire) {
    if (TAIRHASH_EXPIRE_INDEX_LEN(o) == 0) {
        return 0;
    }
#ifdef SLAB_MODE
    *min_expire = o->expire_index->header->level[0].forward->expire_min;
#else
    *min_expire = o->expire_index->header->level[0].forward->score;
#endif
    return 1;
}

/* Takes up to `limit` entries due at `now` out of the db expire index, in expire order. The
 * returned list owns their key names. */
list *popDueExpireIndexEntries(int dbid, long long now, long limit) {
    list *keys = m_listCreate();
    if (g_expire_index[dbid] == NULL) {
        return keys;
    }
    m_zskiplistNode *ln = g_expire_index[dbid]->header->level[0].forward;
    unsigned int taken = 0;
    while (ln && limit-- > 0 && ln->score <= now) {
        expiringKey *ek = RedisModule_Alloc(sizeof(*ek));
        ek->key = ln->member;
        ek->score = ln->score;
        ln->member = NULL;
        m_listAddNodeTail(keys, ek);
        taken++;
        ln = ln->level[0].forward;
    }
    if (taken) {
        m_zslDeleteRangeByRank(g_expire_index[dbid], 1, taken);
    }
    return keys;
}

/* Opens the key of an entry taken out of the db expire index and returns its TairHash if the
 * entry still indexes it, that is the key holds a TairHash whose earliest field expire is the
 * entry's score. Otherwise the entry is stale and NULL is returned: opening the key for writing
 * expired it, or the server freed the key without unlinking it (a key already expired when an
 * RDB is loaded) and the name now holds nothing, another type or a later TairHash that has an
 * entry of its own. */
tairHashObj *openIndexedTairHash(RedisModuleCtx *ctx, const expiringKey *ek, int mode, RedisModuleKey **key) {
    *key = RedisModule_OpenKey(ctx, ek->key, mode);
    if (RedisModule_KeyType(*key) == REDISMODULE_KEYTYPE_MODULE && RedisModule_ModuleTypeGetType(*key) == TairHashType) {
        tairHashObj *o = RedisModule_ModuleTypeGetValue(*key);
        long long min_expire;
        if (tairHashMinExpire(o, &min_expire) && min_expire == ek->score) {
            return o;
        }
    }
    RedisModule_CloseKey(*key);
    *key = NULL;
    return NULL;
}

/* Removes an entry from the list, freeing its key name unless it was handed back to the index
 * (ek->key set to NULL). */
void releaseExpiringKey(list *keys, m_listNode *node) {
    expiringKey *ek = listNodeValue(node);
    if (ek->key) {
        RedisModule_FreeString(NULL, ek->key);
    }
    RedisModule_Free(ek);
    m_listDelNode(keys, node);
}

void swapDbCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    REDISMODULE_NOT_USED(e);
    REDISMODULE_NOT_USED(sub);
    RedisModule_AutoMemory(ctx);

    RedisModuleSwapDbInfo *ei = data;

    int from_dbid = ei->dbnum_first;
    int to_dbid = ei->dbnum_second;

    /* 1. swap index */
    m_zskiplist *tmp_zsl = g_expire_index[from_dbid];
    g_expire_index[from_dbid] = g_expire_index[to_dbid];
    g_expire_index[to_dbid] = tmp_zsl;

    /* 2. swap statistics*/
    uint64_t tmp_stat = g_expire_algorithm.stat_active_expired_field[from_dbid];
    g_expire_algorithm.stat_active_expired_field[from_dbid] = g_expire_algorithm.stat_active_expired_field[to_dbid];
    g_expire_algorithm.stat_active_expired_field[to_dbid] = tmp_stat;

    tmp_stat = g_expire_algorithm.stat_passive_expired_field[from_dbid];
    g_expire_algorithm.stat_passive_expired_field[from_dbid] = g_expire_algorithm.stat_passive_expired_field[to_dbid];
    g_expire_algorithm.stat_passive_expired_field[to_dbid] = tmp_stat;
}

void flushDbCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    REDISMODULE_NOT_USED(e);
    REDISMODULE_NOT_USED(sub);
    RedisModule_AutoMemory(ctx);

    RedisModuleFlushInfo *fi = data;
    if (sub == REDISMODULE_SUBEVENT_FLUSHDB_START) {
        for (int i = 0; i < g_expire_algorithm.db_num; i++) {
            if ((fi->dbnum == -1 || fi->dbnum == i) && g_expire_index[i]) {
                m_zslFree(g_expire_index[i]);
                g_expire_index[i] = NULL;
            }
        }
    }
}

static int keySpaceNotification(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(type);
    RedisModule_AutoMemory(ctx);

#define CMD_NONE 0
#define CMD_RENAME 1
#define CMD_MOVE 2

    static RedisModuleString *from_key = NULL, *to_key = NULL;
    static int from_dbid, to_dbid;
    static int cmd_flag = CMD_NONE;

    int dbid = RedisModule_GetSelectedDb(ctx);

    if (strcmp(event, "rename_from") == 0) {
        from_key = RedisModule_CreateStringFromString(NULL, key);
    } else if (strcmp(event, "rename_to") == 0) {
        to_key = RedisModule_CreateStringFromString(NULL, key);
        cmd_flag = CMD_RENAME;
    } else if (strcmp(event, "move_from") == 0) {
        from_dbid = dbid;
    } else if (strcmp(event, "move_to") == 0) {
        to_dbid = dbid;
        cmd_flag = CMD_MOVE;
    }

    if (cmd_flag != CMD_NONE) {
        RedisModuleString *local_from_key = NULL, *local_to_key = NULL;
        int local_from_dbid, local_to_dbid;
        /* We assign values in advance so that `move` and `rename` can be processed uniformly. */
        if (cmd_flag == CMD_RENAME) {
            local_from_key = from_key;
            local_to_key = to_key;
            /* `rename` does not change the dbid of the key. */
            local_from_dbid = dbid;
            local_to_dbid = dbid;
        } else {
            /* `move` does not change the name of the key. */
            local_from_key = key;
            local_to_key = key;
            local_from_dbid = from_dbid;
            local_to_dbid = to_dbid;
        }

        RedisModule_SelectDb(ctx, local_to_dbid);
        RedisModuleKey *real_key = RedisModule_OpenKey(ctx, local_to_key, REDISMODULE_READ | REDISMODULE_WRITE);
        tairHashObj *tair_hash_obj = NULL;
        if (RedisModule_KeyType(real_key) == REDISMODULE_KEYTYPE_MODULE && RedisModule_ModuleTypeGetType(real_key) == TairHashType) {
            tair_hash_obj = RedisModule_ModuleTypeGetValue(real_key);
        }

        /* Keys of every type are renamed and moved. Only a tairhash tracks its own name,
         * and only one with expiring fields has an entry in the db expire index. */
        if (tair_hash_obj) {
            long long previous_index = 0;
            int indexed = tairHashMinExpire(tair_hash_obj, &previous_index);
            if (indexed) {
                m_zslDelete(dbExpireIndex(local_from_dbid), previous_index, local_from_key, NULL);
            }
            if (tair_hash_obj->key) {
                RedisModule_FreeString(NULL, tair_hash_obj->key);
                tair_hash_obj->key = RedisModule_CreateStringFromString(NULL, local_to_key);
            }
            if (indexed) {
                m_zslInsert(dbExpireIndex(local_to_dbid), previous_index, expireIndexMember(tair_hash_obj));
            }
        }

        /* Release sources. */
        if (cmd_flag == CMD_RENAME) {
            if (to_key) {
                RedisModule_FreeString(NULL, to_key);
                to_key = NULL;
            }

            if (from_key) {
                RedisModule_FreeString(NULL, from_key);
                from_key = NULL;
            }
        }

        /* Reset flag. */
        cmd_flag = CMD_NONE;
    }

    return REDISMODULE_OK;
}

void infoFunc(RedisModuleInfoCtx *ctx, int for_crash_report) {
    RedisModule_InfoAddSection(ctx, "Statistics");
    RedisModule_InfoAddFieldCString(ctx, "active_expire_mode", TAIRHASH_EXPIRE_MODE_NAME);
    RedisModule_InfoAddFieldULongLong(ctx, "expiring_keys", expiringKeyCount());
    RedisModule_InfoAddFieldLongLong(ctx, "active_expire_enable", g_expire_algorithm.enable_active_expire);
    RedisModule_InfoAddFieldLongLong(ctx, "active_expire_period", g_expire_algorithm.active_expire_period);
    RedisModule_InfoAddFieldLongLong(ctx, "active_expire_keys_per_loop", g_expire_algorithm.keys_per_active_loop);
    RedisModule_InfoAddFieldLongLong(ctx, "active_expire_dbs_per_loop", g_expire_algorithm.dbs_per_active_loop);
    RedisModule_InfoAddFieldLongLong(ctx, "active_expire_last_time_msec", g_expire_algorithm.stat_last_active_expire_time_msec);
    RedisModule_InfoAddFieldLongLong(ctx, "active_expire_max_time_msec", g_expire_algorithm.stat_max_active_expire_time_msec);
    RedisModule_InfoAddFieldLongLong(ctx, "active_expire_avg_time_msec", g_expire_algorithm.stat_avg_active_expire_time_msec);
    RedisModule_InfoAddFieldLongLong(ctx, "passive_expire_keys_per_loop", g_expire_algorithm.keys_per_passive_loop);

    RedisModule_InfoAddSection(ctx, "ActiveExpiredFields");
    char buf[10];
    for (int i = 0; i < g_expire_algorithm.db_num; ++i) {
        if ((g_expire_index[i] == NULL || g_expire_index[i]->length == 0) && g_expire_algorithm.stat_active_expired_field[i] == 0) {
            continue;
        }
        snprintf(buf, sizeof(buf), "db%d", i);
        RedisModule_InfoAddFieldLongLong(ctx, buf, g_expire_algorithm.stat_active_expired_field[i]);
    }

    RedisModule_InfoAddSection(ctx, "PassiveExpiredFields");
    for (int i = 0; i < g_expire_algorithm.db_num; ++i) {
        if ((g_expire_index[i] == NULL || g_expire_index[i]->length == 0) && g_expire_algorithm.stat_passive_expired_field[i] == 0) {
            continue;
        }
        snprintf(buf, sizeof(buf), "db%d", i);
        RedisModule_InfoAddFieldLongLong(ctx, buf, g_expire_algorithm.stat_passive_expired_field[i]);
    }
}

#endif

void startExpireTimer(RedisModuleCtx *ctx, void *data) {
    if (!g_expire_algorithm.enable_active_expire) {
        return;
    }

    if (RedisModule_GetTimerInfo(ctx, g_expire_timer_id, NULL, NULL) == REDISMODULE_OK) {
        return;
    }

    g_expire_timer_id = RedisModule_CreateTimer(ctx, g_expire_algorithm.active_expire_period, activeExpireTimerHandler, data);
}

/* Whether every field name among argv[first..argc), taken `step` arguments at a time and each
 * followed by its value when `with_value` is set, fits in a field; replies an error if not. */
static int fieldArgsFit(RedisModuleCtx *ctx, RedisModuleString **argv, int first, int argc, int step, int with_value) {
    size_t len;
    for (int i = first; i < argc; i += step) {
        RedisModule_StringPtrLen(argv[i], &len);
        int fits = len <= FT_NAME_MAX_LEN;
        if (fits && with_value) {
            RedisModule_StringPtrLen(argv[i + 1], &len);
            fits = len <= FT_VALUE_MAX_LEN;
        }
        if (!fits) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_FIELD_TOO_LONG);
            return 0;
        }
    }
    return 1;
}

/* Converts a TTL argument t, in seconds when `seconds` is set, to a unix time in milliseconds
 * counted from `base` (0 for an absolute time). Fails if the result does not fit. */
static int expireToAbsoluteMs(long long t, int seconds, long long base, long long *ms) {
    if (seconds) {
        if (t > LLONG_MAX / 1000) {
            return REDISMODULE_ERR;
        }
        t *= 1000;
    }
    if (t > LLONG_MAX - base) {
        return REDISMODULE_ERR;
    }
    *ms = t + base;
    return REDISMODULE_OK;
}

/* Field expiry earlier in a write command may have emptied the hash; a write that then fails
 * must not leave the empty hash behind. */
static void abortWrite(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *keyname, tairHashObj *o) {
    if (o) {
        delEmptyTairHashIfNeeded(ctx, key, keyname, o);
    }
}

/* fieldExpireIfNeeded for a read command, recording in *expired that the read found an expired field. */
static int readExpireIfNeeded(RedisModuleCtx *ctx, int *expired, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field) {
    int ret = fieldExpireIfNeeded(ctx, dbid, key, o, field, 0);
    *expired |= ret;
    return ret;
}

/* Starts iterating a hash for a full read. Scan mode expires fields during the read, which
 * must not rehash under the iterator; the other modes only filter them out and need not
 * write to the table at all. */
static void fullReadIterInit(fieldTableIterator *it, tairHashObj *o) {
#if defined(SORT_MODE) || defined(SLAB_MODE)
    ftIterInitReadOnly(it, &o->hash);
#else
    ftIterInit(it, &o->hash);
#endif
}

/* Whether a full read (EXHKEYS, EXHVALS, EXHGETALL) found at time `now` leaves out field f
 * because it expired. Scan mode then expires it, which needs its name as a string; the other
 * modes leave that to their expire index. */
static int fullReadSkips(RedisModuleCtx *ctx, int *expired, int dbid, RedisModuleString *keyname, tairHashObj *o, TairHashField *f, long long now) {
    if (f->expire == 0 || f->expire > now) {
        return 0;
    }
#if defined(SORT_MODE) || defined(SLAB_MODE)
    return now > f->expire;
#else
    return readExpireIfNeeded(ctx, expired, dbid, keyname, o, fieldNameString(ctx, f));
#endif
}

/* Read commands open their key read-only, so that a read leaves WATCH and client-side caching
 * alone: closing a key opened for writing signals it modified. A read that expired fields on a
 * writable master did modify it, so the key is then deleted if the read emptied it, through a
 * handle opened for writing, or else signalled as modified. */
static void finishRead(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *keyname, tairHashObj *o, int expired) {
    if (!expired || isReadOnlyStatus(ctx)) {
        return;
    }
    RedisModule_CloseKey(key);
    if (delEmptyTairHashIfNeeded(ctx, NULL, keyname, o)) {
        return;
    }
    if (RedisModule_SignalModifiedKey) {
        RedisModule_SignalModifiedKey(ctx, keyname);
    } else {
        /* Before Redis 6.0: closing a key opened for writing signals it as modified. */
        RedisModule_CloseKey(RedisModule_OpenKey(ctx, keyname, REDISMODULE_WRITE));
    }
}

static int mstrcasecmp(const RedisModuleString *rs1, const char *s2) {
    size_t n1 = strlen(s2);
    size_t n2;
    const char *s1 = RedisModule_StringPtrLen(rs1, &n2);
    if (n1 != n2) {
        return -1;
    }
    return strncasecmp(s1, s2, n1);
}

int tairHashExpireGenericFunc(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, long long basetime, int unit) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4 || argc > 7) {
        return RedisModule_WrongArity(ctx);
    }

    long long milliseconds;
    long long version = 0;
    int field_expired = 0;
    int nokey;

    if (RedisModule_StringToLongLong(argv[3], &milliseconds) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (milliseconds < 0) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    /* A TTL of 0 expires the field at once. */
    if (milliseconds == 0) {
        milliseconds = 1;
    } else if (expireToAbsoluteMs(milliseconds, unit == UNIT_SECONDS, basetime, &milliseconds) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INVALID_EXPIRE);
        return REDISMODULE_ERR;
    }

    RedisModuleString *version_p = NULL;
    int ex_flags = TAIR_HASH_SET_NO_FLAGS;

    if (argc > 4) {
        for (int j = 4; j < argc; j++) {
            RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];
            if (!mstrcasecmp(argv[4], "ver") && !(ex_flags & TAIR_HASH_SET_WITH_ABS_VER) && !(ex_flags & TAIR_HASH_SET_WITH_GT_VER) && next) {
                ex_flags |= TAIR_HASH_SET_WITH_VER;
                version_p = next;
                j++;
            } else if (!mstrcasecmp(argv[4], "abs") && !(ex_flags & TAIR_HASH_SET_WITH_VER) && !(ex_flags & TAIR_HASH_SET_WITH_GT_VER) && next) {
                ex_flags |= TAIR_HASH_SET_WITH_ABS_VER;
                version_p = next;
                j++;
            } else if (!mstrcasecmp(argv[4], "gt") && !(ex_flags & TAIR_HASH_SET_WITH_VER) && !(ex_flags & TAIR_HASH_SET_WITH_ABS_VER) && next) {
                ex_flags |= TAIR_HASH_SET_WITH_GT_VER;
                version_p = next;
                j++;
            } else {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
                return REDISMODULE_ERR;
            }
        }
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0 || ((ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) && version == 0)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModuleString *skey = argv[2], *pkey = argv[1];

    int dbid = RedisModule_GetSelectedDb(ctx);
    if (fieldExpireIfNeeded(ctx, dbid, pkey, tair_hash_obj, skey, 0)) {
        field_expired = 1;
    }

    TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, skey);
    if (field_expired || tair_hash_val == NULL) {
        nokey = 1;
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        nokey = 0;
        if (ex_flags & TAIR_HASH_SET_WITH_VER) {
            if (version != 0 && version != tair_hash_val->version) {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
                return REDISMODULE_ERR;
            }
        } else if (ex_flags & TAIR_HASH_SET_WITH_GT_VER) {
            if (version <= tair_hash_val->version) {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
                return REDISMODULE_ERR;
            }
        }
        if (!(ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) && tair_hash_val->version == LLONG_MAX) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION_OVERFLOW);
            return REDISMODULE_ERR;
        }

        if (milliseconds > 0) {
            int dbid = RedisModule_GetSelectedDb(ctx);
            if (nokey || tair_hash_val->expire == 0) {
                g_expire_algorithm.insert(ctx, dbid, argv[1], tair_hash_obj, skey, milliseconds);
            } else {
                g_expire_algorithm.update(ctx, dbid, argv[1], tair_hash_obj, skey, tair_hash_val->expire, milliseconds);
            }
            ftSetExpire(&tair_hash_obj->hash, tair_hash_val, milliseconds);
        }

        RedisModule_ReplyWithLongLong(ctx, 1);

        if (ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) {
            tair_hash_val->version = version;
        } else {
            tair_hash_val->version += 1;
        }

        size_t vlen = 0, VSIZE_MAX = 5;
        RedisModuleString **v = RedisModule_Alloc(sizeof(RedisModuleString *) * VSIZE_MAX);
        v[vlen++] = RedisModule_CreateStringFromString(ctx, argv[1]);
        v[vlen++] = RedisModule_CreateStringFromString(ctx, argv[2]);
        v[vlen++] = RedisModule_CreateStringFromLongLong(ctx, tair_hash_val->expire);
        if (version_p) {
            v[vlen++] = RedisModule_CreateString(ctx, "ABS", 3);
            v[vlen++] = RedisModule_CreateStringFromLongLong(ctx, tair_hash_val->version);
        }

        RedisModule_Replicate(ctx, "EXHPEXPIREAT", "v", v, vlen);
        RedisModule_Free(v);
    }

    delEmptyTairHashIfNeeded(ctx, key, pkey, tair_hash_obj);
    return REDISMODULE_OK;
}

int tairHashTTLGenericFunc(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int unit) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    int field_expired = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -2);
        return REDISMODULE_OK;
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    RedisModuleString *pkey = argv[1], *skey = argv[2];

    int dbid = RedisModule_GetSelectedDb(ctx);
    if (readExpireIfNeeded(ctx, &expired, dbid, pkey, tair_hash_obj, skey)) {
        field_expired = 1;
    }

    TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, skey);
    if (field_expired || tair_hash_val == NULL) {
        RedisModule_ReplyWithLongLong(ctx, -3);
    } else {
        if (tair_hash_val->expire == 0) {
            RedisModule_ReplyWithLongLong(ctx, -1);
        } else {
            long long ttl = tair_hash_val->expire - RedisModule_Milliseconds();
            if (ttl < 0) {
                ttl = 0;
            }
            if (UNIT_SECONDS == unit) {
                RedisModule_ReplyWithLongLong(ctx, (ttl + 500) / 1000);
            } else {
                RedisModule_ReplyWithLongLong(ctx, ttl);
            }
        }
    }

    finishRead(ctx, key, pkey, tair_hash_obj, expired);
    return REDISMODULE_OK;
}

int mstring2ld(RedisModuleString *val, long double *r_val) {
    if (!val)
        return REDISMODULE_ERR;

    size_t t_len;
    const char *t_ptr = RedisModule_StringPtrLen(val, &t_len);
    if (m_string2ld(t_ptr, t_len, r_val) == 0) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

/* Opens <keyname> for writing after passive expiry has run on it. In scan mode passive
 * expiry concerns only this key and needs a key open of its own, so it runs (and the key
 * is opened again) only when the hash has expiring fields, which the open key tells for
 * free; the other modes expire db-wide and always run it first. */
static RedisModuleKey *openKeyAfterPassiveExpire(RedisModuleCtx *ctx, int dbid, RedisModuleString *keyname) {
#if !defined(SORT_MODE) && !defined(SLAB_MODE)
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyname, REDISMODULE_READ | REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_MODULE || RedisModule_ModuleTypeGetType(key) != TairHashType ||
        TAIRHASH_EXPIRE_INDEX_LEN((tairHashObj *)RedisModule_ModuleTypeGetValue(key)) == 0) {
        return key;
    }
    RedisModule_CloseKey(key);
#endif
    g_expire_algorithm.passiveExpire(ctx, dbid, keyname);
    return RedisModule_OpenKey(ctx, keyname, REDISMODULE_READ | REDISMODULE_WRITE);
}

/* ========================= "tairhash" type commands ======================= */

/* EXHSET <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [NX|XX] [VER version | ABS version] [KEEPTTL] */
int TairHashTypeHset_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    if (!fieldArgsFit(ctx, argv, 2, 3, 2, 1)) {
        return REDISMODULE_ERR;
    }

    long long milliseconds = 0, expire = 0, version = 0;
    RedisModuleString *expire_p = NULL, *version_p = NULL;
    int ex_flags = TAIR_HASH_SET_NO_FLAGS;
    int nokey = 0;

    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, RedisModule_GetSelectedDb(ctx), argv[1]);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    for (int j = 4; j < argc; j++) {
        RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];
        if (!mstrcasecmp(argv[j], "nx") && !(ex_flags & TAIR_HASH_SET_XX)) {
            ex_flags |= TAIR_HASH_SET_NX;
        } else if (!mstrcasecmp(argv[j], "xx") && !(ex_flags & TAIR_HASH_SET_NX)) {
            ex_flags |= TAIR_HASH_SET_XX;
        } else if (!mstrcasecmp(argv[j], "ex") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_EX;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "exat") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_EX;
            ex_flags |= TAIR_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "px") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_PX;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "pxat") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_PX;
            ex_flags |= TAIR_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "ver") && !(ex_flags & TAIR_HASH_SET_WITH_ABS_VER) && !(ex_flags & TAIR_HASH_SET_WITH_GT_VER) && next) {
            ex_flags |= TAIR_HASH_SET_WITH_VER;
            version_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "abs") && !(ex_flags & TAIR_HASH_SET_WITH_VER) && !(ex_flags & TAIR_HASH_SET_WITH_GT_VER) && next) {
            ex_flags |= TAIR_HASH_SET_WITH_ABS_VER;
            version_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "gt") && !(ex_flags & TAIR_HASH_SET_WITH_VER) && !(ex_flags & TAIR_HASH_SET_WITH_ABS_VER) && next) {
            ex_flags |= TAIR_HASH_SET_WITH_GT_VER;
            version_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "keepttl") && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_PX)) {
            ex_flags |= TAIR_HASH_SET_KEEPTTL;
        } else {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (expire_p && expire < 0) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    /* Computed before anything changes, so that an expire that does not fit is rejected cleanly. */
    if (expire_p && expire > 0 &&
        expireToAbsoluteMs(expire, ex_flags & TAIR_HASH_SET_EX, (ex_flags & TAIR_HASH_SET_ABS_EXPIRE) ? 0 : RedisModule_Milliseconds(),
                           &milliseconds) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INVALID_EXPIRE);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0 || ((ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) && version == 0)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleString *pkey = argv[1], *skey = argv[2];

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        if (ex_flags & TAIR_HASH_SET_XX) {
            RedisModule_ReplyWithLongLong(ctx, -1);
            return REDISMODULE_ERR;
        }
        tair_hash_obj = createTairHashTypeObject();
        tair_hash_obj->key = RedisModule_CreateStringFromString(NULL, pkey);
        RedisModule_ModuleTypeSetValue(key, TairHashType, tair_hash_obj);
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    fieldExpireIfNeeded(ctx, dbid, pkey, tair_hash_obj, skey, 0);
    TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, skey);
    if (tair_hash_val == NULL) {
        if (ex_flags & TAIR_HASH_SET_XX) {
            abortWrite(ctx, key, pkey, tair_hash_obj);
            RedisModule_ReplyWithLongLong(ctx, -1);
            return REDISMODULE_ERR;
        }
        nokey = 1;
        tair_hash_val = fieldInsert(tair_hash_obj, skey, argv[3]);
    } else {
        nokey = 0;
        if (ex_flags & TAIR_HASH_SET_NX) {
            RedisModule_ReplyWithLongLong(ctx, -1);
            return REDISMODULE_ERR;
        }

        /* Version equals 0 means no version checking */
        if (ex_flags & TAIR_HASH_SET_WITH_VER) {
            if (version != 0 && version != tair_hash_val->version) {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
                return REDISMODULE_ERR;
            }
        } else if (ex_flags & TAIR_HASH_SET_WITH_GT_VER) {
            if (version <= tair_hash_val->version) {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
                return REDISMODULE_ERR;
            }
        }
        if (!(ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) && tair_hash_val->version == LLONG_MAX) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION_OVERFLOW);
            return REDISMODULE_ERR;
        }
        tair_hash_val = fieldSetValue(tair_hash_obj, tair_hash_val, argv[3]);
    }

    if (ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) {
        tair_hash_val->version = version;
    } else {
        tair_hash_val->version += 1;
    }

    if (expire_p && expire == 0) {
        milliseconds = 1;
    }

    if (milliseconds == 0 && !(ex_flags & TAIR_HASH_SET_KEEPTTL)) {
        g_expire_algorithm.delete(ctx, dbid, argv[1], tair_hash_obj, skey, tair_hash_val->expire);
        ftSetExpire(&tair_hash_obj->hash, tair_hash_val, 0);
    }

    if (milliseconds > 0) {
        if (nokey || tair_hash_val->expire == 0) {
            g_expire_algorithm.insert(ctx, dbid, argv[1], tair_hash_obj, skey, milliseconds);
        } else {
            g_expire_algorithm.update(ctx, dbid, argv[1], tair_hash_obj, skey, tair_hash_val->expire, milliseconds);
        }
        ftSetExpire(&tair_hash_obj->hash, tair_hash_val, milliseconds);
    }

    RedisModule_ReplyWithLongLong(ctx, nokey ? 1 : 0);

    size_t vlen = 0, VSIZE_MAX = 7;
    RedisModuleString **v = RedisModule_Alloc(sizeof(RedisModuleString *) * VSIZE_MAX);
    v[vlen++] = RedisModule_CreateStringFromString(ctx, argv[1]);
    v[vlen++] = RedisModule_CreateStringFromString(ctx, argv[2]);
    v[vlen++] = RedisModule_CreateStringFromString(ctx, argv[3]);
    if (version_p) {
        v[vlen++] = RedisModule_CreateString(ctx, "ABS", 3);
        v[vlen++] = RedisModule_CreateStringFromLongLong(ctx, tair_hash_val->version);
    }
    if (expire_p) {
        v[vlen++] = RedisModule_CreateString(ctx, "PXAT", 4);
        v[vlen++] = RedisModule_CreateStringFromLongLong(ctx, tair_hash_val->expire);
    }
    RedisModule_Replicate(ctx, "EXHSET", "v", v, vlen);
    RedisModule_Free(v);
    return REDISMODULE_OK;
}

/* EXHSETNX <key> <field> <value> */
int TairHashTypeHsetNx_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }
    if (!fieldArgsFit(ctx, argv, 2, 3, 2, 1)) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, RedisModule_GetSelectedDb(ctx), argv[1]);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    RedisModuleString *pkey = argv[1], *skey = argv[2], *svalue = argv[3];

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        tair_hash_obj = createTairHashTypeObject();
        tair_hash_obj->key = RedisModule_CreateStringFromString(NULL, pkey);
        RedisModule_ModuleTypeSetValue(key, TairHashType, tair_hash_obj);
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    /* An expired field is absent, even before expiry has removed it. */
    fieldExpireIfNeeded(ctx, RedisModule_GetSelectedDb(ctx), pkey, tair_hash_obj, skey, 0);
    if (fieldLookup(tair_hash_obj, skey) != NULL) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    fieldInsert(tair_hash_obj, skey, svalue);

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}

/* EXHMSET key field value [field value …] */
int TairHashTypeHmset_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4 || (argc % 2) == 1) {
        return RedisModule_WrongArity(ctx);
    }
    if (!fieldArgsFit(ctx, argv, 2, argc, 2, 1)) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, RedisModule_GetSelectedDb(ctx), argv[1]);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        tair_hash_obj = createTairHashTypeObject();
        tair_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        RedisModule_ModuleTypeSetValue(key, TairHashType, tair_hash_obj);
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    /* Each pair increments the version of its field, a repeated field once per pair: reject
     * the write before changing anything if that could take a version past LLONG_MAX. */
    long long pairs = (argc - 2) / 2;
    for (int i = 2; i < argc; i += 2) {
        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[i]);
        if (tair_hash_val != NULL && tair_hash_val->version > LLONG_MAX - pairs) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION_OVERFLOW);
            return REDISMODULE_ERR;
        }
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    for (int i = 2; i < argc; i += 2) {
        fieldExpireIfNeeded(ctx, dbid, argv[1], tair_hash_obj, argv[i], 0);
        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[i]);
        if (tair_hash_val == NULL) {
            tair_hash_val = fieldInsert(tair_hash_obj, argv[i], argv[i + 1]);
        } else {
            tair_hash_val = fieldSetValue(tair_hash_obj, tair_hash_val, argv[i + 1]);
        }
        tair_hash_val->version++;
    }

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* EXHMSETWITHOPTS tairHashkey field1 val1 ver1 expire1 [field2 val2 ver2 expire2 ...] */
int TairHashTypeHmsetWithOpts_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 6 || ((argc - 2) % 4) != 0) {
        return RedisModule_WrongArity(ctx);
    }
    if (!fieldArgsFit(ctx, argv, 2, argc, 4, 1)) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, RedisModule_GetSelectedDb(ctx), argv[1]);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    /* Created only once every field passed validation. */
    tairHashObj *tair_hash_obj = type == REDISMODULE_KEYTYPE_EMPTY ? NULL : RedisModule_ModuleTypeGetValue(key);

    long long ver;
    long long when;
    long long now = RedisModule_Milliseconds();
    long long fields = (argc - 2) / 4;

    int nokey;
    int dbid = RedisModule_GetSelectedDb(ctx);
    for (int i = 2; i < argc; i += 4) {
        if (RedisModule_StringToLongLong(argv[i + 3], &when) != REDISMODULE_OK || RedisModule_StringToLongLong(argv[i + 2], &ver) != REDISMODULE_OK ||
            ver < 0 || when < 0) {
            abortWrite(ctx, key, argv[1], tair_hash_obj);
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
        if (expireToAbsoluteMs(when, 1, now, &when) != REDISMODULE_OK) {
            abortWrite(ctx, key, argv[1], tair_hash_obj);
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INVALID_EXPIRE);
            return REDISMODULE_ERR;
        }

        if (tair_hash_obj == NULL) {
            continue;
        }
        fieldExpireIfNeeded(ctx, dbid, argv[1], tair_hash_obj, argv[i], 0);
        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[i]);
        if (tair_hash_val != NULL && ver != 0 && tair_hash_val->version != ver) {
            abortWrite(ctx, key, argv[1], tair_hash_obj);
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }
        /* Each entry increments the version of its field, a repeated field once per entry. */
        if (tair_hash_val != NULL && tair_hash_val->version > LLONG_MAX - fields) {
            abortWrite(ctx, key, argv[1], tair_hash_obj);
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION_OVERFLOW);
            return REDISMODULE_ERR;
        }
    }

    if (tair_hash_obj == NULL) {
        tair_hash_obj = createTairHashTypeObject();
        tair_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        RedisModule_ModuleTypeSetValue(key, TairHashType, tair_hash_obj);
    }

    size_t vlen = 0;
    RedisModuleString **v = RedisModule_Alloc(sizeof(RedisModuleString *) * 7);

    for (int i = 2; i < argc; i += 4) {
        if (RedisModule_StringToLongLong(argv[i + 3], &when) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[i]);
        if (tair_hash_val == NULL) {
            tair_hash_val = fieldInsert(tair_hash_obj, argv[i], argv[i + 1]);
            nokey = 1;
        } else {
            tair_hash_val = fieldSetValue(tair_hash_obj, tair_hash_val, argv[i + 1]);
            nokey = 0;
        }
        tair_hash_val->version++;

        /* Cannot overflow: the loop above checked it. */
        expireToAbsoluteMs(when, 1, now, &when);
        if (nokey || tair_hash_val->expire == 0) {
            g_expire_algorithm.insert(ctx, dbid, argv[1], tair_hash_obj, argv[i], when);
        } else {
            g_expire_algorithm.update(ctx, dbid, argv[1], tair_hash_obj, argv[i], tair_hash_val->expire, when);
        }
        ftSetExpire(&tair_hash_obj->hash, tair_hash_val, when);

        v[vlen++] = RedisModule_CreateStringFromString(ctx, argv[1]);
        v[vlen++] = RedisModule_CreateStringFromString(ctx, argv[i]);
        v[vlen++] = RedisModule_CreateStringFromString(ctx, argv[i + 1]);
        v[vlen++] = RedisModule_CreateString(ctx, "ABS", 3);
        v[vlen++] = RedisModule_CreateStringFromLongLong(ctx, tair_hash_val->version);
        v[vlen++] = RedisModule_CreateString(ctx, "PXAT", 4);
        v[vlen++] = RedisModule_CreateStringFromLongLong(ctx, tair_hash_val->expire);
        RedisModule_Replicate(ctx, "EXHSET", "v", v, vlen);
        vlen = 0;
    }

    RedisModule_Free(v);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/*  EXHPEXPIREAT <key> <field> <milliseconds-timestamp> [ VER version | ABS version | GT version ]*/
int TairHashTypeHpexpireAt_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return tairHashExpireGenericFunc(ctx, argv, argc, 0, UNIT_MILLISECONDS);
}

/*  EXHPEXPIRE <key> <field> <milliseconds> [ VER version | ABS version | GT version ]*/
int TairHashTypeHpexpire_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return tairHashExpireGenericFunc(ctx, argv, argc, RedisModule_Milliseconds(), UNIT_MILLISECONDS);
}

/*  EXHEXPIREAT <key> <field> <timestamp> [ VER version | ABS version | GT version ] */
int TairHashTypeHexpireAt_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return tairHashExpireGenericFunc(ctx, argv, argc, 0, UNIT_SECONDS);
}

/*  EXHEXPIRE <key> <field> <seconds> [ VER version | ABS version | GT version ] */
int TairHashTypeHexpire_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return tairHashExpireGenericFunc(ctx, argv, argc, RedisModule_Milliseconds(), UNIT_SECONDS);
}

/*  EXHPTTL <key> <field> */
int TairHashTypeHpttl_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return tairHashTTLGenericFunc(ctx, argv, argc, UNIT_MILLISECONDS);
}

/*  EXHTTL <key> <field> */
int TairHashTypeHttl_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return tairHashTTLGenericFunc(ctx, argv, argc, UNIT_SECONDS);
}

/* EXHPERSIST <key> <field> */
int TairHashTypeHpersist_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    if (fieldExpireIfNeeded(ctx, dbid, argv[1], tair_hash_obj, argv[2], 0)) {
        delEmptyTairHashIfNeeded(ctx, key, argv[1], tair_hash_obj);
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[2]);
    if (tair_hash_val == NULL) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    if (!tair_hash_val->expire) {
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        int dbid = RedisModule_GetSelectedDb(ctx);
        g_expire_algorithm.delete(ctx, dbid, argv[1], tair_hash_obj, argv[2], tair_hash_val->expire);
        ftSetExpire(&tair_hash_obj->hash, tair_hash_val, 0);
        RedisModule_ReplyWithLongLong(ctx, 1);
    }

    return REDISMODULE_OK;
}

/*  EXHVER <key> <field> */
int TairHashTypeHver_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    int field_expired = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    if (readExpireIfNeeded(ctx, &expired, dbid, argv[1], tair_hash_obj, argv[2])) {
        field_expired = 1;
    }

    TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[2]);
    if (field_expired || tair_hash_val == NULL) {
        RedisModule_ReplyWithLongLong(ctx, -2);
    } else {
        RedisModule_ReplyWithLongLong(ctx, tair_hash_val->version);
    }

    finishRead(ctx, key, argv[1], tair_hash_obj, expired);
    return REDISMODULE_OK;
}

/*  EXHSETVER <key> <field> <version> */
int TairHashTypeHsetVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    long long version;

    if (RedisModule_StringToLongLong(argv[3], &version) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    /* version must > 0 */
    if (version <= 0) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, RedisModule_GetSelectedDb(ctx), argv[1]);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[2]);
    if (tair_hash_val == NULL) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    if (fieldExpireIfNeeded(ctx, dbid, argv[1], tair_hash_obj, argv[2], 0)) {
        delEmptyTairHashIfNeeded(ctx, key, argv[1], tair_hash_obj);
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    tair_hash_val->version = version;
    RedisModule_ReplyWithLongLong(ctx, 1);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* EXHINCRBY <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [VER version | ABS version | GT version] [MIN minval] [MAX maxval] [KEEPTTL] */
int TairHashTypeHincrBy_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    if (!fieldArgsFit(ctx, argv, 2, 3, 1, 0)) {
        return REDISMODULE_ERR;
    }

    long long milliseconds = 0, expire = 0, incr = 0, version = 0, min = 0, max = 0;
    RedisModuleString *expire_p = NULL;
    RedisModuleString *version_p = NULL;
    RedisModuleString *min_p = NULL, *max_p = NULL;
    int ex_flags = TAIR_HASH_SET_NO_FLAGS;
    int nokey;

    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, RedisModule_GetSelectedDb(ctx), argv[1]);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    if (RedisModule_StringToLongLong(argv[3], &incr) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_NOT_INTEGER);
        return REDISMODULE_ERR;
    }

    for (int j = 4; j < argc; j++) {
        RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];
        if (!mstrcasecmp(argv[j], "ex") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_EX;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "exat") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_EX;
            ex_flags |= TAIR_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "px") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_PX;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "pxat") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_PX;
            ex_flags |= TAIR_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "ver") && !(ex_flags & TAIR_HASH_SET_WITH_ABS_VER) && !(ex_flags & TAIR_HASH_SET_WITH_GT_VER) && next) {
            ex_flags |= TAIR_HASH_SET_WITH_VER;
            version_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "abs") && !(ex_flags & TAIR_HASH_SET_WITH_VER) && !(ex_flags & TAIR_HASH_SET_WITH_GT_VER) && next) {
            ex_flags |= TAIR_HASH_SET_WITH_ABS_VER;
            version_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "gt") && !(ex_flags & TAIR_HASH_SET_WITH_VER) && !(ex_flags & TAIR_HASH_SET_WITH_ABS_VER) && next) {
            ex_flags |= TAIR_HASH_SET_WITH_GT_VER;
            version_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "min") && next) {
            ex_flags |= TAIR_HASH_SET_WITH_BOUNDARY;
            min_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "max") && next) {
            ex_flags |= TAIR_HASH_SET_WITH_BOUNDARY;
            max_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "keepttl") && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_PX)) {
            ex_flags |= TAIR_HASH_SET_KEEPTTL;
        } else {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (expire_p && expire < 0) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    /* Computed before anything changes, so that an expire that does not fit is rejected cleanly. */
    if (expire_p && expire > 0 &&
        expireToAbsoluteMs(expire, ex_flags & TAIR_HASH_SET_EX, (ex_flags & TAIR_HASH_SET_ABS_EXPIRE) ? 0 : RedisModule_Milliseconds(),
                           &milliseconds) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INVALID_EXPIRE);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0 || ((ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) && version == 0)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != min_p) && (RedisModule_StringToLongLong(min_p, &min))) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INT_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != max_p) && (RedisModule_StringToLongLong(max_p, &max))) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INT_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if (NULL != min_p && NULL != max_p && max < min) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    RedisModuleString *pkey = argv[1], *skey = argv[2];

    /* Created only once the increment is known to apply. */
    tairHashObj *tair_hash_obj = type == REDISMODULE_KEYTYPE_EMPTY ? NULL : RedisModule_ModuleTypeGetValue(key);

    int dbid = RedisModule_GetSelectedDb(ctx);
    TairHashField *tair_hash_val = NULL;
    if (tair_hash_obj) {
        fieldExpireIfNeeded(ctx, dbid, argv[1], tair_hash_obj, argv[2], 0);
        tair_hash_val = fieldLookup(tair_hash_obj, skey);
    }
    nokey = tair_hash_val == NULL;

    long long cur_val = 0;
    if (!nokey) {
        if (!m_string2ll(fieldValue(tair_hash_val), tair_hash_val->value_len, &cur_val)) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_NOT_INTEGER);
            return REDISMODULE_ERR;
        }

        /* Version equals 0 means no version checking */
        if (ex_flags & TAIR_HASH_SET_WITH_VER) {
            if (version != 0 && version != tair_hash_val->version) {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
                return REDISMODULE_ERR;
            }
        } else if (ex_flags & TAIR_HASH_SET_WITH_GT_VER) {
            if (version <= tair_hash_val->version) {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
                return REDISMODULE_ERR;
            }
        }
        if (!(ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) && tair_hash_val->version == LLONG_MAX) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION_OVERFLOW);
            return REDISMODULE_ERR;
        }
    }

    if ((incr < 0 && cur_val < 0 && incr < (LLONG_MIN - cur_val)) || (incr > 0 && cur_val > 0 && incr > (LLONG_MAX - cur_val)) || (max_p != NULL && cur_val + incr > max) || (min_p != NULL && cur_val + incr < min)) {
        abortWrite(ctx, key, argv[1], tair_hash_obj);
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_OVERFLOW);
        return REDISMODULE_ERR;
    }

    if (tair_hash_obj == NULL) {
        tair_hash_obj = createTairHashTypeObject();
        tair_hash_obj->key = RedisModule_CreateStringFromString(NULL, pkey);
        RedisModule_ModuleTypeSetValue(key, TairHashType, tair_hash_obj);
    }

    cur_val += incr;
    RedisModuleString *value = RedisModule_CreateStringFromLongLong(ctx, cur_val);
    if (nokey) {
        tair_hash_val = fieldInsert(tair_hash_obj, skey, value);
    } else {
        tair_hash_val = fieldSetValue(tair_hash_obj, tair_hash_val, value);
    }

    if (ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) {
        tair_hash_val->version = version;
    } else {
        tair_hash_val->version += 1;
    }

    if (expire_p && expire == 0) {
        milliseconds = 1;
    }

    if (milliseconds == 0 && !(ex_flags & TAIR_HASH_SET_KEEPTTL)) {
        g_expire_algorithm.delete(ctx, dbid, argv[1], tair_hash_obj, skey, tair_hash_val->expire);
        ftSetExpire(&tair_hash_obj->hash, tair_hash_val, 0);
    }

    if (milliseconds > 0) {
        if (nokey || tair_hash_val->expire == 0) {
            g_expire_algorithm.insert(ctx, dbid, argv[1], tair_hash_obj, skey, milliseconds);
        } else {
            g_expire_algorithm.update(ctx, dbid, argv[1], tair_hash_obj, skey, tair_hash_val->expire, milliseconds);
        }
        ftSetExpire(&tair_hash_obj->hash, tair_hash_val, milliseconds);
    }

    if (milliseconds > 0) {
        RedisModule_Replicate(ctx, "EXHSET", "sslclcl", argv[1], argv[2], cur_val, "abs",
                              tair_hash_val->version, "pxat", (milliseconds + RedisModule_Milliseconds()));
    } else {
        RedisModule_Replicate(ctx, "EXHSET", "sslcl", argv[1], argv[2], cur_val, "abs", tair_hash_val->version);
    }
    RedisModule_ReplyWithLongLong(ctx, cur_val);
    return REDISMODULE_OK;
}

/* EXHINCRBYFLOAT <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [VER version | ABS version | GT version] [MIN
 * minval] [MAX maxval] [KEEPTTL] */
int TairHashTypeHincrByFloat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }
    if (!fieldArgsFit(ctx, argv, 2, 3, 1, 0)) {
        return REDISMODULE_ERR;
    }

    long long milliseconds = 0, expire = 0, version = 0;
    long double incr = 0, min = 0, max = 0;
    RedisModuleString *expire_p = NULL;
    RedisModuleString *version_p = NULL;
    RedisModuleString *min_p = NULL, *max_p = NULL;
    int ex_flags = TAIR_HASH_SET_NO_FLAGS;
    int nokey = 0;

    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, RedisModule_GetSelectedDb(ctx), argv[1]);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    if (mstring2ld(argv[3], &incr) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_NOT_FLOAT);
        return REDISMODULE_ERR;
    }

    for (int j = 4; j < argc; j++) {
        RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];

        if (!mstrcasecmp(argv[j], "ex") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_EX;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "exat") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_EX;
            ex_flags |= TAIR_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "px") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_PX;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "pxat") && !(ex_flags & TAIR_HASH_SET_PX) && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_KEEPTTL) && next) {
            ex_flags |= TAIR_HASH_SET_PX;
            ex_flags |= TAIR_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "ver") && !(ex_flags & TAIR_HASH_SET_WITH_ABS_VER) && !(ex_flags & TAIR_HASH_SET_WITH_GT_VER) && next) {
            ex_flags |= TAIR_HASH_SET_WITH_VER;
            version_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "abs") && !(ex_flags & TAIR_HASH_SET_WITH_VER) && !(ex_flags & TAIR_HASH_SET_WITH_GT_VER) && next) {
            ex_flags |= TAIR_HASH_SET_WITH_ABS_VER;
            version_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "gt") && !(ex_flags & TAIR_HASH_SET_WITH_VER) && !(ex_flags & TAIR_HASH_SET_WITH_ABS_VER) && next) {
            ex_flags |= TAIR_HASH_SET_WITH_GT_VER;
            version_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "min") && next) {
            ex_flags |= TAIR_HASH_SET_WITH_BOUNDARY;
            min_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "max") && next) {
            ex_flags |= TAIR_HASH_SET_WITH_BOUNDARY;
            max_p = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "keepttl") && !(ex_flags & TAIR_HASH_SET_EX) && !(ex_flags & TAIR_HASH_SET_PX)) {
            ex_flags |= TAIR_HASH_SET_KEEPTTL;
        } else {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (expire_p && expire < 0) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    /* Computed before anything changes, so that an expire that does not fit is rejected cleanly. */
    if (expire_p && expire > 0 &&
        expireToAbsoluteMs(expire, ex_flags & TAIR_HASH_SET_EX, (ex_flags & TAIR_HASH_SET_ABS_EXPIRE) ? 0 : RedisModule_Milliseconds(),
                           &milliseconds) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INVALID_EXPIRE);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0 || ((ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) && version == 0)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != min_p) && (mstring2ld(min_p, &min) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_FLOAT_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != max_p) && (mstring2ld(max_p, &max) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_FLOAT_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if (NULL != min_p && NULL != max_p && max < min) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    /* Created only once the increment is known to apply. */
    tairHashObj *tair_hash_obj = type == REDISMODULE_KEYTYPE_EMPTY ? NULL : RedisModule_ModuleTypeGetValue(key);

    RedisModuleString *skey = argv[2];
    int dbid = RedisModule_GetSelectedDb(ctx);
    TairHashField *tair_hash_val = NULL;
    if (tair_hash_obj) {
        fieldExpireIfNeeded(ctx, dbid, argv[1], tair_hash_obj, argv[2], 0);
        tair_hash_val = fieldLookup(tair_hash_obj, skey);
    }
    nokey = tair_hash_val == NULL;

    long double cur_val = 0;
    if (!nokey) {
        if (!m_string2ld(fieldValue(tair_hash_val), tair_hash_val->value_len, &cur_val)) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_NOT_FLOAT);
            return REDISMODULE_ERR;
        }

        /* Version equals 0 means no version checking */
        if (ex_flags & TAIR_HASH_SET_WITH_VER) {
            if (version != 0 && version != tair_hash_val->version) {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
                return REDISMODULE_ERR;
            }
        } else if (ex_flags & TAIR_HASH_SET_WITH_GT_VER) {
            if (version <= tair_hash_val->version) {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
                return REDISMODULE_ERR;
            }
        }
        if (!(ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) && tair_hash_val->version == LLONG_MAX) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION_OVERFLOW);
            return REDISMODULE_ERR;
        }
    }

    if (isnan(cur_val + incr) || isinf(cur_val + incr) || (max_p != NULL && cur_val + incr > max) || (min_p != NULL && cur_val + incr < min)) {
        abortWrite(ctx, key, argv[1], tair_hash_obj);
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_OVERFLOW);
        return REDISMODULE_ERR;
    }

    if (tair_hash_obj == NULL) {
        tair_hash_obj = createTairHashTypeObject();
        tair_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        RedisModule_ModuleTypeSetValue(key, TairHashType, tair_hash_obj);
    }

    cur_val += incr;

    char dbuf[MAX_LONG_DOUBLE_CHARS] = {0};
    int dlen = m_ld2string(dbuf, sizeof(dbuf), cur_val, 1);
    RedisModuleString *value = RedisModule_CreateString(ctx, dbuf, dlen);
    if (nokey) {
        tair_hash_val = fieldInsert(tair_hash_obj, skey, value);
    } else {
        tair_hash_val = fieldSetValue(tair_hash_obj, tair_hash_val, value);
    }

    if (ex_flags & (TAIR_HASH_SET_WITH_ABS_VER | TAIR_HASH_SET_WITH_GT_VER)) {
        tair_hash_val->version = version;
    } else {
        tair_hash_val->version += 1;
    }

    if (expire_p && expire == 0) {
        milliseconds = 1;
    }

    if (milliseconds == 0 && !(ex_flags & TAIR_HASH_SET_KEEPTTL)) {
        g_expire_algorithm.delete(ctx, dbid, argv[1], tair_hash_obj, skey, tair_hash_val->expire);
        ftSetExpire(&tair_hash_obj->hash, tair_hash_val, 0);
    }

    if (milliseconds > 0) {
        if (nokey || tair_hash_val->expire == 0) {
            g_expire_algorithm.insert(ctx, dbid, argv[1], tair_hash_obj, skey, milliseconds);
        } else {
            g_expire_algorithm.update(ctx, dbid, argv[1], tair_hash_obj, skey, tair_hash_val->expire, milliseconds);
        }
        ftSetExpire(&tair_hash_obj->hash, tair_hash_val, milliseconds);
    }

    if (milliseconds > 0) {
        RedisModule_Replicate(ctx, "EXHSET", "ssbclcl", argv[1], argv[2], dbuf, (size_t)dlen, "abs", tair_hash_val->version, "pxat",
                              (milliseconds + RedisModule_Milliseconds()));
    } else {
        RedisModule_Replicate(ctx, "EXHSET", "ssbcl", argv[1], argv[2], dbuf, (size_t)dlen, "abs", tair_hash_val->version);
    }
    RedisModule_ReplyWithString(ctx, value);
    return REDISMODULE_OK;
}

/* EXHGET <key> <field> */
int TairHashTypeHget_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    RedisModuleString *pkey = argv[1], *skey = argv[2];

    int field_expire = 0;
    int dbid = RedisModule_GetSelectedDb(ctx);
    if (readExpireIfNeeded(ctx, &expired, dbid, argv[1], tair_hash_obj, argv[2])) {
        field_expire = 1;
    }

    TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, skey);
    if (field_expire || tair_hash_val == NULL) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        replyWithFieldValue(ctx, tair_hash_val);
    }

    finishRead(ctx, key, pkey, tair_hash_obj, expired);
    return REDISMODULE_OK;
}

/* EXHGETWITHVER <key> <field> */
int TairHashTypeHgetWithVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int field_expired = 0;
    int dbid = RedisModule_GetSelectedDb(ctx);
    if (readExpireIfNeeded(ctx, &expired, dbid, argv[1], tair_hash_obj, argv[2])) {
        field_expired = 1;
    }

    TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[2]);
    if (field_expired || tair_hash_val == NULL) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        RedisModule_ReplyWithArray(ctx, 2);
        replyWithFieldValue(ctx, tair_hash_val);
        RedisModule_ReplyWithLongLong(ctx, tair_hash_val->version);
    }
    finishRead(ctx, key, argv[1], tair_hash_obj, expired);
    return REDISMODULE_OK;
}

/* EXHMGET key field [field ...] */
int TairHashTypeHmget_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, argc - 2);
        for (int ii = 2; ii < argc; ++ii) {
            RedisModule_ReplyWithNull(ctx);
        }
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    /* Exactly one element (possibly null) per requested field. */
    RedisModule_ReplyWithArray(ctx, argc - 2);
    for (int ii = 2; ii < argc; ++ii) {
        if (readExpireIfNeeded(ctx, &expired, dbid, argv[1], tair_hash_obj, argv[ii])) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }
        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[ii]);
        if (tair_hash_val == NULL) {
            RedisModule_ReplyWithNull(ctx);
        } else {
            replyWithFieldValue(ctx, tair_hash_val);
        }
    }
    finishRead(ctx, key, argv[1], tair_hash_obj, expired);
    return REDISMODULE_OK;
}

/* EXHMGETWITHVER key field [field ...] */
int TairHashTypeHmgetWithVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, argc - 2);
        for (int ii = 2; ii < argc; ++ii) {
            RedisModule_ReplyWithNull(ctx);
        }
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    /* Exactly one element (possibly null) per requested field. */
    RedisModule_ReplyWithArray(ctx, argc - 2);
    for (int ii = 2; ii < argc; ++ii) {
        if (readExpireIfNeeded(ctx, &expired, dbid, argv[1], tair_hash_obj, argv[ii])) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }
        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[ii]);
        if (tair_hash_val == NULL) {
            RedisModule_ReplyWithNull(ctx);
        } else {
            RedisModule_ReplyWithArray(ctx, 2);
            replyWithFieldValue(ctx, tair_hash_val);
            RedisModule_ReplyWithLongLong(ctx, tair_hash_val->version);
        }
    }
    finishRead(ctx, key, argv[1], tair_hash_obj, expired);

    return REDISMODULE_OK;
}

/* EXHDEL <key> <field> <field> <field> ...*/
int TairHashTypeHdel_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    long long j, deleted = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    for (j = 2; j < argc; j++) {
        /* Internal will perform RedisModule_Replicate EXHDEL for replication */
        fieldExpireIfNeeded(ctx, dbid, argv[1], tair_hash_obj, argv[j], 0);
        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[j]);
        if (tair_hash_val) {
            if (tair_hash_val->expire > 0) {
                g_expire_algorithm.delete(ctx, dbid, argv[1], tair_hash_obj, argv[j], tair_hash_val->expire);
            }
            fieldDelete(tair_hash_obj, argv[j]);

            RedisModule_Replicate(ctx, "EXHDEL", "ss", argv[1], argv[j]);
            deleted++;
        }
    }

    delEmptyTairHashIfNeeded(ctx, key, argv[1], tair_hash_obj);
    RedisModule_ReplyWithLongLong(ctx, deleted);
    return REDISMODULE_OK;
}

/* Since using `RedisModule_Replicate` directly in the timer callback will generate nested MULTIs, we have
 * to generate a new internal command and then use `RedisModule_Call` to call it in the module. It is best
 * not to use this command directly in the client. */

/* EXHDELREPL <key> <field> */
int TairHashTypeHdelRepl_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    long long deleted = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[2]);
    if (tair_hash_val) {
        /* A field with a TTL leaves the expire indexes with the hash table. */
        g_expire_algorithm.delete(ctx, dbid, argv[1], tair_hash_obj, argv[2], tair_hash_val->expire);
        fieldDelete(tair_hash_obj, argv[2]);
        RedisModule_Replicate(ctx, "EXHDEL", "ss", argv[1], argv[2]);
        deleted++;
        delEmptyTairHashIfNeeded(ctx, key, argv[1], tair_hash_obj);
    }

    RedisModule_ReplyWithLongLong(ctx, deleted);
    return REDISMODULE_OK;
}

/* EXHDELWITHVER <key> <field> version> <field> <version> ...*/
int TairHashTypeHdelWithVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4 || ((argc - 2) % 2) != 0) {
        return RedisModule_WrongArity(ctx);
    }

    long long j, deleted = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    long long ver;

    for (j = 2; j < argc; j += 2) {
        if (RedisModule_StringToLongLong(argv[j + 1], &ver) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    for (j = 2; j < argc; j += 2) {
        RedisModule_StringToLongLong(argv[j + 1], &ver);

        /* Internal will perform RedisModule_Replicate EXHDEL for replication */
        fieldExpireIfNeeded(ctx, dbid, argv[1], tair_hash_obj, argv[j], 0);

        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[j]);
        if (tair_hash_val != NULL) {
            if (ver == 0 || ver == tair_hash_val->version) {
                if (tair_hash_val->expire > 0) {
                    g_expire_algorithm.delete(ctx, dbid, argv[1], tair_hash_obj, argv[j], tair_hash_val->expire);
                }
                fieldDelete(tair_hash_obj, argv[j]);
                RedisModule_Replicate(ctx, "EXHDEL", "ss", argv[1], argv[j]);
                deleted++;
            }
        }
    }

    delEmptyTairHashIfNeeded(ctx, key, argv[1], tair_hash_obj);
    RedisModule_ReplyWithLongLong(ctx, deleted);
    return REDISMODULE_OK;
}

/* EXHLEN <key> [noexp]*/
int TairHashTypeHlen_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    int noexp = 0;
    uint64_t len = 0;

    if (argc != 2 && argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    if (argc == 3) {
        if (!mstrcasecmp(argv[2], "noexp")) {
            noexp = 1;
        } else {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    if (noexp) {
        fieldTableIterator it;
        TairHashField *f;
        long long now = RedisModule_Milliseconds();
        ftIterInitReadOnly(&it, &tair_hash_obj->hash);
        while ((f = ftIterNext(&it)) != NULL) {
            if (f->expire && now > f->expire) {
                continue;
            }
            len++;
        }
        ftIterRelease(&it);
    } else {
        len = ftSize(&tair_hash_obj->hash);
    }

    RedisModule_ReplyWithLongLong(ctx, len);
    return REDISMODULE_OK;
}

/* EXHEXISTS key field */
int TairHashTypeHexists_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int field_expired = 0;
    int dbid = RedisModule_GetSelectedDb(ctx);
    if (readExpireIfNeeded(ctx, &expired, dbid, argv[1], tair_hash_obj, argv[2])) {
        field_expired = 1;
    }

    TairHashField *tairHashval = fieldLookup(tair_hash_obj, argv[2]);
    if (field_expired || tairHashval == NULL) {
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        RedisModule_ReplyWithLongLong(ctx, 1);
    }

    finishRead(ctx, key, argv[1], tair_hash_obj, expired);
    return REDISMODULE_OK;
}

/* EXHSTRLEN key field */
int TairHashTypeHstrlen_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int field_expired = 0;
    int dbid = RedisModule_GetSelectedDb(ctx);
    if (readExpireIfNeeded(ctx, &expired, dbid, argv[1], tair_hash_obj, argv[2])) {
        field_expired = 1;
    }
    TairHashField *val = fieldLookup(tair_hash_obj, argv[2]);
    if (field_expired || !val) {
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        RedisModule_ReplyWithLongLong(ctx, val->value_len);
    }

    finishRead(ctx, key, argv[1], tair_hash_obj, expired);
    return REDISMODULE_OK;
}

/* EXHKEYS key */
int TairHashTypeHkeys_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithArray(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    TairHashField *data;
    uint64_t cn = 0;

    fieldTableIterator it;

    int dbid = RedisModule_GetSelectedDb(ctx);
    /* Without expiring fields nothing is filtered out, so the length is known up front
     * and the reply avoids a deferred length (an extra reply-list node per read). Once
     * that length is sent every field follows: none is checked against its expire. */
    int exact = ftExpiring(&tair_hash_obj->hash) == 0;
    RedisModule_ReplyWithArray(ctx, exact ? (long)ftSize(&tair_hash_obj->hash) : REDISMODULE_POSTPONED_ARRAY_LEN);
    long long now = RedisModule_Milliseconds();
    fullReadIterInit(&it, tair_hash_obj);
    while ((data = ftIterNext(&it)) != NULL) {
        if (!exact && fullReadSkips(ctx, &expired, dbid, argv[1], tair_hash_obj, data, now)) {
            continue;
        }
        replyWithFieldName(ctx, data);
        cn++;
    }
    ftIterRelease(&it);

    finishRead(ctx, key, argv[1], tair_hash_obj, expired);
    if (!exact) {
        RedisModule_ReplySetArrayLength(ctx, cn);
    }
    return REDISMODULE_OK;
}

/* EXHVALS key */
int TairHashTypeHvals_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithArray(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    TairHashField *data;
    uint64_t cn = 0;

    fieldTableIterator it;

    int dbid = RedisModule_GetSelectedDb(ctx);
    /* Without expiring fields nothing is filtered out, so the length is known up front
     * and the reply avoids a deferred length (an extra reply-list node per read). Once
     * that length is sent every field follows: none is checked against its expire. */
    int exact = ftExpiring(&tair_hash_obj->hash) == 0;
    RedisModule_ReplyWithArray(ctx, exact ? (long)ftSize(&tair_hash_obj->hash) : REDISMODULE_POSTPONED_ARRAY_LEN);
    long long now = RedisModule_Milliseconds();
    fullReadIterInit(&it, tair_hash_obj);
    while ((data = ftIterNext(&it)) != NULL) {
        if (!exact && fullReadSkips(ctx, &expired, dbid, argv[1], tair_hash_obj, data, now)) {
            continue;
        }
        replyWithFieldValue(ctx, data);
        cn++;
    }
    ftIterRelease(&it);

    finishRead(ctx, key, argv[1], tair_hash_obj, expired);
    if (!exact) {
        RedisModule_ReplySetArrayLength(ctx, cn);
    }
    return REDISMODULE_OK;
}

int tairHashGetAllGenericFunc(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int returnVer) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    TairHashField *data;
    uint64_t cn = 0;

    fieldTableIterator it;

    int dbid = RedisModule_GetSelectedDb(ctx);
    /* Without expiring fields nothing is filtered out, so the length is known up front
     * and the reply avoids a deferred length (an extra reply-list node per read). Once
     * that length is sent every field follows: none is checked against its expire. */
    int exact = ftExpiring(&tair_hash_obj->hash) == 0;
    RedisModule_ReplyWithArray(ctx, exact ? (long)ftSize(&tair_hash_obj->hash) * (returnVer > 0 ? 3 : 2) : REDISMODULE_POSTPONED_ARRAY_LEN);
    long long now = RedisModule_Milliseconds();
    fullReadIterInit(&it, tair_hash_obj);
    while ((data = ftIterNext(&it)) != NULL) {
        if (!exact && fullReadSkips(ctx, &expired, dbid, argv[1], tair_hash_obj, data, now)) {
            continue;
        }
        replyWithFieldName(ctx, data);
        cn++;
        replyWithFieldValue(ctx, data);
        cn++;
        if (returnVer > 0) {
            RedisModule_ReplyWithLongLong(ctx, data->version);
            cn++;
        }
    }
    ftIterRelease(&it);

    finishRead(ctx, key, argv[1], tair_hash_obj, expired);
    if (!exact) {
        RedisModule_ReplySetArrayLength(ctx, cn);
    }
    return REDISMODULE_OK;
}

/* EXHGETALL key */
int TairHashTypeHgetAll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return tairHashGetAllGenericFunc(ctx, argv, argc, 0);
}

/* EXHGETALLWITHVER key */
int TairHashTypeHgetAllWithVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return tairHashGetAllGenericFunc(ctx, argv, argc, 1);
}

static int parseScanCursor(RedisModuleString *cs, unsigned long *cursor) {
    char *eptr;

    /* Use strtoul() because we need an *unsigned* long, so
     * getLongLongFromObject() does not cover the whole cursor space. */
    errno = 0;
    size_t cs_len;
    const char *ptr = RedisModule_StringPtrLen(cs, &cs_len);
    *cursor = strtoul(ptr, &eptr, 10);
    if (isspace(ptr[0]) || eptr[0] != '\0' || errno == ERANGE) {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

/* EXHSCAN key cursor [MATCH pattern] [COUNT count]*/
int TairHashTypeHscan_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3 || argc > 7) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    unsigned long cursor;
    if (parseScanCursor(argv[2], &cursor) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    /* Step 1: Parse options. */
    RedisModuleString *pattern = NULL;
    long long count = TAIR_HASH_SCAN_DEFAULT_COUNT;
    for (int j = 3; j < argc; j++) {
        RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];
        if (!mstrcasecmp(argv[j], "MATCH") && next) {
            pattern = next;
            j++;
        } else if (!mstrcasecmp(argv[j], "COUNT") && next) {
            if (RedisModule_StringToLongLong(next, &count) == REDISMODULE_ERR) {
                RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
                return REDISMODULE_ERR;
            }
            j++;
        } else {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    /* The matcher takes its lengths as ints. */
    size_t pattern_len = 0;
    const char *pattern_ptr = pattern ? RedisModule_StringPtrLen(pattern, &pattern_len) : NULL;
    if (pattern_len > INT_MAX) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    tairHashObj *tair_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithSimpleString(ctx, "0");
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        tair_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (tair_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    /* Step 2: Iterate the collection.*/
    long maxiterations = count * 10;
    list *keys = m_listCreate();

    /* COUNT bounds the returned elements, a field and its value being two. */
    do {
        cursor = ftScan(&tair_hash_obj->hash, cursor, tairhashScanCallback, keys);
    } while (cursor && maxiterations-- && 2 * listLength(keys) < (unsigned long)count);

    m_listNode *node, *nextnode;
    node = listFirst(keys);

    int dbid = RedisModule_GetSelectedDb(ctx);
    long long now = RedisModule_Milliseconds();
    /* Step 3: Filter elements. An expired field is deleted here, so its list node must
     * not be dereferenced afterwards. */
    while (node) {
        TairHashField *f = listNodeValue(node);
        nextnode = listNextNode(node);
        int filter = 0;

        /* Filter element if it does not match the pattern. */
        if (pattern_ptr && !m_stringmatchlen(pattern_ptr, pattern_len, fieldName(f), f->name_len, 0)) {
            filter = 1;
        }

        /* Filter element if it is an expired key. */
        if (!filter && f->expire && f->expire <= now && readExpireIfNeeded(ctx, &expired, dbid, argv[1], tair_hash_obj, fieldNameString(ctx, f))) {
            filter = 1;
        }

        if (filter) {
            m_listDelNode(keys, node);
        }
        node = nextnode;
    }

    /* Step 4: Reply to the client. */
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithString(ctx, RedisModule_CreateStringFromLongLong(ctx, cursor));

    RedisModule_ReplyWithArray(ctx, 2 * listLength(keys));
    while ((node = listFirst(keys)) != NULL) {
        TairHashField *f = listNodeValue(node);
        replyWithFieldName(ctx, f);
        replyWithFieldValue(ctx, f);
        m_listDelNode(keys, node);
    }

    m_listRelease(keys);
    finishRead(ctx, key, argv[1], tair_hash_obj, expired);
    return REDISMODULE_OK;
}

/* exhexpireinfo */
int TairHashTypeActiveExpireInfo_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);

    RedisModule_AutoMemory(ctx);

    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *info_a = RedisModule_CreateStringPrintf(
        ctx,
        "\r\n"
        "# Active expire statistics\r\n"
        "tair_hash_active_expire_mode:%s\r\n"
        "tair_hash_expiring_keys:%llu\r\n"
        "enable_active_expire:%ld\r\n"
        "tair_hash_active_expire_period:%ld\r\n"
        "tair_hash_active_expire_keys_per_loop:%ld\r\n"
        "tair_hash_active_expire_dbs_per_loop:%ld\r\n"
        "tair_hash_active_expire_last_time_msec:%ld\r\n"
        "tair_hash_active_expire_max_time_msec:%ld\r\n"
        "tair_hash_active_expire_avg_time_msec:%ld\r\n"
        "tair_hash_passive_expire_keys_per_loop:%ld\r\n",
        TAIRHASH_EXPIRE_MODE_NAME,
        expiringKeyCount(),
        (long)g_expire_algorithm.enable_active_expire,
        (long)g_expire_algorithm.active_expire_period,
        (long)g_expire_algorithm.keys_per_active_loop,
        (long)g_expire_algorithm.dbs_per_active_loop,
        (long)g_expire_algorithm.stat_last_active_expire_time_msec,
        (long) g_expire_algorithm.stat_max_active_expire_time_msec,
        (long)g_expire_algorithm.stat_avg_active_expire_time_msec,
        (long)g_expire_algorithm.keys_per_passive_loop);

    size_t d_len;

#define DB_DETAIL "\r\n# DB detail statistics\r\n"

    /* The reply has a line per database and no bound on the number of databases, so it grows as needed. */
    RedisModule_StringAppendBuffer(ctx, info_a, DB_DETAIL, strlen(DB_DETAIL));

    for (int i = 0; i < g_expire_algorithm.db_num; ++i) {
        if (g_expire_algorithm.stat_active_expired_field[i] == 0 && g_expire_algorithm.stat_passive_expired_field[i] == 0) {
            continue;
        }
        RedisModuleString *info_d = RedisModule_CreateStringPrintf(ctx, "db: %d, active_expired_fields: %ld, passive_expired_fields: %ld\r\n", i,
                                                                   (long)g_expire_algorithm.stat_active_expired_field[i], (long)g_expire_algorithm.stat_passive_expired_field[i]);
        const char *d_buf = RedisModule_StringPtrLen(info_d, &d_len);
        RedisModule_StringAppendBuffer(ctx, info_a, d_buf, d_len);
        RedisModule_FreeString(ctx, info_d);
    }
    RedisModule_ReplyWithString(ctx, info_a);
    RedisModule_FreeString(ctx, info_a);
    return REDISMODULE_OK;
}

/* ========================== "tairhashtype" type methods ======================= */

void *TairHashTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    REDISMODULE_NOT_USED(encver);

    tairHashObj *o = createTairHashTypeObject();
    uint64_t len = RedisModule_LoadUnsigned(rdb);
    /* The payload holds the name the key had when it was saved. RESTORE can load it under
     * another name, and the db expire index must track the name the key has now. */
    RedisModuleString *saved_key = RedisModule_LoadString(rdb);
    const RedisModuleString *loading_key = RedisModule_GetKeyNameFromIO ? RedisModule_GetKeyNameFromIO(rdb) : NULL;
    if (loading_key) {
        o->key = RedisModule_CreateStringFromString(NULL, loading_key);
        RedisModule_FreeString(NULL, saved_key);
    } else {
        o->key = saved_key;
    }
    /* The count sizes the table, up to a bound: a corrupt count must not make it allocate
     * without limit before the first field is read. */
    if (len) {
        ftExpand(&o->hash, len < TAIRHASH_RDB_PRESIZE_MAX ? len : TAIRHASH_RDB_PRESIZE_MAX);
    }

    int dbid = RedisModule_GetDbIdFromIO ? RedisModule_GetDbIdFromIO(rdb) : 0;

    while (len--) {
        size_t name_len, value_len;
        char *name = RedisModule_LoadStringBuffer(rdb, &name_len);
        long long version = RedisModule_LoadUnsigned(rdb);
        long long expire = RedisModule_LoadUnsigned(rdb);
        char *value = RedisModule_LoadStringBuffer(rdb, &value_len);
        if (name_len > FT_NAME_MAX_LEN || value_len > FT_VALUE_MAX_LEN || ftFind(&o->hash, name, name_len) != NULL) {
            RedisModule_LogIOError(rdb, "warning", "tairhash: a field is repeated or too long, the value is corrupt");
            RedisModule_Free(name);
            RedisModule_Free(value);
            tairHashTypeReleaseObject(o);
            return NULL;
        }
        /* Earlier releases could store a negative expire. It is a time long past, as is 1, which a
         * TTL of 0 stores, and the expire indexes count on an expire being positive. */
        if (expire < 0) {
            expire = 1;
        }
        TairHashField *f = fieldCreate(name, name_len, value, value_len);
        f->version = version;
        f->expire = expire;
        ftAdd(&o->hash, f);
        if (expire) {
            RedisModuleString *field = RedisModule_CreateString(NULL, name, name_len);
            g_expire_algorithm.insert(NULL, dbid, NULL, o, field, expire);
            RedisModule_FreeString(NULL, field);
        }
        RedisModule_Free(name);
        RedisModule_Free(value);
    }

    return o;
}

void TairHashTypeRdbSave(RedisModuleIO *rdb, void *value) {
    tairHashObj *o = (tairHashObj *)value;

    RedisModule_SaveUnsigned(rdb, ftSize(&o->hash));
    RedisModule_SaveString(rdb, o->key);

    fieldTableIterator it;
    TairHashField *f;
    ftIterInitReadOnly(&it, &o->hash);
    while ((f = ftIterNext(&it)) != NULL) {
        RedisModule_SaveStringBuffer(rdb, fieldName(f), f->name_len);
        RedisModule_SaveUnsigned(rdb, f->version);
        RedisModule_SaveUnsigned(rdb, f->expire);
        RedisModule_SaveStringBuffer(rdb, fieldValue(f), f->value_len);
    }
    ftIterRelease(&it);
}

void TairHashTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    tairHashObj *o = (tairHashObj *)value;
    fieldTableIterator it;
    TairHashField *f;
    long long now = RedisModule_Milliseconds();
    ftIterInitReadOnly(&it, &o->hash);
    while ((f = ftIterNext(&it)) != NULL) {
        if (f->expire) {
            if (now > f->expire) {
                /* For expired field, we do not REWRITE it. */
                continue;
            }
            RedisModule_EmitAOF(aof, "EXHSET", "sbbclcl", key, fieldName(f), (size_t)f->name_len, fieldValue(f), (size_t)f->value_len, "PXAT",
                                f->expire, "ABS", f->version);
        } else {
            RedisModule_EmitAOF(aof, "EXHSET", "sbbcl", key, fieldName(f), (size_t)f->name_len, fieldValue(f), (size_t)f->value_len, "ABS",
                                f->version);
        }
    }
    ftIterRelease(&it);
}

void TairHashTypeFree(void *value) {
    if (value) {
        tairHashTypeReleaseObject(value);
    }
}

/* Set at load time when the server provides RedisModule_MallocSizeString (Redis 7.0+). */
static size_t (*g_malloc_size_string)(RedisModuleString *str) = NULL;

/* Allocator size of a module string. A short string's bytes are embedded in its object's
 * allocation, whose size the allocator reports exactly; RedisModule_MallocSizeString
 * assumes a full object header, which Valkey 8+ shortens for embedded strings. A longer
 * string's bytes are a separate allocation, which RedisModule_MallocSizeString includes
 * and only the length approximates without it. */
static size_t stringMallocSize(RedisModuleString *str) {
    size_t len;
    const char *ptr = RedisModule_StringPtrLen(str, &len);
    size_t object = allocSize(str, 16 + 3 + len + 1);
    uintptr_t start = (uintptr_t)str, bytes = (uintptr_t)ptr;
    if (bytes > start && bytes + len < start + object) {
        return object;
    }
    if (g_malloc_size_string) {
        return g_malloc_size_string(str);
    }
    return allocSize(str, 16) + len + 1;
}

/* What the hash actually holds in the allocator: the object (which embeds the field
 * table), bucket arrays, fields, key name and the expire index with its own copies of
 * the names of expiring fields. */
static size_t tairHashMemUsage(const tairHashObj *o) {
    size_t size = allocSize(o, sizeof(*o)) + ftMemUsage(&o->hash);
    if (o->key) {
        size += stringMallocSize(o->key);
    }

    if (o->expire_index) {
        size += allocSize(o->expire_index, sizeof(*o->expire_index));
#ifdef SLAB_MODE
        for (tairhash_zskiplistNode *x = o->expire_index->header; x != NULL; x = x->level[0].forward) {
            size += allocSize(x, sizeof(*x));
            if (x->slab) {
                size += allocSize(x->slab, sizeof(*x->slab));
                for (int i = 0; i < x->slab->num_keys; i++) {
                    size += stringMallocSize(x->slab->keys[i]);
                }
            }
        }
#else
        for (m_zskiplistNode *x = o->expire_index->header; x != NULL; x = x->level[0].forward) {
            /* The header has no member. */
            size += allocSize(x, sizeof(*x)) + (x->member ? stringMallocSize(x->member) : 0);
        }
#endif
    }
    return size;
}

#if defined(SORT_MODE) || defined(SLAB_MODE)

size_t TairHashTypeMemUsage2(RedisModuleKeyOptCtx *ctx, const void *value) {
    REDISMODULE_NOT_USED(ctx);
    return value ? tairHashMemUsage((const tairHashObj *)value) : 0;
}

void TairHashTypeUnlink2(RedisModuleKeyOptCtx *ctx, const void *value) {
    struct tairHashObj *o = (struct tairHashObj *)value;

    int dbid = RedisModule_GetDbIdFromOptCtx(ctx);

    long long min_expire;
    if (tairHashMinExpire(o, &min_expire)) {
        /* UNLINK is a synchronous call, so ExpireNode can be safely deleted here. */
        m_zslDelete(dbExpireIndex(dbid), min_expire, o->key, NULL);
    }
}

void *TairHashTypeCopy2(RedisModuleKeyOptCtx *ctx, const void *value) {
    struct tairHashObj *old = (struct tairHashObj *)value;
    struct tairHashObj *new = createTairHashTypeObject();

    int to_dbid = RedisModule_GetToDbIdFromOptCtx(ctx);
    const RedisModuleString *tokey = RedisModule_GetToKeyNameFromOptCtx(ctx);

    new->key = RedisModule_CreateStringFromString(NULL, tokey);
    ftExpand(&new->hash, ftSize(&old->hash));

    fieldTableIterator it;
    TairHashField *f;
    ftIterInitReadOnly(&it, &old->hash);
    while ((f = ftIterNext(&it)) != NULL) {
        TairHashField *copy = fieldCreate(fieldName(f), f->name_len, fieldValue(f), f->value_len);
        copy->version = f->version;
        copy->expire = f->expire;
        ftAdd(&new->hash, copy);
        if (copy->expire) {
            RedisModuleString *field = RedisModule_CreateString(NULL, fieldName(f), f->name_len);
            g_expire_algorithm.insert(NULL, to_dbid, NULL, new, field, copy->expire);
            RedisModule_FreeString(NULL, field);
        }
    }
    ftIterRelease(&it);
    return new;
}

size_t TairHashTypeEffort2(RedisModuleKeyOptCtx *ctx, const void *value) {
    REDISMODULE_NOT_USED(ctx);
    return tairHashFreeEffort(value);
}
#else

size_t TairHashTypeMemUsage(const void *value) {
    return value ? tairHashMemUsage((const tairHashObj *)value) : 0;
}

size_t TairHashTypeEffort(RedisModuleString *key, const void *value) {
    REDISMODULE_NOT_USED(key);
    return tairHashFreeEffort(value);
}

#endif

void TairHashTypeDigest(RedisModuleDigest *md, void *value) {
    tairHashObj *o = (tairHashObj *)value;
    if (!o) {
        return;
    }

    fieldTableIterator it;
    TairHashField *f;
    ftIterInitReadOnly(&it, &o->hash);
    while ((f = ftIterNext(&it)) != NULL) {
        RedisModule_DigestAddStringBuffer(md, (unsigned char *)fieldName(f), f->name_len);
        RedisModule_DigestAddStringBuffer(md, (unsigned char *)fieldValue(f), f->value_len);
        RedisModule_DigestEndSequence(md);
    }
    ftIterRelease(&it);
}

int Module_CreateCommands(RedisModuleCtx *ctx) {
#define CREATE_CMD(name, tgt, attr, firstkey, lastkey, keystep)                                              \
    do {                                                                                                     \
        if (RedisModule_CreateCommand(ctx, name, tgt, attr, firstkey, lastkey, keystep) != REDISMODULE_OK) { \
            RedisModule_Log(ctx, "notice", "reg cmd error");                                                 \
            return REDISMODULE_ERR;                                                                          \
        }                                                                                                    \
    } while (0);

#define CREATE_WRCMD(name, tgt) CREATE_CMD(name, tgt, "write deny-oom", 1, 1, 1);
#define CREATE_ROCMD(name, tgt) CREATE_CMD(name, tgt, "readonly fast", 1, 1, 1);
#define CREATE_WRMCMD(name, tgt, firstkey, lastkey, keystep) CREATE_CMD(name, tgt, "write deny-oom", firstkey, lastkey, keystep);
#define CREATE_ROMCMD(name, tgt, firstkey, lastkey, keystep) CREATE_CMD(name, tgt, "readonly fast", firstkey, lastkey, keystep);

    /* write cmds */
    CREATE_WRCMD("exhset", TairHashTypeHset_RedisCommand)
    CREATE_WRCMD("exhdel", TairHashTypeHdel_RedisCommand)
    CREATE_WRCMD("exhdelrepl", TairHashTypeHdelRepl_RedisCommand)
    CREATE_WRCMD("exhdelwithver", TairHashTypeHdelWithVer_RedisCommand)
    CREATE_WRCMD("exhincrby", TairHashTypeHincrBy_RedisCommand)
    CREATE_WRCMD("exhincrbyfloat", TairHashTypeHincrByFloat_RedisCommand)
    CREATE_WRCMD("exhsetnx", TairHashTypeHsetNx_RedisCommand)
    CREATE_WRCMD("exhmset", TairHashTypeHmset_RedisCommand)
    CREATE_WRCMD("exhmsetwithopts", TairHashTypeHmsetWithOpts_RedisCommand)
    CREATE_WRCMD("exhsetver", TairHashTypeHsetVer_RedisCommand)
    CREATE_WRCMD("exhexpire", TairHashTypeHexpire_RedisCommand)
    CREATE_WRCMD("exhexpireat", TairHashTypeHexpireAt_RedisCommand)
    CREATE_WRCMD("exhpexpire", TairHashTypeHpexpire_RedisCommand)
    CREATE_WRCMD("exhpexpireat", TairHashTypeHpexpireAt_RedisCommand)
    CREATE_WRCMD("exhpersist", TairHashTypeHpersist_RedisCommand)

    /* readonly cmds */
    CREATE_ROCMD("exhget", TairHashTypeHget_RedisCommand)
    CREATE_ROCMD("exhlen", TairHashTypeHlen_RedisCommand)
    CREATE_ROCMD("exhexists", TairHashTypeHexists_RedisCommand)
    CREATE_ROCMD("exhstrlen", TairHashTypeHstrlen_RedisCommand)
    CREATE_ROCMD("exhkeys", TairHashTypeHkeys_RedisCommand)
    CREATE_ROCMD("exhvals", TairHashTypeHvals_RedisCommand)
    CREATE_ROCMD("exhgetall", TairHashTypeHgetAll_RedisCommand)
    CREATE_ROCMD("exhgetallwithver", TairHashTypeHgetAllWithVer_RedisCommand)
    CREATE_ROCMD("exhmget", TairHashTypeHmget_RedisCommand)
    CREATE_ROCMD("exhmgetwithver", TairHashTypeHmgetWithVer_RedisCommand)
    CREATE_ROCMD("exhscan", TairHashTypeHscan_RedisCommand)
    CREATE_ROCMD("exhver", TairHashTypeHver_RedisCommand)
    CREATE_ROCMD("exhttl", TairHashTypeHttl_RedisCommand)
    CREATE_ROCMD("exhpttl", TairHashTypeHpttl_RedisCommand)
    CREATE_ROCMD("exhgetwithver", TairHashTypeHgetWithVer_RedisCommand)
    CREATE_ROMCMD("exhexpireinfo", TairHashTypeActiveExpireInfo_RedisCommand, 0, 0, 0)

    return REDISMODULE_OK;
}

/* The number of databases of the server. The module API has no call for it, and selecting a
 * database past the last one fails, so search for the first index that cannot be selected.
 * The search selects on a thread-safe context, which has a client of its own: the context
 * RedisModule_OnLoad gets has none on Redis 5.0, where selecting on it crashes. */
static int serverDbCount(void) {
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    int lo = 1, hi = 1; /* databases 0..lo-1 exist, database hi does not once the loop below ends */
    while (RedisModule_SelectDb(ctx, hi) == REDISMODULE_OK) {
        lo = hi + 1;
        hi = hi > INT_MAX / 2 ? INT_MAX : hi * 2;
    }
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (RedisModule_SelectDb(ctx, mid) == REDISMODULE_OK) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    RedisModule_FreeThreadSafeContext(ctx);
    return lo;
}

int __attribute__((visibility("default"))) RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "tairhash", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (argc % 2) {
        RedisModule_Log(ctx, "warning", "Invalid number of arguments passed");
        return REDISMODULE_ERR;
    }

    if (RedisModule_GetServerVersion) {
        int version = RedisModule_GetServerVersion();
        redis_patch_ver = version & 0x000000ff;
        redis_minor_ver = (version & 0x0000ff00) >> 8;
        redis_major_ver = (version & 0x00ff0000) >> 16;
    }

#if defined(SORT_MODE) || defined(SLAB_MODE)
    if (redis_major_ver < 7) {
        RedisModule_Log(ctx, "warning", "Redis version (%d.%d.%d) is too old, please upgrade to 7.0.0 or above", redis_major_ver, redis_minor_ver, redis_patch_ver);
        return REDISMODULE_ERR;
    }
#endif

    g_expire_algorithm.enable_active_expire = 1;
    g_expire_algorithm.active_expire_period = TAIR_HASH_ACTIVE_EXPIRE_PERIOD;
    g_expire_algorithm.dbs_per_active_loop = TAIR_HASH_ACTIVE_DBS_PER_CALL;
    g_expire_algorithm.keys_per_active_loop = TAIR_HASH_ACTIVE_EXPIRE_KEYS_PER_LOOP;
    g_expire_algorithm.keys_per_passive_loop = TAIR_HASH_PASSIVE_EXPIRE_KEYS_PER_LOOP;

    for (int ii = 0; ii < argc; ii += 2) {
        if (!mstrcasecmp(argv[ii], "enable_active_expire")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR) {
                RedisModule_Log(ctx, "warning", "Invalid argument for enable_active_expire");
                return REDISMODULE_ERR;
            }
            g_expire_algorithm.enable_active_expire = v;
        } else if (!mstrcasecmp(argv[ii], "active_expire_period")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR) {
                RedisModule_Log(ctx, "warning", "Invalid argument for active_expire_period");
                return REDISMODULE_ERR;
            }
            g_expire_algorithm.active_expire_period = v;
        } else if (!mstrcasecmp(argv[ii], "active_expire_keys_per_loop")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR) {
                RedisModule_Log(ctx, "warning", "Invalid argument for active_expire_keys_per_loop");
                return REDISMODULE_ERR;
            }
            g_expire_algorithm.keys_per_active_loop = v;
        } else if (!mstrcasecmp(argv[ii], "active_expire_dbs_per_loop")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR) {
                RedisModule_Log(ctx, "warning", "Invalid argument for active_expire_dbs_per_loop");
                return REDISMODULE_ERR;
            }
            g_expire_algorithm.dbs_per_active_loop = v;
        } else if (!mstrcasecmp(argv[ii], "passive_expire_keys_per_loop")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR) {
                RedisModule_Log(ctx, "warning", "Invalid argument for passive_expire_keys_per_loop");
                return REDISMODULE_ERR;
            }
            g_expire_algorithm.keys_per_passive_loop = v;
        } else {
            RedisModule_Log(ctx, "warning", "Unrecognized option");
            return REDISMODULE_ERR;
        }
    }

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = TairHashTypeRdbLoad,
        .rdb_save = TairHashTypeRdbSave,
        .aof_rewrite = TairHashTypeAofRewrite,
        .free = TairHashTypeFree,
        .digest = TairHashTypeDigest,
#if defined(SORT_MODE) || defined(SLAB_MODE)
        .unlink2 = TairHashTypeUnlink2,
        .copy2 = TairHashTypeCopy2,
        .free_effort2 = TairHashTypeEffort2,
        .mem_usage2 = TairHashTypeMemUsage2,
#else
        .mem_usage = TairHashTypeMemUsage,
        .free_effort = TairHashTypeEffort,
#endif
    };

    TairHashType = RedisModule_CreateDataType(ctx, "tairhash-", 0, &tm);
    if (TairHashType == NULL)
        return REDISMODULE_ERR;

    ftModuleInit();
    if (RedisModule_GetApi("RedisModule_MallocSizeString", (void **)&g_malloc_size_string) != REDISMODULE_OK) {
        g_malloc_size_string = NULL;
    }

    if (REDISMODULE_ERR == Module_CreateCommands(ctx)) {
        return REDISMODULE_ERR;
    }

    /* Nothing below fails: a load that returns an error above has allocated nothing to leak. */
    g_expire_algorithm.db_num = serverDbCount();
    g_expire_algorithm.stat_active_expired_field = RedisModule_Calloc(g_expire_algorithm.db_num, sizeof(uint64_t));
    g_expire_algorithm.stat_passive_expired_field = RedisModule_Calloc(g_expire_algorithm.db_num, sizeof(uint64_t));

#if defined(SORT_MODE) || defined(SLAB_MODE)
    g_expire_index = RedisModule_Calloc(g_expire_algorithm.db_num, sizeof(*g_expire_index));

    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_SwapDB, swapDbCallback);
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, flushDbCallback);
    RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_GENERIC, keySpaceNotification);
    RedisModule_RegisterInfoFunc(ctx, infoFunc);
#endif

    g_expire_algorithm.insert = insert;
    g_expire_algorithm.update = update;
    g_expire_algorithm.delete = delete;
    g_expire_algorithm.deleteAndPropagate = deleteAndPropagate;
    g_expire_algorithm.activeExpire = activeExpire;
    g_expire_algorithm.passiveExpire = passiveExpire;

    if (g_expire_algorithm.enable_active_expire) {
        /* Here we can't directly use the 'ctx' passed by OnLoad, because
         * in some old version redis `CreateTimer` will trigger a crash, see bugfix:
         * https://github.com/redis/redis/commit/096592506ef3f548a4a3484d5829e04749a24a99
         * https://github.com/redis/redis/commit/7b5f4b175b96dca2093dc1898c3df97e3e096526 */
        RedisModuleCtx *ctx2 = RedisModule_GetThreadSafeContext(NULL);
        startExpireTimer(ctx2, NULL);
        RedisModule_FreeThreadSafeContext(ctx2);
    }
    return REDISMODULE_OK;
}
