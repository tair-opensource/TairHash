/*
 * Copyright 2021 Alibaba Tair Team
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#define REDISMODULE_EXPERIMENTAL_API
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

#include "heap-inl.h"
#include "list.h"
#include "redismodule.h"
#include "util.h"

static RedisModuleType *TairHashType;

#define EX_HASH_SET_NO_FLAGS 0
#define EX_HASH_SET_NX (1 << 0)
#define EX_HASH_SET_XX (1 << 1)
#define EX_HASH_SET_EX (1 << 2)
#define EX_HASH_SET_PX (1 << 3)
#define EX_HASH_SET_ABS_EXPIRE (1 << 4)
#define EX_HASH_SET_WITH_VER (1 << 5)
#define EX_HASH_SET_WITH_ABS_VER (1 << 6)
#define EX_HASH_SET_WITH_BOUNDARY (1 << 7)
#define EX_HASH_SET_WITH_NOACTIVE (1 << 8)
#define EX_HASH_SET_WITH_TRYPASSIVE (1 << 9)

#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

#define EX_HASH_ACTIVE_EXPIRE_PERIOD 1000
#define EX_HASH_ACTIVE_EXPIRE_KEYS_PER_LOOP 200

#define EX_HASH_SCAN_DEFAULT_COUNT 10

#define DB_NUM 16
#define DBS_PER_CALL 3
#define KEYS_PER_LOOP 3

enum SCAN_DIR {
    LEFT,
    RIGHT,
};

static RedisModuleTimerID expire_timer_id = 0;
static struct heap timer_heap[DB_NUM];

static uint32_t enable_active_expire = 1; /* We start active expire by default */
static uint32_t ex_hash_active_expire_period = EX_HASH_ACTIVE_EXPIRE_PERIOD;
static uint32_t ex_hash_active_expire_keys_per_loop = EX_HASH_ACTIVE_EXPIRE_KEYS_PER_LOOP;
static uint32_t ex_hash_active_expire_dbs_per_loop = DBS_PER_CALL;
static uint32_t ex_hash_passive_expire_keys_per_loop = KEYS_PER_LOOP;
static uint64_t stat_expired_field[DB_NUM];
static uint64_t stat_last_active_expire_time_msec;
static uint64_t stat_avg_active_expire_time_msec = 0;
static uint64_t stat_max_active_expire_time_msec = 0;

/* Here we share using a global now time to avoid the performance impact of having to get time
from the system each time passive active is active */
volatile long long g_now;

/*TODO: Change binary heap to multiple heap to reduce height */
#define EXPIRE_NODE_INSERT(key, field)                                                       \
    if (enable_active_expire) {                                                              \
        db_index = RedisModule_GetSelectedDb(ctx);                                           \
        struct expire_node *node = createExpireNode((key), (field), milliseconds, db_index); \
        /* To avoid data duplication, we reuse key and field directly, we                    \
        will release them manually in the future */                                          \
        RedisModule_RetainString(NULL, (key));                                               \
        RedisModule_RetainString(NULL, (field));                                             \
        /* Insert directly, without checking if the field is already in the                  \
        heap, the dirty heap node will be cleared by the timer */                            \
        heap_insert(&timer_heap[db_index], &node->inner_node, expire_node_compare);          \
    }

/* ========================== Internal data structure  =======================*/

typedef struct exHashVal {
    long long version;
    long long expire;
    RedisModuleString *value;
} exHashVal;

static struct exHashVal *createExhashVal(void) {
    struct exHashVal *o;
    o = RedisModule_Calloc(1, sizeof(*o));
    return o;
}

static void exHashValRelease(struct exHashVal *o) {
    if (o) {
        if (o->value) RedisModule_FreeString(NULL, o->value);
        RedisModule_Free(o);
    }
}

typedef struct exHashObj {
    RedisModuleDict *hash;
    RedisModuleString *key;
    unsigned int dbid;
} exHashObj;

static struct exHashObj *createExhashTypeObject() {
    exHashObj *o = RedisModule_Calloc(1, sizeof(*o));
    return o;
}

static void freeAllExpireNodes(struct heap *heap);
static void exHashTypeReleaseObject(struct exHashObj *o) {
    size_t keylen;
    void *data;
    if (o->hash) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(o->hash, "^", NULL, 0);
        while (RedisModule_DictNextC(iter, &keylen, &data)) {
            exHashValRelease(data);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(NULL, o->hash);
    }
    if (o->key) RedisModule_FreeString(NULL, o->key);
    RedisModule_Free(o);
}

#pragma pack(1)
typedef struct expire_node {
    struct heap_node inner_node;
    RedisModuleString *key;
    RedisModuleString *field;
    long long expire;
    unsigned char dbid;
} expire_node;

static struct expire_node *toExpireNode(const struct heap_node *node) { return (struct expire_node *)node; }

static struct expire_node *createExpireNode(RedisModuleString *key, RedisModuleString *field, long long expire,
                                            int dbid) {
    struct expire_node *node = RedisModule_Calloc(1, sizeof(*node));
    node->key = key;
    node->field = field;
    node->expire = expire;
    node->dbid = dbid;
    return node;
}

static void freeExpireNode(struct expire_node *node) {
    if (!node) return;

    if (node->key) {
        RedisModule_FreeString(NULL, node->key);
        node->key = NULL;
    }

    if (node->field) {
        RedisModule_FreeString(NULL, node->field);
        node->field = NULL;
    }

    RedisModule_Free(node);
}

static inline int expire_node_compare(const struct heap_node *a, const struct heap_node *b) {
    return toExpireNode(a)->expire < toExpireNode(b)->expire;
}

static void freeAllExpireNodes(struct heap *heap) {
    if (!heap) return;

    struct heap_node *root_node;
    while ((root_node = heap_min(heap)) != NULL) {
        heap_remove(heap, root_node, expire_node_compare);
        freeExpireNode(toExpireNode(root_node));
    }
}

/* ========================== Common  func =============================*/
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec) * 1000000;
    ust += tv.tv_usec;
    return ust;
}

static inline long long mstime(void) {
    long long now = ustime() / 1000;
    g_now = now;
    return now;
}

inline static int expireTairHashObjIfNeeded(RedisModuleCtx *ctx, RedisModuleString *key, exHashObj *o,
                                            RedisModuleString *field, int is_timer_ctx) {
    int nokey;
    exHashVal *ex_hash_val = NULL;
    ex_hash_val = RedisModule_DictGet(o->hash, field, &nokey);

    if (nokey) return 0;

    long long when = ex_hash_val->expire;
    long long now;

    if (is_timer_ctx) {
        now = g_now;
    } else {
        now = mstime();
    }

    if (when == 0) return 0;

    /* Slave only determines if it has timed out and does not perform a delete operation */
    if (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE) {
        return now > when;
    }

    if (now < when) return 0;

    if (is_timer_ctx) {
        /* In the timer handler Context we cannot directly use RedisModule_Replicate!
           If we just use RedisModule_Call and add one ! flag, there will be two MULTI
           are generated, so we do a hack, to mark the ctx2 plus a REDISMODULE_CTX_THREAD_SAFE,
           so moduleReplicateMultiIfNeeded won't produce a MULTI for the RedisModule_Call.
        */
        RedisModuleCtx *ctx2 = RedisModule_GetThreadSafeContext(NULL);
        RedisModule_SelectDb(ctx2, RedisModule_GetSelectedDb(ctx));
        RedisModuleCallReply *reply = RedisModule_Call(ctx2, "EXHDEL", "ss!", key, field);
        if (reply != NULL) {
            RedisModule_FreeCallReply(reply);
        }
        RedisModule_FreeThreadSafeContext(ctx2);
    } else {
        exHashVal *ex_hash_oldval = NULL;
        RedisModule_DictDel(o->hash, field, &ex_hash_oldval);
        RedisModule_Replicate(ctx, "EXHDEL", "ss", key, field);
        exHashValRelease(ex_hash_oldval);
    }

    return 1;
}

int delEmptyExhashIfNeeded(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *raw_key, RedisModuleDict *hash,
                           int field_expired) {
    if (!hash || RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE || RedisModule_DictSize(hash) != 0) {
        return 0;
    }

    RedisModule_DeleteKey(key);
    RedisModule_Replicate(ctx, "DEL", "s", raw_key);
    return 1;
}

/* The function in the EXHGETALL EXHKEYS/EXHVALS command is invoked, the goal is to make these
commands can return ASAP */
int expireTairHashObjIfNeededNoDel(RedisModuleCtx *ctx, RedisModuleString *key, exHashObj *o, RedisModuleString *field) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(key);

    int nokey = 0;
    exHashVal *ex_hash_val = NULL;
    ex_hash_val = RedisModule_DictGet(o->hash, field, &nokey);

    if (nokey) return 0;

    mstime_t when = ex_hash_val->expire;
    mstime_t now = mstime();

    /* No expire */
    if (when == 0) return 0;

    return now > when;
}

inline static int isExpire(long long when) {
    /* No expire */
    if (when == 0) return 0;

    mstime_t now = mstime();
    return now > when;
}

/* Active expire algorithm implementiation */
void activeExpireTimerHandler(RedisModuleCtx *ctx, void *data) {
    REDISMODULE_NOT_USED(data);
    RedisModule_AutoMemory(ctx);

    int keys_per_db;
    int i = 0;
    static unsigned int current_db = 0;
    static unsigned long long loop_cnt = 0, total_expire_time = 0;

    exHashObj *ex_hash_obj = NULL;
    struct expire_node *expire_node = NULL;
    long long when;
    int nokey;

    int dbs_per_call = ex_hash_active_expire_dbs_per_loop;
    if (dbs_per_call > DB_NUM) dbs_per_call = DB_NUM;

    long long start = mstime();

    for (; i < dbs_per_call; ++i) {
        keys_per_db = ex_hash_active_expire_keys_per_loop;
        while (keys_per_db--) {
            expire_node = toExpireNode(heap_min(&timer_heap[current_db % DB_NUM]));
            /* Empty heap */
            if (expire_node == NULL) {
                /* Avoid continuous scanning of empty db */
                dbs_per_call = dbs_per_call < DB_NUM ? ++dbs_per_call : DB_NUM;
                /* Next DB */
                break;
            }

            RedisModule_SelectDb(ctx, expire_node->dbid);
            long long dbsize = RedisModule_DbSize(ctx);
            /* Perform a quick cleanup when there is no data in the DB (such as when flushall is executed) */
            if (dbsize == 0) {
                freeAllExpireNodes(&timer_heap[current_db % DB_NUM]);
                /* Next DB */
                break;
            }

            when = expire_node->expire;

            /* 0 means no timeout and should not theoretically be added to timer_heap */
            if (when == 0) {
                heap_remove(&timer_heap[current_db % DB_NUM], &expire_node->inner_node, expire_node_compare);
                freeExpireNode(expire_node);
                continue;
            }

            RedisModuleKey *key = RedisModule_OpenKey(ctx, expire_node->key, REDISMODULE_READ | REDISMODULE_WRITE);
            int type = RedisModule_KeyType(key);
            /* Key has been deleted or the type is not exhash ,just delete the dirty node */
            if (REDISMODULE_KEYTYPE_EMPTY == type || RedisModule_ModuleTypeGetType(key) != TairHashType) {
                heap_remove(&timer_heap[current_db % DB_NUM], &expire_node->inner_node, expire_node_compare);
                freeExpireNode(expire_node);
                continue;
            }

            ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
            if (g_now > when) {
                if (ex_hash_obj && ex_hash_obj->hash) {
                    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, expire_node->field, &nokey);
                    if (nokey || ex_hash_val->expire != expire_node->expire) {
                        /* In master and slave, the dirty node in the heap can be removed directly */
                        heap_remove(&timer_heap[current_db % DB_NUM], &expire_node->inner_node, expire_node_compare);
                        freeExpireNode(expire_node);
                        continue;
                    }

                    if (expireTairHashObjIfNeeded(ctx, expire_node->key, ex_hash_obj, expire_node->field, 1)) {
                        /* In slave,the expired node in the heap can not be removed directly */
                        if (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE) {
                            continue;
                        }
                        /* In master,the expired node in the heap can be removed directly */
                        heap_remove(&timer_heap[current_db % DB_NUM], &expire_node->inner_node, expire_node_compare);
                        freeExpireNode(expire_node);
                        stat_expired_field[current_db % DB_NUM]++;
                    }
                }
            } else {
                /* Next DB */
                break;
            }
        }
        current_db++;
    }

    stat_last_active_expire_time_msec = mstime() - start;
    stat_max_active_expire_time_msec = stat_max_active_expire_time_msec < stat_last_active_expire_time_msec
                                           ? stat_last_active_expire_time_msec
                                           : stat_max_active_expire_time_msec;
    total_expire_time += stat_last_active_expire_time_msec;
    ++loop_cnt;
    if (loop_cnt % 1000 == 0) {
        stat_avg_active_expire_time_msec = total_expire_time / loop_cnt;
        loop_cnt = 0;
        total_expire_time = 0;
    }

    /* Since the module that exports the data type must not be uninstalled, the timer can always be run */
    if (enable_active_expire)
        expire_timer_id = RedisModule_CreateTimer(ctx, ex_hash_active_expire_period, activeExpireTimerHandler, NULL);
}

void startExpireTimer(RedisModuleCtx *ctx, void *data) {
    if (!enable_active_expire) return;

    if (RedisModule_GetTimerInfo(ctx, expire_timer_id, NULL, NULL) == REDISMODULE_OK) {
        return;
    }

    expire_timer_id = RedisModule_CreateTimer(ctx, ex_hash_active_expire_period, activeExpireTimerHandler, data);
}

inline static void latencySensitivePassiveExpire(RedisModuleCtx *ctx, unsigned int db) {
    exHashObj *ex_hash_obj = NULL;
    struct expire_node *expire_node = NULL;
    long long when;
    int nokey;

    int keys_per_loop = ex_hash_passive_expire_keys_per_loop;

    while (keys_per_loop--) {
        expire_node = toExpireNode(heap_min(&timer_heap[db]));
        /* Empty heap */
        if (expire_node == NULL) {
            return;
        }

        when = expire_node->expire;

        RedisModuleKey *key = RedisModule_OpenKey(ctx, expire_node->key, REDISMODULE_READ | REDISMODULE_WRITE);
        int type = RedisModule_KeyType(key);

        if (REDISMODULE_KEYTYPE_EMPTY == type || RedisModule_ModuleTypeGetType(key) != TairHashType) {
            heap_remove(&timer_heap[db], &expire_node->inner_node, expire_node_compare);
            freeExpireNode(expire_node);
            return;
        }

        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
        RedisModule_CloseKey(key);

        if (g_now > when) {
            if (ex_hash_obj && ex_hash_obj->hash) {
                exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, expire_node->field, &nokey);
                if (nokey || ex_hash_val->expire != expire_node->expire) {
                    heap_remove(&timer_heap[db], &expire_node->inner_node, expire_node_compare);
                    freeExpireNode(expire_node);
                    return;
                }

                if (expireTairHashObjIfNeeded(ctx, expire_node->key, ex_hash_obj, expire_node->field, 0)) {
                    if (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE) {
                        continue;
                    }
                    heap_remove(&timer_heap[db], &expire_node->inner_node, expire_node_compare);
                    freeExpireNode(expire_node);
                }
            }
        } else {
            return;
        }
    }
}

static int rsStrcasecmp(const RedisModuleString *rs1, const char *s2) {
    size_t n1 = strlen(s2);
    size_t n2;
    const char *s1 = RedisModule_StringPtrLen(rs1, &n2);
    if (n1 != n2) {
        return -1;
    }
    return strncasecmp(s1, s2, n1);
}

static int mstrmatchlen(RedisModuleString *pattern, RedisModuleString *str, int nocase) {
    size_t plen, slen;
    const char *pattern_p = RedisModule_StringPtrLen(pattern, &plen);
    const char *str_p = RedisModule_StringPtrLen(str, &slen);
    return m_stringmatchlen(pattern_p, plen, str_p, slen, nocase);
}

int exhashExpireGenericFunc(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, long long basetime, int unit) {
    RedisModule_AutoMemory(ctx);
    int j;

    if (argc < 4 || argc > 7) {
        return RedisModule_WrongArity(ctx);
    }

    long long milliseconds;
    int nokey;
    int field_expired = 0;
    int db_index = 0;
    long long version = 0;

    if (RedisModule_StringToLongLong(argv[3], &milliseconds) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleString *version_p = NULL;
    int ex_flags = EX_HASH_SET_NO_FLAGS;

    if (argc > 4) {
        for (j = 4; j < argc; j++) {
            RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];

            if (!rsStrcasecmp(argv[4], "ver") && !(ex_flags & EX_HASH_SET_WITH_ABS_VER) && next) {
                ex_flags |= EX_HASH_SET_WITH_VER;
                version_p = next;
                j++;
            } else if (!rsStrcasecmp(argv[4], "abs") && !(ex_flags & EX_HASH_SET_WITH_VER) && next) {
                ex_flags |= EX_HASH_SET_WITH_ABS_VER;
                version_p = next;
                j++;
            } else if (!rsStrcasecmp(argv[j], "noactive")) {
                ex_flags |= EX_HASH_SET_WITH_NOACTIVE;
            } else {
                RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
                return REDISMODULE_ERR;
            }
        }
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0 || ((ex_flags & EX_HASH_SET_WITH_ABS_VER) && version == 0)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expired = 1;
    }

    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (field_expired || nokey || !ex_hash_val) {
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (ex_flags & EX_HASH_SET_WITH_VER && version != 0 && version != ex_hash_val->version) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }

        if (unit == UNIT_SECONDS) milliseconds *= 1000;
        milliseconds += basetime;
        ex_hash_val->expire = milliseconds;
        if (milliseconds > 0) {
            if (!(ex_flags & EX_HASH_SET_WITH_NOACTIVE)) EXPIRE_NODE_INSERT(argv[1], argv[2])
        }
        RedisModule_ReplyWithLongLong(ctx, 1);
    }

    if (ex_flags & EX_HASH_SET_WITH_ABS_VER) {
        ex_hash_val->version = version;
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int exhashTTLGenericFunc(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int unit) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    int nokey;
    int field_expired = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -2);
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expired = 1;
    }

    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (field_expired || nokey || !ex_hash_val) {
        RedisModule_ReplyWithLongLong(ctx, -3);
    } else {
        if (ex_hash_val->expire == 0) {
            RedisModule_ReplyWithLongLong(ctx, -1);
        } else {
            long long ttl = ex_hash_val->expire - mstime();
            if (ttl < 0) ttl = 0;
            if (UNIT_SECONDS == unit)
                RedisModule_ReplyWithLongLong(ctx, (ttl + 500) / 1000);
            else
                RedisModule_ReplyWithLongLong(ctx, ttl);
            ;
        }
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired);
    return REDISMODULE_OK;
}

int mstring2ld(RedisModuleString *val, long double *r_val) {
    if (!val) return REDISMODULE_ERR;

    size_t t_len;
    const char *t_ptr = RedisModule_StringPtrLen(val, &t_len);
    if (m_string2ld(t_ptr, t_len, r_val) == 0) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

/* ========================= "exhash" type commands ======================= */
/* EXHSET <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [NX|XX] [VER version | ABS version] [
 * NOACTIVE ] [ WITHPE ] */
int TairHashTypeHset_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    long long milliseconds = 0, expire = 0, version = 0;
    RedisModuleString *expire_p = NULL, *version_p = NULL;
    int j;
    int ex_flags = EX_HASH_SET_NO_FLAGS;
    int nokey = 0;
    int db_index;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    for (j = 4; j < argc; j++) {
        RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];

        if (!rsStrcasecmp(argv[j], "nx") && !(ex_flags & EX_HASH_SET_XX)) {
            ex_flags |= EX_HASH_SET_NX;
        } else if (!rsStrcasecmp(argv[j], "xx") && !(ex_flags & EX_HASH_SET_NX)) {
            ex_flags |= EX_HASH_SET_XX;
        } else if (!rsStrcasecmp(argv[j], "ex") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_EX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "exat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_EX;
            ex_flags |= EX_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "px") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_PX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "pxat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_PX;
            ex_flags |= EX_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "ver") && !(ex_flags & EX_HASH_SET_WITH_ABS_VER) && next) {
            ex_flags |= EX_HASH_SET_WITH_VER;
            version_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "abs") && !(ex_flags & EX_HASH_SET_WITH_VER) && next) {
            ex_flags |= EX_HASH_SET_WITH_ABS_VER;
            version_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "noactive") && (ex_flags & EX_HASH_SET_PX || ex_flags & EX_HASH_SET_EX)) {
            ex_flags |= EX_HASH_SET_WITH_NOACTIVE;
        } else if (!rsStrcasecmp(argv[j], "withpe")) {
            ex_flags |= EX_HASH_SET_WITH_TRYPASSIVE;
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (expire < 0) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0 || ((ex_flags & EX_HASH_SET_WITH_ABS_VER) && version == 0)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (ex_flags & EX_HASH_SET_WITH_TRYPASSIVE) {
        latencySensitivePassiveExpire(ctx, RedisModule_GetSelectedDb(ctx));
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        if (ex_flags & EX_HASH_SET_XX) {
            RedisModule_ReplyWithLongLong(ctx, -1);
            return REDISMODULE_ERR;
        }
        ex_hash_obj = createExhashTypeObject();
        ex_hash_obj->hash = RedisModule_CreateDict(NULL);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0);
    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (nokey) {
        if (ex_flags & EX_HASH_SET_XX) {
            RedisModule_ReplyWithLongLong(ctx, -1);
            return REDISMODULE_ERR;
        }
        ex_hash_val = createExhashVal();
        ex_hash_val->version = 0;
    } else {
        if (ex_flags & EX_HASH_SET_NX) {
            RedisModule_ReplyWithLongLong(ctx, -1);
            return REDISMODULE_ERR;
        }

        /* Version equals 0 means no version checking */
        if (ex_flags & EX_HASH_SET_WITH_VER && version != 0 && version != ex_hash_val->version) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }
    }

    ex_hash_val->version += 1;

    if (ex_flags & EX_HASH_SET_WITH_ABS_VER) {
        ex_hash_val->version = version;
    }

    if (0 < expire) {
        if (ex_flags & EX_HASH_SET_EX) {
            expire *= 1000;
        }
        if (ex_flags & EX_HASH_SET_ABS_EXPIRE) {
            milliseconds = expire;
        } else {
            milliseconds = mstime() + expire;
        }
    } else if (0 == expire) {
        milliseconds = 0;
    }

    ex_hash_val->expire = milliseconds;
    if (milliseconds > 0) {
        if (!(ex_flags & EX_HASH_SET_WITH_NOACTIVE)) EXPIRE_NODE_INSERT(argv[1], argv[2])
    }

    if (!nokey && ex_hash_val->value) {
        RedisModule_FreeString(NULL, ex_hash_val->value);
    }

    ex_hash_val->value = argv[3];
    /* we reuse it,to avoid memcopy */
    RedisModule_RetainString(NULL, argv[3]);

    if (nokey) {
        RedisModule_DictSet(ex_hash_obj->hash, argv[2], ex_hash_val);
        RedisModule_ReplyWithLongLong(ctx, 1);
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
    }

    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* EXHSETNX <key> <field> <value> */
int TairHashTypeHsetNx_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    int nokey = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        ex_hash_obj = createExhashTypeObject();
        ex_hash_obj->hash = RedisModule_CreateDict(NULL);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (nokey) {
        ex_hash_val = createExhashVal();
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    ex_hash_val->value = argv[3];
    /* we reuse it,to avoid memcopy */
    RedisModule_RetainString(NULL, argv[3]);
    RedisModule_DictSet(ex_hash_obj->hash, argv[2], ex_hash_val);

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}

/* EXHMSET key field value [field value …] */
int TairHashTypeHmset_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if ((argc % 2) == 1) {
        return RedisModule_WrongArity(ctx);
    }

    int nokey;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        ex_hash_obj = createExhashTypeObject();
        ex_hash_obj->hash = RedisModule_CreateDict(NULL);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    for (int i = 2; i < argc; i += 2) {
        expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[i], 0);
        exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[i], &nokey);
        if (nokey) {
            ex_hash_val = createExhashVal();
            if (ex_hash_val == NULL) {
                RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
                return REDISMODULE_ERR;
            }
        } else {
            if (ex_hash_val->value) RedisModule_FreeString(NULL, ex_hash_val->value);
        }
        ex_hash_val->value = argv[i + 1];
        /* we reuse it,to avoid memcopy */
        RedisModule_RetainString(NULL, argv[i + 1]);
        ex_hash_val->version++;

        if (nokey) {
            RedisModule_DictSet(ex_hash_obj->hash, argv[i], ex_hash_val);
        }
    }

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* EXHMSETWITHOPTS exhashkey field1 val1 ver1 expire1 [field2 val2 ver2 expire2 ...] */
int TairHashTypeHmsetWithOpts_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (((argc - 2) % 4) != 0) {
        return RedisModule_WrongArity(ctx);
    }

    int db_index = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        ex_hash_obj = createExhashTypeObject();
        ex_hash_obj->hash = RedisModule_CreateDict(NULL);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    long long ver;
    long long when;
    long long milliseconds = 0;

    int nokey;
    for (int i = 2; i < argc; i += 4) {
        if (RedisModule_StringToLongLong(argv[i + 2], &ver) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        if (RedisModule_StringToLongLong(argv[i + 3], &when) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        if (ver < 0 || when < 0) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[i], 0);
        exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[i], &nokey);
        if (nokey || ver == 0 || ex_hash_val->version == ver) {
            continue;
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }
    }

    for (int i = 2; i < argc; i += 4) {
        if (RedisModule_StringToLongLong(argv[i + 3], &when) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[i], &nokey);
        if (nokey) {
            ex_hash_val = createExhashVal();
        } else {
            if (ex_hash_val->value) RedisModule_FreeString(NULL, ex_hash_val->value);
        }
        ex_hash_val->value = argv[i + 1];
        /* we reuse it,to avoid memcopy */
        RedisModule_RetainString(NULL, argv[i + 1]);
        ex_hash_val->version++;
        if (when > 0) {
            milliseconds = mstime() + when * 1000;
        } else if (when == 0) {
            milliseconds = 0;
        }

        ex_hash_val->expire = milliseconds;
        if (milliseconds > 0) {
            EXPIRE_NODE_INSERT(argv[1], argv[i])
        }

        if (nokey) {
            RedisModule_DictSet(ex_hash_obj->hash, argv[i], ex_hash_val);
        }
    }

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/*  EXHPEXPIREAT <key> <field> <milliseconds-timestamp> [ VER version | ABS version ] [ NOACTIVE ]*/
int TairHashTypeHpexpireAt_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return exhashExpireGenericFunc(ctx, argv, argc, 0, UNIT_MILLISECONDS);
}

/*  EXHPEXPIRE <key> <field> <milliseconds> [ VER version | ABS version ] [ NOACTIVE ]*/
int TairHashTypeHpexpire_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return exhashExpireGenericFunc(ctx, argv, argc, mstime(), UNIT_MILLISECONDS);
}

/*  EXHEXPIREAT <key> <field> <timestamp> [ VER version | ABS version ] [ NOACTIVE ]*/
int TairHashTypeHexpireAt_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return exhashExpireGenericFunc(ctx, argv, argc, 0, UNIT_SECONDS);
}

/*  EXHEXPIRE <key> <field> <seconds> [ VER version | ABS version ] [ NOACTIVE ]*/
int TairHashTypeHexpire_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return exhashExpireGenericFunc(ctx, argv, argc, mstime(), UNIT_SECONDS);
}

/*  EXHPTTL <key> <field> */
int TairHashTypeHpttl_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return exhashTTLGenericFunc(ctx, argv, argc, UNIT_MILLISECONDS);
}

/*  EXHTTL <key> <field> */
int TairHashTypeHttl_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return exhashTTLGenericFunc(ctx, argv, argc, UNIT_SECONDS);
}

/*  EXHVER <key> <field> */
int TairHashTypeHver_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    int nokey;
    int field_expired = 0;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, -1);
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expired = 1;
    }

    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (field_expired || nokey || !ex_hash_val) {
        RedisModule_ReplyWithLongLong(ctx, -2);
    } else {
        RedisModule_ReplyWithLongLong(ctx, ex_hash_val->version);
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired);
    return REDISMODULE_OK;
}

/*  EXHSETVER <key> <field> <version> */
int TairHashTypeHsetVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    long long version;
    int nokey;

    if (RedisModule_StringToLongLong(argv[3], &version) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    /* version must > 0 */
    if (version <= 0) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (nokey) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, 1);
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    ex_hash_val->version = version;
    RedisModule_ReplyWithLongLong(ctx, 1);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* EXHINCRBY <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [VER version | ABS version] [MIN minval]
 * [MAX maxval] [NOACTIVE] [ WITHPE ] */
int TairHashTypeHincrBy_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    long long milliseconds = 0, expire = 0, incr = 0, version = 0, min = 0, max = 0;
    RedisModuleString *expire_p = NULL;
    RedisModuleString *version_p = NULL;
    RedisModuleString *min_p = NULL, *max_p = NULL;
    int j;
    int ex_flags = EX_HASH_SET_NO_FLAGS;
    int nokey = 0;
    int db_index;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    if (RedisModule_StringToLongLong(argv[3], &incr) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_NOT_INTEGER);
        return REDISMODULE_ERR;
    }

    for (j = 4; j < argc; j++) {
        RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];

        if (!rsStrcasecmp(argv[j], "ex") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
            ex_flags |= EX_HASH_SET_EX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "exat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_EX;
            ex_flags |= EX_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "px") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_PX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "pxat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_PX;
            ex_flags |= EX_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "ver") && !(ex_flags & EX_HASH_SET_WITH_ABS_VER) && next) {
            ex_flags |= EX_HASH_SET_WITH_VER;
            version_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "abs") && !(ex_flags & EX_HASH_SET_WITH_VER) && next) {
            ex_flags |= EX_HASH_SET_WITH_ABS_VER;
            version_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "min") && next) {
            ex_flags |= EX_HASH_SET_WITH_BOUNDARY;
            min_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "max") && next) {
            ex_flags |= EX_HASH_SET_WITH_BOUNDARY;
            max_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "noactive") && (ex_flags & EX_HASH_SET_PX || ex_flags & EX_HASH_SET_EX)) {
            ex_flags |= EX_HASH_SET_WITH_NOACTIVE;
        } else if (!rsStrcasecmp(argv[j], "withpe")) {
            ex_flags |= EX_HASH_SET_WITH_TRYPASSIVE;
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (expire < 0) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0 || ((ex_flags & EX_HASH_SET_WITH_ABS_VER) && version == 0)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != min_p) && (RedisModule_StringToLongLong(min_p, &min))) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INT_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != max_p) && (RedisModule_StringToLongLong(max_p, &max))) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INT_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if (NULL != min_p && NULL != max_p && max < min) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if (ex_flags & EX_HASH_SET_WITH_TRYPASSIVE) {
        latencySensitivePassiveExpire(ctx, RedisModule_GetSelectedDb(ctx));
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        ex_hash_obj = createExhashTypeObject();
        ex_hash_obj->hash = RedisModule_CreateDict(NULL);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0);
    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (nokey) {
        ex_hash_val = createExhashVal();
    }

    long long cur_val;
    if (type == REDISMODULE_KEYTYPE_EMPTY || nokey) {
        ex_hash_val->value = RedisModule_CreateStringFromLongLong(NULL, 0);
        cur_val = 0;
        ex_hash_val->version = 0;
    } else {
        if (RedisModule_StringToLongLong(ex_hash_val->value, &cur_val) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_NOT_INTEGER);
            return REDISMODULE_ERR;
        }

        /* Version equals 0 means no version checking */
        if (ex_flags & EX_HASH_SET_WITH_VER && version != 0 && version != ex_hash_val->version) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }
    }

    if ((incr < 0 && cur_val < 0 && incr < (LLONG_MIN - cur_val))
        || (incr > 0 && cur_val > 0 && incr > (LLONG_MAX - cur_val)) || (max_p != NULL && cur_val + incr > max)
        || (min_p != NULL && cur_val + incr < min)) {
        if (nokey) exHashValRelease(ex_hash_val);
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_OVERFLOW);
        return REDISMODULE_ERR;
    }

    ex_hash_val->version++;

    if (ex_flags & EX_HASH_SET_WITH_ABS_VER) {
        ex_hash_val->version = version;
    }

    cur_val += incr;

    if (ex_hash_val->value) RedisModule_FreeString(NULL, ex_hash_val->value);
    ex_hash_val->value = RedisModule_CreateStringFromLongLong(NULL, cur_val);

    if (0 < expire) {
        if (ex_flags & EX_HASH_SET_EX) {
            expire *= 1000;
        }
        if (ex_flags & EX_HASH_SET_ABS_EXPIRE) {
            milliseconds = expire;
        } else {
            milliseconds = mstime() + expire;
        }
    } else if (0 == expire) {
        milliseconds = 0;
    }

    ex_hash_val->expire = milliseconds;
    if (milliseconds > 0) {
        if (!(ex_flags & EX_HASH_SET_WITH_NOACTIVE)) EXPIRE_NODE_INSERT(argv[1], argv[2])
    }

    if (nokey) {
        RedisModule_DictSet(ex_hash_obj->hash, argv[2], ex_hash_val);
    }

    RedisModule_Replicate(ctx, "EXHSET", "sss", argv[1], argv[2], ex_hash_val->value);
    RedisModule_ReplyWithLongLong(ctx, cur_val);
    return REDISMODULE_OK;
}

/* EXHINCRBYFLOAT <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [VER version | ABS version] [MIN
 * minval] [MAX maxval] [NOACTIVE] */
int TairHashTypeHincrByFloat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    long long milliseconds = 0, expire = 0, version = 0;
    long double incr = 0, min = 0, max = 0;
    RedisModuleString *expire_p = NULL;
    RedisModuleString *version_p = NULL;
    RedisModuleString *min_p = NULL, *max_p = NULL;
    int j;
    int ex_flags = EX_HASH_SET_NO_FLAGS;
    int nokey = 0;
    int db_index;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    if (mstring2ld(argv[3], &incr) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_NOT_FLOAT);
        return REDISMODULE_ERR;
    }

    for (j = 4; j < argc; j++) {
        RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];

        if (!rsStrcasecmp(argv[j], "ex") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
            ex_flags |= EX_HASH_SET_EX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "exat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_EX;
            ex_flags |= EX_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "px") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_PX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "pxat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX)
                   && next) {
            ex_flags |= EX_HASH_SET_PX;
            ex_flags |= EX_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "ver") && !(ex_flags & EX_HASH_SET_WITH_ABS_VER) && next) {
            ex_flags |= EX_HASH_SET_WITH_VER;
            version_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "abs") && !(ex_flags & EX_HASH_SET_WITH_VER) && next) {
            ex_flags |= EX_HASH_SET_WITH_ABS_VER;
            version_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "min") && next) {
            ex_flags |= EX_HASH_SET_WITH_BOUNDARY;
            min_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "max") && next) {
            ex_flags |= EX_HASH_SET_WITH_BOUNDARY;
            max_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "noactive") && (ex_flags & EX_HASH_SET_PX || ex_flags & EX_HASH_SET_EX)) {
            ex_flags |= EX_HASH_SET_WITH_NOACTIVE;
        } else if (!rsStrcasecmp(argv[j], "withpe")) {
            ex_flags |= EX_HASH_SET_WITH_TRYPASSIVE;
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (expire < 0) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != version_p) && (RedisModule_StringToLongLong(version_p, &version) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (version < 0 || ((ex_flags & EX_HASH_SET_WITH_ABS_VER) && version == 0)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != min_p) && (mstring2ld(min_p, &min) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_FLOAT_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if ((NULL != max_p) && (mstring2ld(max_p, &max) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_FLOAT_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if (NULL != min_p && NULL != max_p && max < min) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_MIN_MAX);
        return REDISMODULE_ERR;
    }

    if (ex_flags & EX_HASH_SET_WITH_TRYPASSIVE) {
        latencySensitivePassiveExpire(ctx, RedisModule_GetSelectedDb(ctx));
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        ex_hash_obj = createExhashTypeObject();
        ex_hash_obj->hash = RedisModule_CreateDict(NULL);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0);
    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (nokey) {
        ex_hash_val = createExhashVal();
    }

    long double cur_val;
    if (type == REDISMODULE_KEYTYPE_EMPTY || nokey) {
        ex_hash_val->value = RedisModule_CreateStringFromLongLong(NULL, 0);
        cur_val = 0;
        ex_hash_val->version = 0;
    } else {
        if (mstring2ld(ex_hash_val->value, &cur_val) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_NOT_FLOAT);
            return REDISMODULE_ERR;
        }

        /* Version equals 0 means no version checking */
        if (ex_flags & EX_HASH_SET_WITH_VER && version != 0 && version != ex_hash_val->version) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }
    }

    if (isnan(cur_val + incr) || isinf(cur_val + incr)) {
        if (nokey) exHashValRelease(ex_hash_val);
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_OVERFLOW);
        return REDISMODULE_ERR;
    }

    if ((max_p != NULL && cur_val + incr > max) || (min_p != NULL && cur_val + incr < min)) {
        if (nokey) exHashValRelease(ex_hash_val);
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_OVERFLOW);
        return REDISMODULE_ERR;
    }

    ex_hash_val->version++;

    if (ex_flags & EX_HASH_SET_WITH_ABS_VER) {
        ex_hash_val->version = version;
    }

    cur_val += incr;

    char dbuf[MAX_LONG_DOUBLE_CHARS] = {0};
    int dlen = m_ld2string(dbuf, sizeof(dbuf), cur_val, 1);

    if (ex_hash_val->value) RedisModule_FreeString(NULL, ex_hash_val->value);
    ex_hash_val->value = RedisModule_CreateString(NULL, dbuf, dlen);

    if (0 < expire) {
        if (ex_flags & EX_HASH_SET_EX) {
            expire *= 1000;
        }
        if (ex_flags & EX_HASH_SET_ABS_EXPIRE) {
            milliseconds = expire;
        } else {
            milliseconds = mstime() + expire;
        }
    } else if (0 == expire) {
        milliseconds = 0;
    }

    ex_hash_val->expire = milliseconds;
    if (milliseconds > 0) {
        if (!(ex_flags & EX_HASH_SET_WITH_NOACTIVE)) EXPIRE_NODE_INSERT(argv[1], argv[2])
    }

    if (nokey) {
        RedisModule_DictSet(ex_hash_obj->hash, argv[2], ex_hash_val);
    }

    /* FIXME:version */
    RedisModule_Replicate(ctx, "EXHSET", "sss", argv[1], argv[2], ex_hash_val->value);
    RedisModule_ReplyWithString(ctx, ex_hash_val->value);
    return REDISMODULE_OK;
}

/* EXHGET <key> <field> */
int TairHashTypeHget_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int field_expire = 0, nokey;
    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expire = 1;
    }

    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (field_expire || nokey || !ex_hash_val) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        RedisModule_ReplyWithString(ctx, ex_hash_val->value);
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expire);
    return REDISMODULE_OK;
}

/* EXHGETWITHVER <key> <field> */
int TairHashTypeHgetWithVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int field_expired = 0, nokey;

    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expired = 1;
    }

    exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (field_expired || nokey || !ex_hash_val) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithString(ctx, ex_hash_val->value);
        RedisModule_ReplyWithLongLong(ctx, ex_hash_val->version);
    }
    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired);
    return REDISMODULE_OK;
}

/* EXHMGET key field [field ...] */
int TairHashTypeHmget_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int cn = 0, field_expired = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (int ii = 2; ii < argc; ++ii) {
        if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[ii], 0)) {
            RedisModule_ReplyWithNull(ctx);
            ++cn;
            field_expired = 1;
            continue;
        }
        exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[ii], NULL);
        if (ex_hash_val == NULL) {
            RedisModule_ReplyWithNull(ctx);
            ++cn;
        } else {
            RedisModule_ReplyWithString(ctx, ex_hash_val->value);
            ++cn;
        }
    }
    RedisModule_ReplySetArrayLength(ctx, cn);
    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired);
    return REDISMODULE_OK;
}

/* EXHMGETWITHVER key field [field ...] */
int TairHashTypeHmgetWithVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
        for (int ii = 2; ii < argc; ++ii) {
            RedisModule_ReplyWithNull(ctx);
        }
        RedisModule_ReplySetArrayLength(ctx, argc - 2);
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int cn = 0, field_expired = 0;
    int nokey;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (int ii = 2; ii < argc; ++ii) {
        if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[ii], 0)) {
            RedisModule_ReplyWithNull(ctx);
            ++cn;
            field_expired = 1;
            continue;
        }
        exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[ii], &nokey);
        if (nokey || !ex_hash_val) {
            RedisModule_ReplyWithNull(ctx);
            ++cn;
        } else {
            RedisModule_ReplyWithArray(ctx, 2);
            RedisModule_ReplyWithString(ctx, ex_hash_val->value);
            RedisModule_ReplyWithLongLong(ctx, ex_hash_val->version);
            ++cn;
        }
    }
    RedisModule_ReplySetArrayLength(ctx, cn);
    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired);

    return REDISMODULE_OK;
}

/* EXHDEL <key> <field> <field> <field> ...*/
int TairHashTypeHdel_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    static int i = 0;
    i++;

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    long long j, deleted = 0;
    exHashVal *ex_hash_oldval = NULL;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int field_expired = 0;
    for (j = 2; j < argc; j++) {
        /* Internal will perform RedisModule_Replicate EXHDEL for replication */
        field_expired = expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[j], 0);
        if (RedisModule_DictDel(ex_hash_obj->hash, argv[j], &ex_hash_oldval) == REDISMODULE_OK) {
            exHashValRelease(ex_hash_oldval);
            RedisModule_Replicate(ctx, "EXHDEL", "ss", argv[1], argv[j]);
            deleted++;
        }
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired + deleted);
    RedisModule_ReplyWithLongLong(ctx, deleted);
    return REDISMODULE_OK;
}

/* EXHDELWITHVER <key> <field> version> <field> <version> ...*/
int TairHashTypeHdelWithVer_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    static int i = 0;
    i++;

    if (argc < 4 || ((argc - 2) % 2) != 0) {
        return RedisModule_WrongArity(ctx);
    }

    long long j, deleted = 0;
    exHashVal *ex_hash_oldval = NULL;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    long long ver;
    int nokey, field_expired;

    for (j = 2; j < argc; j += 2) {
        if (RedisModule_StringToLongLong(argv[j + 1], &ver) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        /* Internal will perform RedisModule_Replicate EXHDEL for replication */
        if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[j], 0)) {
            field_expired = 1;
        }
        exHashVal *ex_hash_val = RedisModule_DictGet(ex_hash_obj->hash, argv[j], &nokey);
        if (!nokey && ex_hash_val && (ver == 0 || ver == ex_hash_val->version)) {
            if (RedisModule_DictDel(ex_hash_obj->hash, argv[j], &ex_hash_oldval) == REDISMODULE_OK) {
                exHashValRelease(ex_hash_oldval);
                RedisModule_Replicate(ctx, "EXHDEL", "ss", argv[1], argv[j]);
                deleted++;
            }
        }
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired + deleted);
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
        if (!rsStrcasecmp(argv[2], "noexp")) {
            noexp = 1;
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    if (noexp) {
        void *data;
        char *skey;
        size_t skeylen;
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(ex_hash_obj->hash, "^", NULL, 0);
        while ((skey = RedisModule_DictNextC(iter, &skeylen, &data)) != NULL) {
            if (isExpire(((exHashVal *)data)->expire)) {
                continue;
            }
            len++;
        }
        RedisModule_DictIteratorStop(iter);
    } else {
        len = RedisModule_DictSize(ex_hash_obj->hash);
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

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int field_expired = 0;
    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expired = 1;
    }

    int nokey = 0;
    RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (field_expired || nokey) {
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        RedisModule_ReplyWithLongLong(ctx, 1);
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired);
    return REDISMODULE_OK;
}

/* EXHSTRLEN key field */
int TairHashTypeHstrlen_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    int nokey = 0, field_expired = 0;
    size_t len = 0;
    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expired = 1;
    }
    exHashVal *val = RedisModule_DictGet(ex_hash_obj->hash, argv[2], &nokey);
    if (field_expired || nokey || !val) {
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        RedisModule_StringPtrLen(val->value, &len);
        RedisModule_ReplyWithLongLong(ctx, len);
    }
    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired);

    return REDISMODULE_OK;
}

/* EXHKEYS key */
int TairHashTypeHkeys_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithArray(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    void *data;
    char *skey;
    size_t skeylen;
    uint64_t cn = 0;

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(ex_hash_obj->hash, "^", NULL, 0);
    while ((skey = RedisModule_DictNextC(iter, &skeylen, &data)) != NULL) {
        if (isExpire(((exHashVal *)data)->expire)) {
            continue;
        }
        RedisModule_ReplyWithStringBuffer(ctx, skey, skeylen);
        cn++;
    }
    RedisModule_DictIteratorStop(iter);

    RedisModule_ReplySetArrayLength(ctx, cn);
    return REDISMODULE_OK;
}

/* EXHVALS key */
int TairHashTypeHvals_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithArray(ctx, 0);
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    void *data;
    char *skey;
    size_t skeylen;
    uint64_t cn = 0;

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(ex_hash_obj->hash, "^", NULL, 0);
    while ((skey = RedisModule_DictNextC(iter, &skeylen, &data)) != NULL) {
        if (isExpire(((exHashVal *)data)->expire)) {
            continue;
        }
        RedisModule_ReplyWithString(ctx, ((exHashVal *)data)->value);
        cn++;
    }
    RedisModule_DictIteratorStop(iter);

    RedisModule_ReplySetArrayLength(ctx, cn);
    return REDISMODULE_OK;
}

/* EXHGETALL key */
int TairHashTypeHgetAll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    void *data;
    char *skey;
    size_t skeylen;
    uint64_t cn = 0;

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(ex_hash_obj->hash, "^", NULL, 0);
    while ((skey = RedisModule_DictNextC(iter, &skeylen, &data)) != NULL) {
        /* Since exhgetall is very easy to generate slow queries, we simply judge the
         timeout here instead of actually performing the delete operation and shorten
         the code execution time ASAP */
        if (isExpire(((exHashVal *)data)->expire)) {
            continue;
        }
        RedisModule_ReplyWithStringBuffer(ctx, skey, skeylen);
        cn++;
        RedisModule_ReplyWithString(ctx, ((exHashVal *)data)->value);
        cn++;
    }
    RedisModule_DictIteratorStop(iter);

    RedisModule_ReplySetArrayLength(ctx, cn);
    return REDISMODULE_OK;
}

/* EXHSCAN key op subkey [MATCH pattern] [COUNT count] [DIR left/right] [NOVAL] */
int TairHashTypeHscan_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    enum SCAN_DIR dir = RIGHT;

    const char *op = NULL;

    /* Used to locate the initial scan position */
    if (!rsStrcasecmp(argv[2], ">")) {
        op = ">";
    } else if (!rsStrcasecmp(argv[2], ">=")) {
        op = ">=";
    } else if (!rsStrcasecmp(argv[2], "<")) {
        op = "<";
    } else if (!rsStrcasecmp(argv[2], "<=")) {
        op = "<=";
    } else if (!rsStrcasecmp(argv[2], "^")) {
        op = "^";
    } else if (!rsStrcasecmp(argv[2], "$")) {
        op = "$";
    } else if (!rsStrcasecmp(argv[2], "==")) {
        op = "==";
    } else {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    /* Step 1: Parse options. */
    int j;
    RedisModuleString *pattern = NULL;
    long long count = EX_HASH_SCAN_DEFAULT_COUNT;
    int noval = 0;
    for (j = 4; j < argc; j++) {
        RedisModuleString *next = (j == argc - 1) ? NULL : argv[j + 1];
        if (!rsStrcasecmp(argv[j], "MATCH") && next) {
            pattern = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "COUNT") && next) {
            if (RedisModule_StringToLongLong(next, &count) == REDISMODULE_ERR) {
                RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
                return REDISMODULE_ERR;
            }
            j++;
        } else if ((!rsStrcasecmp(argv[j], "DIR") && next)) {
            if (!rsStrcasecmp(next, "left")) {
                dir = LEFT;
            } else if (!rsStrcasecmp(next, "right")) {
                dir = RIGHT;
            } else {
                RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
                return REDISMODULE_ERR;
            }
            j++;
        } else if (!rsStrcasecmp(argv[j], "NOVAL")) {
            noval = 1;
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithSimpleString(ctx, "");
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    } else {
        if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
            return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    /* Step 2: Iterate the collection.*/
    void *data;
    RedisModuleString *skey = NULL;
    long long cn = 0;

    list *tmp_scan = listCreate();

    RedisModuleDictIter *iter = RedisModule_DictIteratorStart(ex_hash_obj->hash, op, argv[3]);
    if (dir == RIGHT) {
        while ((skey = RedisModule_DictNext(ctx, iter, &data)) != NULL && (cn++ < count)) {
            listAddNodeTail(tmp_scan, skey);
            listAddNodeTail(tmp_scan, ((exHashVal *)data)->value);
        }
    } else if (dir == LEFT) {
        while ((skey = RedisModule_DictPrev(ctx, iter, &data)) != NULL && (cn++ < count)) {
            listAddNodeTail(tmp_scan, skey);
            listAddNodeTail(tmp_scan, ((exHashVal *)data)->value);
        }
    }
    RedisModule_DictIteratorStop(iter);

    RedisModuleString *cursor_key = skey != NULL ? RedisModule_CreateStringFromString(NULL, skey) : NULL;

    /* Step 3: Filter elements. */
    listNode *node, *nextnode;
    node = listFirst(tmp_scan);
    int field_expired = 0;
    while (node) {
        RedisModuleString *skey = listNodeValue(node);
        nextnode = listNextNode(node);
        int filter = 0;

        if (!filter && pattern) {
            if (!mstrmatchlen(pattern, skey, 0)) filter = 1;
        }

        if (!filter && expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, skey, 0)) {
            filter = 1;
            field_expired = 1;
        }

        if (filter) {
            listDelNode(tmp_scan, node);
        }

        node = nextnode;
        nextnode = listNextNode(node);
        if (filter) {
            listDelNode(tmp_scan, node);
        }

        node = nextnode;
    }

    /* Step 4: Reply to the client. */
    RedisModule_ReplyWithArray(ctx, 2);
    if (cursor_key == NULL) {
        RedisModule_ReplyWithSimpleString(ctx, "");
    } else {
        RedisModule_ReplyWithString(ctx, cursor_key);
        RedisModule_FreeString(NULL, cursor_key);
    }

    if (noval) {
        RedisModule_ReplyWithArray(ctx, 0);
    } else {
        RedisModule_ReplyWithArray(ctx, listLength(tmp_scan));
        while ((node = listFirst(tmp_scan)) != NULL) {
            RedisModuleString *skey = listNodeValue(node);
            RedisModule_ReplyWithString(ctx, skey);
            listDelNode(tmp_scan, node);
        }
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj->hash, field_expired);
    listRelease(tmp_scan);
    return REDISMODULE_OK;
}

/* exh_expire_info */
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
        "enable_active_expire:%d\r\n"
        "ex_hash_active_expire_period:%d\r\n"
        "ex_hash_active_expire_keys_per_loop:%d\r\n"
        "ex_hash_active_expire_dbs_per_loop:%d\r\n"
        "ex_hash_active_expire_last_time_msec:%lld\r\n"
        "ex_hash_active_expire_max_time_msec:%lld\r\n"
        "ex_hash_active_expire_avg_time_msec:%lld\r\n"
        "ex_hash_passive_expire_keys_per_loop:%lld\r\n",
        enable_active_expire, ex_hash_active_expire_period, ex_hash_active_expire_keys_per_loop,
        ex_hash_active_expire_dbs_per_loop, stat_last_active_expire_time_msec, stat_max_active_expire_time_msec,
        stat_avg_active_expire_time_msec, (long long)ex_hash_passive_expire_keys_per_loop);

    size_t a_len, d_len, t_size = 0;
    const char *a_buf = RedisModule_StringPtrLen(info_a, &a_len);
    char buf[1024 * 1024] = {0};

    strncat(buf, a_buf, a_len);
    t_size += a_len;

#define DB_DETAIL "\r\n# DB detail statistics\r\n"
    d_len = strlen(DB_DETAIL);
    strncat(buf, DB_DETAIL, d_len);
    t_size += d_len;

    int i;
    for (i = 0; i < DB_NUM; ++i) {
        if (timer_heap[i].nelts == 0 && stat_expired_field[i] == 0) {
            continue;
        }
        RedisModuleString *info_d
            = RedisModule_CreateStringPrintf(ctx, "db: %d, unexpired_fields: %d, active_expired_fields: %ld\r\n", i,
                                             timer_heap[i].nelts, (long)stat_expired_field[i]);
        const char *d_buf = RedisModule_StringPtrLen(info_d, &d_len);
        strncat(buf, d_buf, d_len);
        RedisModule_FreeString(ctx, info_d);
        t_size += d_len;
    }

    RedisModule_ReplyWithStringBuffer(ctx, buf, t_size);
    RedisModule_FreeString(ctx, info_a);
    return REDISMODULE_OK;
}

/* ========================== "exhashtype" type methods ======================= */

void *TairHashTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    REDISMODULE_NOT_USED(encver);

    exHashObj *o = createExhashTypeObject();
    o->hash = RedisModule_CreateDict(NULL);
    uint64_t len = RedisModule_LoadUnsigned(rdb);
    o->key = RedisModule_LoadString(rdb);
    o->dbid = RedisModule_LoadUnsigned(rdb);

    RedisModuleString *skey;
    long long version, expire;
    RedisModuleString *value;

    while (len-- > 0) {
        skey = RedisModule_LoadString(rdb);
        version = RedisModule_LoadUnsigned(rdb);
        expire = RedisModule_LoadUnsigned(rdb);
        value = RedisModule_LoadString(rdb);
        if (isExpire(expire)) {
            RedisModule_FreeString(NULL, skey);
            /* For field that has expired, we do not load it into DB */
            continue;
        }
        exHashVal *hashv = createExhashVal();
        hashv->version = version;
        hashv->expire = expire;
        hashv->value = value;
        RedisModule_DictSet(o->hash, skey, hashv);
        if (hashv->expire > 0) {
            if (enable_active_expire) {
                struct expire_node *node
                    = createExpireNode(RedisModule_CreateStringFromString(NULL, o->key), skey, hashv->expire, o->dbid);
                heap_insert(&timer_heap[o->dbid], &node->inner_node, expire_node_compare);
                continue;
            }
        }
        RedisModule_FreeString(NULL, skey);
    }
    return o;
}

void TairHashTypeRdbSave(RedisModuleIO *rdb, void *value) {
    exHashObj *o = (exHashObj *)value;

    void *data;
    char *skey;
    size_t skeylen;

    if (o->hash) {
        RedisModuleDict *tmp_dict = RedisModule_CreateDict(NULL);
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(o->hash, "^", NULL, 0);
        while ((skey = RedisModule_DictNextC(iter, &skeylen, &data)) != NULL) {
            exHashVal *val = (exHashVal *)data;
            if (isExpire(val->expire)) {
                /* For field that has expired, we do not save it in an RDB file */
                continue;
            }
            RedisModule_DictSetC(tmp_dict, skey, skeylen, data);
        }
        RedisModule_DictIteratorStop(iter);

        RedisModule_SaveUnsigned(rdb, RedisModule_DictSize(tmp_dict));
        RedisModule_SaveString(rdb, o->key);
        RedisModule_SaveUnsigned(rdb, o->dbid);

        iter = RedisModule_DictIteratorStartC(tmp_dict, "^", NULL, 0);
        while ((skey = RedisModule_DictNextC(iter, &skeylen, &data)) != NULL) {
            exHashVal *val = (exHashVal *)data;
            RedisModule_SaveStringBuffer(rdb, skey, skeylen);
            RedisModule_SaveUnsigned(rdb, val->version);
            RedisModule_SaveUnsigned(rdb, val->expire);
            RedisModule_SaveString(rdb, val->value);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(NULL, tmp_dict);
    }
}

void TairHashTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    exHashObj *o = (exHashObj *)value;
    void *data;
    char *skey;
    size_t skeylen;
    if (o->hash) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(o->hash, "^", NULL, 0);
        while ((skey = RedisModule_DictNextC(iter, &skeylen, &data)) != NULL) {
            exHashVal *val = (exHashVal *)data;
            if (val->expire) {
                if (isExpire(val->expire)) {
                    /* For expired field, we do not REWRITE it in the AOF file */
                    continue;
                }
                RedisModule_EmitAOF(aof, "EXHSET", "sbsclcl", key, skey, skeylen, val->value, "PXAT", val->expire,
                                    "ABS", val->version);
            } else {
                RedisModule_EmitAOF(aof, "EXHSET", "sbscl", key, skey, skeylen, val->value, "ABS", val->version);
            }
        }
        RedisModule_DictIteratorStop(iter);
    }
}

size_t TairHashTypeMemUsage(const void *value) {
    exHashObj *o = (exHashObj *)value;

    uint64_t size = 0;
    void *data;
    char *skey;
    size_t skeylen;

    if (!o) {
        return size;
    }

    if (o->hash) {
        size += sizeof(*o);

        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(o->hash, "^", NULL, 0);
        while ((skey = RedisModule_DictNextC(iter, &skeylen, &data)) != NULL) {
            exHashVal *val = (exHashVal *)data;
            size += sizeof(*val);
            size += skeylen;
            size_t len;
            RedisModule_StringPtrLen(val->value, &len);
            size += len;
        }
        RedisModule_DictIteratorStop(iter);
    }

    return size;
}

void TairHashTypeFree(void *value) {
    if (value) exHashTypeReleaseObject(value);
}

void TairHashTypeDigest(RedisModuleDigest *md, void *value) {
    exHashObj *o = (exHashObj *)value;

    void *data;
    char *skey;
    size_t skeylen;

    if (!o) {
        return;
    }

    /* TODO: Need to exclude expired fields? */
    if (o->hash) {
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(o->hash, "^", NULL, 0);
        while ((skey = RedisModule_DictNextC(iter, &skeylen, &data)) != NULL) {
            exHashVal *val = (exHashVal *)data;
            size_t val_len;
            const char *val_ptr = RedisModule_StringPtrLen(val->value, &val_len);
            RedisModule_DigestAddStringBuffer(md, (unsigned char *)skey, skeylen);
            RedisModule_DigestAddStringBuffer(md, (unsigned char *)val_ptr, val_len);
        }
        RedisModule_DictIteratorStop(iter);
    }
    RedisModule_DigestEndSequence(md);
}

int Module_CreateCommands(RedisModuleCtx *ctx) {
#define CREATE_CMD(name, tgt, attr)                                                       \
    do {                                                                                  \
        if (RedisModule_CreateCommand(ctx, name, tgt, attr, 1, 1, 1) != REDISMODULE_OK) { \
            return REDISMODULE_ERR;                                                       \
        }                                                                                 \
    } while (0);

#define CREATE_WRCMD(name, tgt) CREATE_CMD(name, tgt, "write deny-oom")
#define CREATE_ROCMD(name, tgt) CREATE_CMD(name, tgt, "readonly fast")

    CREATE_WRCMD("exhset", TairHashTypeHset_RedisCommand)
    CREATE_ROCMD("exhget", TairHashTypeHget_RedisCommand)
    CREATE_WRCMD("exhdel", TairHashTypeHdel_RedisCommand)
    CREATE_WRCMD("exhdelwithver", TairHashTypeHdelWithVer_RedisCommand)
    CREATE_ROCMD("exhlen", TairHashTypeHlen_RedisCommand)
    CREATE_ROCMD("exhexists", TairHashTypeHexists_RedisCommand)
    CREATE_ROCMD("exhstrlen", TairHashTypeHstrlen_RedisCommand)
    CREATE_WRCMD("exhincrby", TairHashTypeHincrBy_RedisCommand)
    CREATE_WRCMD("exhincrbyfloat", TairHashTypeHincrByFloat_RedisCommand)
    CREATE_ROCMD("exhkeys", TairHashTypeHkeys_RedisCommand)
    CREATE_ROCMD("exhvals", TairHashTypeHvals_RedisCommand)
    CREATE_ROCMD("exhgetall", TairHashTypeHgetAll_RedisCommand)
    CREATE_ROCMD("exhmget", TairHashTypeHmget_RedisCommand)
    CREATE_ROCMD("exhmgetwithver", TairHashTypeHmgetWithVer_RedisCommand)
    CREATE_ROCMD("exhscan", TairHashTypeHscan_RedisCommand)
    CREATE_WRCMD("exhsetnx", TairHashTypeHsetNx_RedisCommand)
    CREATE_WRCMD("exhmset", TairHashTypeHmset_RedisCommand)
    CREATE_WRCMD("exhmsetwithopts", TairHashTypeHmsetWithOpts_RedisCommand)
    CREATE_ROCMD("exhver", TairHashTypeHver_RedisCommand)
    CREATE_ROCMD("exhttl", TairHashTypeHttl_RedisCommand)
    CREATE_ROCMD("exhpttl", TairHashTypeHpttl_RedisCommand)
    CREATE_WRCMD("exhsetver", TairHashTypeHsetVer_RedisCommand)
    CREATE_WRCMD("exhexpire", TairHashTypeHexpire_RedisCommand)
    CREATE_WRCMD("exhexpireat", TairHashTypeHexpireAt_RedisCommand)
    CREATE_WRCMD("exhpexpire", TairHashTypeHpexpire_RedisCommand)
    CREATE_WRCMD("exhpexpireat", TairHashTypeHpexpireAt_RedisCommand)
    CREATE_ROCMD("exhgetwithver", TairHashTypeHgetWithVer_RedisCommand)
    CREATE_ROCMD("exhexpireinfo", TairHashTypeActiveExpireInfo_RedisCommand)

    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "exhash---", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (argc % 2) {
        RedisModule_Log(ctx, "warning", "Invalid number of arguments passed");
        return REDISMODULE_ERR;
    }

    for (int ii = 0; ii < argc; ii += 2) {
        if (!rsStrcasecmp(argv[ii], "enable_active_expire")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR) {
                RedisModule_Log(ctx, "warning", "Invalid argument for enable_active_expire");
                return REDISMODULE_ERR;
            }
            enable_active_expire = v;
        } else if (!rsStrcasecmp(argv[ii], "active_expire_period")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR) {
                RedisModule_Log(ctx, "warning", "Invalid argument for active_expire_period");
                return REDISMODULE_ERR;
            }
            ex_hash_active_expire_period = v;
        } else if (!rsStrcasecmp(argv[ii], "active_expire_keys_per_loop")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR) {
                RedisModule_Log(ctx, "warning", "Invalid argument for active_expire_keys_per_loop");
                return REDISMODULE_ERR;
            }
            ex_hash_active_expire_keys_per_loop = v;
        } else if (!rsStrcasecmp(argv[ii], "active_expire_dbs_per_loop")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR) {
                RedisModule_Log(ctx, "warning", "Invalid argument for active_expire_dbs_per_loop");
                return REDISMODULE_ERR;
            }
            ex_hash_active_expire_dbs_per_loop = v;
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
        .mem_usage = TairHashTypeMemUsage,
        .free = TairHashTypeFree,
        .digest = TairHashTypeDigest,
    };

    TairHashType = RedisModule_CreateDataType(ctx, "exhash---", 0, &tm);
    if (TairHashType == NULL) return REDISMODULE_ERR;

    if (REDISMODULE_ERR == Module_CreateCommands(ctx)) {
        return REDISMODULE_ERR;
    }

    if (enable_active_expire) {
        for (int i = 0; i < DB_NUM; i++) {
            heap_init(&timer_heap[i]);
        }
        RedisModuleCtx *ctx2 = RedisModule_GetThreadSafeContext(NULL);
        startExpireTimer(ctx2, NULL);
        RedisModule_FreeThreadSafeContext(ctx2);
    }

    return REDISMODULE_OK;
}
