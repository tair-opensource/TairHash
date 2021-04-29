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

#include "dict.h"
#include "list.h"
#include "redismodule.h"
#include "skiplist.h"
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

#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

#define EX_HASH_ACTIVE_EXPIRE_PERIOD 1000
#define EX_HASH_ACTIVE_EXPIRE_KEYS_PER_LOOP 1000

#define EX_HASH_SCAN_DEFAULT_COUNT 10

#define DB_NUM 16
#define DBS_PER_CALL 16
#define KEYS_PER_LOOP 3

static RedisModuleTimerID expire_timer_id = 0;
static m_zskiplist *zsl[DB_NUM];

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

inline RedisModuleString *takeAndRef(RedisModuleString *str) {
    RedisModule_RetainString(NULL, str);
    return str;
}

#define ACTIVE_EXPIRE_INSERT(dbid, key, field, expire)              \
    if (enable_active_expire) {                                     \
        if (expire) {                                               \
            m_zslInsert(zsl[dbid], expire, key, takeAndRef(field)); \
        }                                                           \
    }

#define ACTIVE_EXPIRE_UPDATE(dbid, key, field, cur_expire, new_expire)       \
    if (enable_active_expire) {                                              \
        if (cur_expire != new_expire) {                                      \
            m_zslUpdateScore(zsl[dbid], cur_expire, key, field, new_expire); \
        }                                                                    \
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
        if (o->value) {
            RedisModule_FreeString(NULL, o->value);
        }
        RedisModule_Free(o);
    }
}
typedef struct exHashObj {
    dict *hash;
    unsigned int dbid;
    RedisModuleString *key;
} exHashObj;

static struct exHashObj *createTairHashTypeObject() {
    exHashObj *o = RedisModule_Calloc(1, sizeof(*o));
    return o;
}

void tairhashScanCallback(void *privdata, const m_dictEntry *de) {
    list *keys = (list *)privdata;
    RedisModuleString *key, *val = NULL;

    RedisModuleString *skey = dictGetKey(de);
    exHashVal *sval = dictGetVal(de);
    key = skey;
    if (sval) {
        val = sval->value;
    }
    m_listAddNodeTail(keys, key);
    if (val) {
        m_listAddNodeTail(keys, val);
    }
}

uint64_t dictModuleStrHash(const void *key) {
    size_t len;
    const char *buf = RedisModule_StringPtrLen(key, &len);
    return m_dictGenHashFunction(buf, (int)len);
}

int dictModuleStrKeyCompare(void *privdata, const void *key1,
                            const void *key2) {
    size_t l1, l2;
    DICT_NOTUSED(privdata);

    const char *buf1 = RedisModule_StringPtrLen(key1, &l1);
    const char *buf2 = RedisModule_StringPtrLen(key2, &l2);
    if (l1 != l2) return 0;
    return memcmp(buf1, buf2, l1) == 0;
}

void dictModuleKeyDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);
    if (val) {
        RedisModule_FreeString(NULL, val);
    }
}

void dictModuleValueDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);
    if (val) {
        exHashVal *v = (exHashVal *)val;
        exHashValRelease(v);
    }
}

m_dictType tairhashDictType = {
    dictModuleStrHash,        /* hash function */
    NULL,                     /* key dup */
    NULL,                     /* val dup */
    dictModuleStrKeyCompare,  /* key compare */
    dictModuleKeyDestructor,  /* key destructor */
    dictModuleValueDestructor /* val destructor */
};

static void exHashTypeReleaseObject(struct exHashObj *o) {
    m_zslDeleteWholeKey(zsl[o->dbid], o->key);
    m_dictRelease(o->hash);
    RedisModule_FreeString(NULL, o->key);
    RedisModule_Free(o);
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
                                            RedisModuleString *field, int is_timer) {
    exHashVal *ex_hash_val = m_dictFetchValue(o->hash, field);
    if (ex_hash_val == NULL) {
        return 0;
    }

    long long when = ex_hash_val->expire;
    long long now;

    now = mstime();

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
        m_zslDelete(zsl[o->dbid], when, key_dup, field_dup, NULL);
    }
    m_dictDelete(o->hash, field);
    RedisModule_Replicate(ctx, "EXHDEL", "ss", key_dup, field_dup);
    RedisModule_FreeString(NULL, key_dup);
    RedisModule_FreeString(NULL, field_dup);
    return 1;
}

int delEmptyExhashIfNeeded(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *raw_key, exHashObj *obj) {
    if (!obj || RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE || dictSize(obj->hash) != 0) {
        return 0;
    }

    RedisModule_DeleteKey(key);
    RedisModule_Replicate(ctx, "DEL", "s", raw_key);
    return 1;
}

inline static int isExpire(long long when) {
    /* No expire */
    if (when == 0) {
        return 0;
    }

    mstime_t now = mstime();
    return now > when;
}

/* Active expire algorithm implementiation */
void activeExpireTimerHandler(RedisModuleCtx *ctx, void *data) {
    REDISMODULE_NOT_USED(data);
    RedisModule_AutoMemory(ctx);

    /* Slave do not exe active expire. */
    if (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE) {
        goto restart;
    }

    int i = 0;
    static unsigned int current_db = 0;
    static unsigned long long loop_cnt = 0, total_expire_time = 0;

    exHashObj *ex_hash_obj = NULL;

    int dbs_per_call = ex_hash_active_expire_dbs_per_loop;
    if (dbs_per_call > DB_NUM)
        dbs_per_call = DB_NUM;

    int start_index;
    m_zskiplistNode *ln = NULL;

    RedisModuleString *key, *field;
    RedisModuleKey *real_key;

    long long start = mstime();
    for (; i < dbs_per_call; ++i) {
        int range_len = ex_hash_active_expire_keys_per_loop;
        unsigned long zsl_len = zsl[current_db % DB_NUM]->length;
        range_len = range_len > zsl_len ? zsl_len : range_len;
        if (range_len == 0) {
            current_db++;
            continue;
        }

        RedisModule_SelectDb(ctx, current_db % DB_NUM);
        ln = zsl[current_db % DB_NUM]->header->level[0].forward;
        start_index = 0;
        while (range_len--) {
            key = ln->key;
            field = ln->field;

            real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ | REDISMODULE_WRITE);
            int type = RedisModule_KeyType(real_key);
            assert(type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(real_key) == TairHashType);

            ex_hash_obj = RedisModule_ModuleTypeGetValue(real_key);
            if (expireTairHashObjIfNeeded(ctx, key, ex_hash_obj, field, 1)) {
                stat_expired_field[current_db % DB_NUM]++;
                start_index++;
            } else {
                break;
            }
            ln = ln->level[0].forward;
        }

        if (start_index) {
            m_zslDeleteRangeByRank(zsl[current_db % DB_NUM], 1, start_index);
        }
        
        delEmptyExhashIfNeeded(ctx, real_key, key, ex_hash_obj);

        current_db++;
    }

    stat_last_active_expire_time_msec = mstime() - start;
    stat_max_active_expire_time_msec = stat_max_active_expire_time_msec < stat_last_active_expire_time_msec ? stat_last_active_expire_time_msec : stat_max_active_expire_time_msec;
    total_expire_time += stat_last_active_expire_time_msec;
    ++loop_cnt;
    if (loop_cnt % 1000 == 0) {
        stat_avg_active_expire_time_msec = total_expire_time / loop_cnt;
        loop_cnt = 0;
        total_expire_time = 0;
    }

restart:
    /* Since the module that exports the data type must not be uninstalled, the timer can always be run */
    if (enable_active_expire)
        expire_timer_id = RedisModule_CreateTimer(ctx, ex_hash_active_expire_period, activeExpireTimerHandler, NULL);
}

void startExpireTimer(RedisModuleCtx *ctx, void *data) {
    if (!enable_active_expire)
        return;

    if (RedisModule_GetTimerInfo(ctx, expire_timer_id, NULL, NULL) == REDISMODULE_OK) {
        return;
    }

    expire_timer_id = RedisModule_CreateTimer(ctx, ex_hash_active_expire_period, activeExpireTimerHandler, data);
}

inline static void latencySensitivePassiveExpire(RedisModuleCtx *ctx, unsigned int dbid) {
    exHashObj *ex_hash_obj = NULL;

    int keys_per_loop = ex_hash_passive_expire_keys_per_loop;

    int start_index = 0;
    m_zskiplistNode *ln = NULL;

    RedisModuleString *key, *field;

    unsigned long zsl_len = zsl[dbid]->length;
    keys_per_loop = keys_per_loop > zsl_len ? zsl_len : keys_per_loop;

    ln = zsl[dbid]->header->level[0].forward;
    while (keys_per_loop--) {
        key = ln->key;
        field = ln->field;

        RedisModuleKey *real_key = RedisModule_OpenKey(ctx, key, REDISMODULE_READ | REDISMODULE_WRITE);
        int type = RedisModule_KeyType(real_key);
        assert(type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(real_key) == TairHashType);

        ex_hash_obj = RedisModule_ModuleTypeGetValue(real_key);
        if (expireTairHashObjIfNeeded(ctx, key, ex_hash_obj, field, 1)) {
            start_index++;
        } else {
            break;
        }
        ln = ln->level[0].forward;
    }

    if (start_index) {
        m_zslDeleteRangeByRank(zsl[dbid], 1, start_index);
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
    long long version = 0;
    int field_expired = 0;
    int nokey;

    if (RedisModule_StringToLongLong(argv[3], &milliseconds) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    // TODO: expire <= 0

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
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModuleString *skey = argv[2], *pkey = argv[1];

    if (expireTairHashObjIfNeeded(ctx, pkey, ex_hash_obj, skey, 0)) {
        field_expired = 1;
    }

    exHashVal *ex_hash_val = NULL;
    m_dictEntry *de = m_dictFind(ex_hash_obj->hash, skey);
    if (field_expired || de == NULL) {
        nokey = 1;
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        nokey = 0;
        skey = dictGetKey(de);
        ex_hash_val = dictGetVal(de);
        if (ex_flags & EX_HASH_SET_WITH_VER && version != 0 && version != ex_hash_val->version) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_VERSION);
            return REDISMODULE_ERR;
        }

        if (unit == UNIT_SECONDS) {
            milliseconds *= 1000;
        }

        milliseconds += basetime;
        if (milliseconds > 0) {
            if (nokey || ex_hash_val->expire == 0) {
                ACTIVE_EXPIRE_INSERT(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, skey, milliseconds);
            } else {
                ACTIVE_EXPIRE_UPDATE(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, skey, ex_hash_val->expire, milliseconds)
            }
            ex_hash_val->expire = milliseconds;
        }

        RedisModule_ReplyWithLongLong(ctx, 1);
    }

    if (ex_flags & EX_HASH_SET_WITH_ABS_VER) {
        ex_hash_val->version = version;
    }

    RedisModule_ReplicateVerbatim(ctx);
    delEmptyExhashIfNeeded(ctx, key, pkey, ex_hash_obj);
    return REDISMODULE_OK;
}

int exhashTTLGenericFunc(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int unit) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

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

    RedisModuleString *pkey = argv[1], *skey = argv[2];

    if (expireTairHashObjIfNeeded(ctx, pkey, ex_hash_obj, skey, 0)) {
        field_expired = 1;
    }

    exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, skey);
    if (field_expired || ex_hash_val == NULL) {
        RedisModule_ReplyWithLongLong(ctx, -3);
    } else {
        if (ex_hash_val->expire == 0) {
            RedisModule_ReplyWithLongLong(ctx, -1);
        } else {
            long long ttl = ex_hash_val->expire - mstime();
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

    delEmptyExhashIfNeeded(ctx, key, pkey, ex_hash_obj);
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

/* ========================= "exhash" type commands ======================= */

/* EXHSET <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [NX|XX] [VER version | ABS version] */
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
        } else if (!rsStrcasecmp(argv[j], "ex") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
            ex_flags |= EX_HASH_SET_EX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "exat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
            ex_flags |= EX_HASH_SET_EX;
            ex_flags |= EX_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "px") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
            ex_flags |= EX_HASH_SET_PX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "pxat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
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
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (expire_p && expire <= 0) {
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

    latencySensitivePassiveExpire(ctx, RedisModule_GetSelectedDb(ctx));

    RedisModuleString *pkey = argv[1], *skey = argv[2];

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        if (ex_flags & EX_HASH_SET_XX) {
            RedisModule_ReplyWithLongLong(ctx, -1);
            return REDISMODULE_ERR;
        }
        ex_hash_obj = createTairHashTypeObject();
        ex_hash_obj->hash = m_dictCreate(&tairhashDictType, NULL);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, pkey);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    expireTairHashObjIfNeeded(ctx, pkey, ex_hash_obj, skey, 0);
    exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, skey);
    if (ex_hash_val == NULL) {
        if (ex_flags & EX_HASH_SET_XX) {
            RedisModule_ReplyWithLongLong(ctx, -1);
            return REDISMODULE_ERR;
        }
        nokey = 1;
        ex_hash_val = createExhashVal();
        ex_hash_val->version = 0;
        ex_hash_val->expire = 0;
        ex_hash_val->value = NULL;
    } else {
        nokey = 0;
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
    }

    if (milliseconds > 0) {
        if (nokey || ex_hash_val->expire == 0) {
            ACTIVE_EXPIRE_INSERT(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, skey, milliseconds);
        } else {
            ACTIVE_EXPIRE_UPDATE(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, skey, ex_hash_val->expire, milliseconds)
        }
        ex_hash_val->expire = milliseconds;
    }

    if (ex_hash_val->value) {
        RedisModule_FreeString(NULL, ex_hash_val->value);
    }

    ex_hash_val->value = takeAndRef(argv[3]);
    if (nokey) {
        m_dictAdd(ex_hash_obj->hash, takeAndRef(skey), ex_hash_val);
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

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    RedisModuleString *pkey = argv[1], *skey = argv[2], *svalue = argv[3];

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        ex_hash_obj = createTairHashTypeObject();
        ex_hash_obj->hash = m_dictCreate(&tairhashDictType, NULL);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, pkey);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, skey);
    if (ex_hash_val == NULL) {
        ex_hash_val = createExhashVal();
    } else {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    ex_hash_val->value = takeAndRef(svalue);
    m_dictAdd(ex_hash_obj->hash, takeAndRef(skey), ex_hash_val);

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
        ex_hash_obj = createTairHashTypeObject();
        ex_hash_obj->hash = m_dictCreate(&tairhashDictType, NULL);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    for (int i = 2; i < argc; i += 2) {
        expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[i], 0);
        exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, argv[i]);
        if (ex_hash_val == NULL) {
            nokey = 1;
            ex_hash_val = createExhashVal();
            if (ex_hash_val == NULL) {
                RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
                return REDISMODULE_ERR;
            }
        } else {
            if (ex_hash_val->value) {
                RedisModule_FreeString(NULL, ex_hash_val->value);
            }
        }
        ex_hash_val->value = takeAndRef(argv[i + 1]);
        ex_hash_val->version++;
        if (nokey) {
            m_dictAdd(ex_hash_obj->hash, takeAndRef(argv[i]), ex_hash_val);
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

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        ex_hash_obj = createTairHashTypeObject();
        ex_hash_obj->hash = m_dictCreate(&tairhashDictType, NULL);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    long long ver;
    long long when;

    int nokey;

    for (int i = 2; i < argc; i += 4) {
        if (RedisModule_StringToLongLong(argv[i + 3], &when) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        if (RedisModule_StringToLongLong(argv[i + 2], &ver) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        if (ver < 0 || when < 0) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[i], 0);
        exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, argv[i]);
        if (ex_hash_val == NULL || ver == 0 || ex_hash_val->version == ver) {
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

        exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, argv[i]);
        if (ex_hash_val == NULL) {
            ex_hash_val = createExhashVal();
            ex_hash_val->expire = 0;
            ex_hash_val->version = 0;
            ex_hash_val->value = NULL;
            nokey = 1;
        } else {
            nokey = 0;
        }

        if (ex_hash_val->value) {
            RedisModule_FreeString(NULL, ex_hash_val->value);
        }

        ex_hash_val->value = takeAndRef(argv[i + 1]);
        ex_hash_val->version++;
        if (when > 0) {
            when = mstime() + when * 1000;
            if (nokey || ex_hash_val->expire == 0) {
                ACTIVE_EXPIRE_INSERT(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, argv[i], when);
            } else {
                ACTIVE_EXPIRE_UPDATE(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, argv[i], ex_hash_val->expire, when)
            }
            ex_hash_val->expire = when;
        }

        if (nokey) {
            m_dictAdd(ex_hash_obj->hash, takeAndRef(argv[i]), ex_hash_val);
        }
    }

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/*  EXHPEXPIREAT <key> <field> <milliseconds-timestamp> [ VER version | ABS version ]*/
int TairHashTypeHpexpireAt_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return exhashExpireGenericFunc(ctx, argv, argc, 0, UNIT_MILLISECONDS);
}

/*  EXHPEXPIRE <key> <field> <milliseconds> [ VER version | ABS version ]*/
int TairHashTypeHpexpire_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return exhashExpireGenericFunc(ctx, argv, argc, mstime(), UNIT_MILLISECONDS);
}

/*  EXHEXPIREAT <key> <field> <timestamp> [ VER version | ABS version ] */
int TairHashTypeHexpireAt_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return exhashExpireGenericFunc(ctx, argv, argc, 0, UNIT_SECONDS);
}

/*  EXHEXPIRE <key> <field> <seconds> [ VER version | ABS version ] */
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
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expired = 1;
    }

    exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, argv[2]);
    if (field_expired || ex_hash_val == NULL) {
        RedisModule_ReplyWithLongLong(ctx, -2);
    } else {
        RedisModule_ReplyWithLongLong(ctx, ex_hash_val->version);
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);
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
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, argv[2]);
    if (ex_hash_val == NULL) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);
        RedisModule_ReplyWithLongLong(ctx, 0);
        return REDISMODULE_OK;
    }

    ex_hash_val->version = version;
    RedisModule_ReplyWithLongLong(ctx, 1);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* EXHINCRBY <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [VER version | ABS version] [MIN minval] [MAX maxval]  */
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
    int nokey;

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
        } else if (!rsStrcasecmp(argv[j], "exat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
            ex_flags |= EX_HASH_SET_EX;
            ex_flags |= EX_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "px") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
            ex_flags |= EX_HASH_SET_PX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "pxat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
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
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (expire_p && expire <= 0) {
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

    RedisModuleString *pkey = argv[1], *skey = argv[2];

    latencySensitivePassiveExpire(ctx, RedisModule_GetSelectedDb(ctx));

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        ex_hash_obj = createTairHashTypeObject();
        ex_hash_obj->hash = m_dictCreate(&tairhashDictType, NULL);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, pkey);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0);
    exHashVal *ex_hash_val = NULL;
    m_dictEntry *de = m_dictFind(ex_hash_obj->hash, skey);
    if (de == NULL) {
        nokey = 1;
        ex_hash_val = createExhashVal();
        ex_hash_val->expire = 0;
        ex_hash_val->version = 0;
    } else {
        nokey = 0;
        ex_hash_val = dictGetVal(de);
        skey = dictGetKey(de);
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

    if ((incr < 0 && cur_val < 0 && incr < (LLONG_MIN - cur_val)) || (incr > 0 && cur_val > 0 && incr > (LLONG_MAX - cur_val)) || (max_p != NULL && cur_val + incr > max) || (min_p != NULL && cur_val + incr < min)) {
        if (nokey) {
            exHashValRelease(ex_hash_val);
        }
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_OVERFLOW);
        return REDISMODULE_ERR;
    }

    ex_hash_val->version++;

    if (ex_flags & EX_HASH_SET_WITH_ABS_VER) {
        ex_hash_val->version = version;
    }

    cur_val += incr;

    if (ex_hash_val->value) {
        RedisModule_FreeString(NULL, ex_hash_val->value);
    }
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
    }

    if (milliseconds > 0) {
        if (nokey || ex_hash_val->expire == 0) {
            ACTIVE_EXPIRE_INSERT(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, skey, milliseconds);
        } else {
            ACTIVE_EXPIRE_UPDATE(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, skey, ex_hash_val->expire, milliseconds)
        }
        ex_hash_val->expire = milliseconds;
    }

    if (nokey) {
        m_dictAdd(ex_hash_obj->hash, takeAndRef(skey), ex_hash_val);
    }

    RedisModule_Replicate(ctx, "EXHSET", "sss", argv[1], argv[2], ex_hash_val->value);
    RedisModule_ReplyWithLongLong(ctx, cur_val);
    return REDISMODULE_OK;
}

/* EXHINCRBYFLOAT <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [VER version | ABS version] [MIN
 * minval] [MAX maxval] */
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
        } else if (!rsStrcasecmp(argv[j], "exat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
            ex_flags |= EX_HASH_SET_EX;
            ex_flags |= EX_HASH_SET_ABS_EXPIRE;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "px") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
            ex_flags |= EX_HASH_SET_PX;
            expire_p = next;
            j++;
        } else if (!rsStrcasecmp(argv[j], "pxat") && !(ex_flags & EX_HASH_SET_PX) && !(ex_flags & EX_HASH_SET_EX) && next) {
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
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    if ((NULL != expire_p) && (RedisModule_StringToLongLong(expire_p, &expire) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    if (expire_p && expire <= 0) {
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

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        ex_hash_obj = createTairHashTypeObject();
        ex_hash_obj->hash = m_dictCreate(&tairhashDictType, NULL);
        ex_hash_obj->dbid = RedisModule_GetSelectedDb(ctx);
        ex_hash_obj->key = RedisModule_CreateStringFromString(NULL, argv[1]);
        RedisModule_ModuleTypeSetValue(key, TairHashType, ex_hash_obj);
    } else {
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    if (ex_hash_obj == NULL) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_INTERNAL_ERR);
        return REDISMODULE_ERR;
    }

    RedisModuleString *skey = argv[2];

    expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0);
    m_dictEntry *de = m_dictFind(ex_hash_obj->hash, skey);
    exHashVal *ex_hash_val = NULL;
    if (de == NULL) {
        nokey = 1;
        ex_hash_val = createExhashVal();
        ex_hash_val->expire = 0;
        ex_hash_val->version = 0;
    } else {
        nokey = 0;
        ex_hash_val = dictGetVal(de);
        skey = dictGetKey(de);
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
        if (nokey) {
            exHashValRelease(ex_hash_val);
        }
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_OVERFLOW);
        return REDISMODULE_ERR;
    }

    if ((max_p != NULL && cur_val + incr > max) || (min_p != NULL && cur_val + incr < min)) {
        if (nokey)
            exHashValRelease(ex_hash_val);
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

    if (ex_hash_val->value)
        RedisModule_FreeString(NULL, ex_hash_val->value);
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
    }

    if (milliseconds > 0) {
        if (nokey || ex_hash_val->expire == 0) {
            ACTIVE_EXPIRE_INSERT(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, skey, milliseconds);
        } else {
            ACTIVE_EXPIRE_UPDATE(RedisModule_GetSelectedDb(ctx), ex_hash_obj->key, skey, ex_hash_val->expire, milliseconds)
        }
        ex_hash_val->expire = milliseconds;
    }

    if (nokey) {
        m_dictAdd(ex_hash_obj->hash, takeAndRef(skey), ex_hash_val);
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

    RedisModuleString *pkey = argv[1], *skey = argv[2];

    int field_expire = 0;
    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expire = 1;
    }

    exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, skey);
    if (field_expire || ex_hash_val == NULL) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        RedisModule_ReplyWithString(ctx, ex_hash_val->value);
    }

    delEmptyExhashIfNeeded(ctx, key, pkey, ex_hash_obj);
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

    int field_expired = 0;

    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expired = 1;
    }

    exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, argv[2]);
    if (field_expired || ex_hash_val == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithString(ctx, ex_hash_val->value);
        RedisModule_ReplyWithLongLong(ctx, ex_hash_val->version);
    }
    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);
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
        exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, argv[ii]);
        if (ex_hash_val == NULL) {
            RedisModule_ReplyWithNull(ctx);
            ++cn;
        } else {
            RedisModule_ReplyWithString(ctx, ex_hash_val->value);
            ++cn;
        }
    }
    RedisModule_ReplySetArrayLength(ctx, cn);
    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);
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
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (int ii = 2; ii < argc; ++ii) {
        if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[ii], 0)) {
            RedisModule_ReplyWithNull(ctx);
            ++cn;
            field_expired = 1;
            continue;
        }
        exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, argv[ii]);
        if (ex_hash_val == NULL) {
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
    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);

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
        ex_hash_obj = RedisModule_ModuleTypeGetValue(key);
    }

    exHashVal *ex_hash_val = NULL;
    for (j = 2; j < argc; j++) {
        /* Internal will perform RedisModule_Replicate EXHDEL for replication */
        expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[j], 0);
        m_dictEntry *de = m_dictFind(ex_hash_obj->hash, argv[j]);
        if (de) {
            ex_hash_val = dictGetVal(de);
            m_zslDelete(zsl[RedisModule_GetSelectedDb(ctx)], ex_hash_val->expire, argv[1], argv[j], NULL);
            m_dictDelete(ex_hash_obj->hash, argv[j]);
            RedisModule_Replicate(ctx, "EXHDEL", "ss", argv[1], argv[j]);
            deleted++;
        }
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);
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
    int field_expired;

    for (j = 2; j < argc; j += 2) {
        if (RedisModule_StringToLongLong(argv[j + 1], &ver) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }

        /* Internal will perform RedisModule_Replicate EXHDEL for replication */
        if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[j], 0)) {
            field_expired = 1;
        }
        exHashVal *ex_hash_val = (exHashVal *)m_dictFetchValue(ex_hash_obj->hash, argv[j]);
        if (ex_hash_val != NULL) {
            if (ver == 0 || ver == ex_hash_val->version) {
                if (m_dictDelete(ex_hash_obj->hash, argv[j]) == DICT_OK) {
                    RedisModule_Replicate(ctx, "EXHDEL", "ss", argv[1], argv[j]);
                    deleted++;
                }
            }
        }
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);
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

    m_dictIterator *di;
    m_dictEntry *de;

    if (noexp) {
        exHashVal *data;
        di = m_dictGetIterator(ex_hash_obj->hash);
        while ((de = m_dictNext(di)) != NULL) {
            data = (exHashVal *)dictGetVal(de);
            if (isExpire(data->expire)) {
                continue;
            }
            len++;
        }
        m_dictReleaseIterator(di);
    } else {
        len = dictSize(ex_hash_obj->hash);
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

    exHashVal *exhashval = m_dictFetchValue(ex_hash_obj->hash, argv[2]);
    if (field_expired || exhashval == NULL) {
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        RedisModule_ReplyWithLongLong(ctx, 1);
    }

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);
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

    int field_expired = 0;
    size_t len = 0;
    if (expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, argv[2], 0)) {
        field_expired = 1;
    }
    exHashVal *val = m_dictFetchValue(ex_hash_obj->hash, argv[2]);
    if (field_expired || !val) {
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        RedisModule_StringPtrLen(val->value, &len);
        RedisModule_ReplyWithLongLong(ctx, len);
    }
    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);

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

    exHashVal *data;
    RedisModuleString *skey;
    uint64_t cn = 0;

    m_dictIterator *di;
    m_dictEntry *de;

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    di = m_dictGetIterator(ex_hash_obj->hash);
    while ((de = m_dictNext(di)) != NULL) {
        skey = (RedisModuleString *)dictGetKey(de);
        data = (exHashVal *)dictGetVal(de);
        if (isExpire(data->expire)) {
            continue;
        }
        RedisModule_ReplyWithString(ctx, skey);
        cn++;
    }
    m_dictReleaseIterator(di);

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

    exHashVal *data;
    uint64_t cn = 0;

    m_dictIterator *di;
    m_dictEntry *de;

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    di = m_dictGetIterator(ex_hash_obj->hash);
    while ((de = m_dictNext(di)) != NULL) {
        data = (exHashVal *)dictGetVal(de);
        if (isExpire(((exHashVal *)data)->expire)) {
            continue;
        }
        RedisModule_ReplyWithString(ctx, data->value);
        cn++;
    }
    m_dictReleaseIterator(di);

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

    exHashVal *data;
    RedisModuleString *skey;
    uint64_t cn = 0;

    m_dictIterator *di;
    m_dictEntry *de;

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    di = m_dictGetIterator(ex_hash_obj->hash);
    while ((de = m_dictNext(di)) != NULL) {
        skey = (RedisModuleString *)dictGetKey(de);
        data = (exHashVal *)dictGetVal(de);
        /* Since exhgetall is very easy to generate slow queries, we simply judge the
         timeout here instead of actually performing the delete operation and shorten 
         the code execution time ASAP */
        if (isExpire(((exHashVal *)data)->expire)) {
            continue;
        }
        RedisModule_ReplyWithString(ctx, skey);
        cn++;
        RedisModule_ReplyWithString(ctx, data->value);
        cn++;
    }
    m_dictReleaseIterator(di);

    RedisModule_ReplySetArrayLength(ctx, cn);
    return REDISMODULE_OK;
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

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (REDISMODULE_KEYTYPE_EMPTY != type && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    unsigned long cursor;
    if (parseScanCursor(argv[2], &cursor) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }

    /* Step 1: Parse options. */
    int j;
    RedisModuleString *pattern = NULL;
    long long count = EX_HASH_SCAN_DEFAULT_COUNT;
    for (j = 3; j < argc; j++) {
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
        } else {
            RedisModule_ReplyWithError(ctx, EXHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }

    exHashObj *ex_hash_obj = NULL;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithLongLong(ctx, 0);
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
    long maxiterations = count * 10;
    list *keys = m_listCreate();

    do {
        cursor = m_dictScan(ex_hash_obj->hash, cursor, tairhashScanCallback, NULL, keys);
    } while (cursor && maxiterations-- && listLength(keys) < (unsigned long)count);

    m_listNode *node, *nextnode;
    node = listFirst(keys);

    /* Step 3: Filter elements. */
    while (node) {
        RedisModuleString *skey = listNodeValue(node);
        nextnode = listNextNode(node);
        int filter = 0;

        /* Filter element if it does not match the pattern. */
        if (!filter && pattern) {
            if (!mstrmatchlen(pattern, skey, 0))
                filter = 1;
        }

        /* Filter element if it is an expired key. */
        if (!filter && expireTairHashObjIfNeeded(ctx, argv[1], ex_hash_obj, skey, 0)) {
            filter = 1;
        }

        /* Remove the element and its associted value if needed. */
        if (filter) {
            m_listDelNode(keys, node);
        }

        node = nextnode;
        nextnode = listNextNode(node);
        if (filter) {
            m_listDelNode(keys, node);
        }

        node = nextnode;
    }

    /* Step 4: Reply to the client. */
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, cursor);

    RedisModule_ReplyWithArray(ctx, listLength(keys));
    while ((node = listFirst(keys)) != NULL) {
        RedisModuleString *skey = listNodeValue(node);
        RedisModule_ReplyWithString(ctx, skey);
        m_listDelNode(keys, node);
    }

    m_listRelease(keys);

    delEmptyExhashIfNeeded(ctx, key, argv[1], ex_hash_obj);

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
        if (zsl[i]->length == 0 && stat_expired_field[i] == 0) {
            continue;
        }
        RedisModuleString *info_d = RedisModule_CreateStringPrintf(ctx, "db: %d, unexpired_fields: %ld, active_expired_fields: %ld\r\n", i,
                                                                   zsl[i]->length, (long)stat_expired_field[i]);
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

    exHashObj *o = createTairHashTypeObject();
    o->hash = m_dictCreate(&tairhashDictType, NULL);
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
            RedisModule_FreeString(NULL, value);
            /* For field that has expired, we do not load it into DB */
            continue;
        }
        exHashVal *hashv = createExhashVal();
        hashv->version = version;
        hashv->expire = expire;
        hashv->value = takeAndRef(value);
        m_dictAdd(o->hash, takeAndRef(skey), hashv);
        if (hashv->expire > 0) {
            if (enable_active_expire) {
                ACTIVE_EXPIRE_INSERT(o->dbid, o->key, skey, hashv->expire);
            }
        }
        RedisModule_FreeString(NULL, value);
        RedisModule_FreeString(NULL, skey);
    }
    return o;
}

void TairHashTypeRdbSave(RedisModuleIO *rdb, void *value) {
    exHashObj *o = (exHashObj *)value;
    RedisModuleString *skey;

    m_listNode *node, *nextnode;
    m_dictIterator *di;
    m_dictEntry *de;

    uint64_t cnt = 0;

    if (o->hash) {
        list *tmp_hash = m_listCreate();
        di = m_dictGetIterator(o->hash);
        while ((de = m_dictNext(di)) != NULL) {
            if (isExpire(((exHashVal *)dictGetVal(de))->expire)) {
                /* For field that has expired, we do not save it in an RDB file */
                continue;
            }
            m_listAddNodeTail(tmp_hash, dictGetKey(de));
            m_listAddNodeTail(tmp_hash, dictGetVal(de));
            cnt++;
        }
        m_dictReleaseIterator(di);

        RedisModule_SaveUnsigned(rdb, cnt);
        RedisModule_SaveString(rdb, o->key);
        RedisModule_SaveUnsigned(rdb, o->dbid);

        node = listFirst(tmp_hash);
        while (node) {
            skey = (RedisModuleString *)listNodeValue(node);
            nextnode = listNextNode(node);
            exHashVal *val = (exHashVal *)listNodeValue(nextnode);
            RedisModule_SaveString(rdb, skey);
            RedisModule_SaveUnsigned(rdb, val->version);
            RedisModule_SaveUnsigned(rdb, val->expire);
            RedisModule_SaveString(rdb, val->value);
            node = listNextNode(nextnode);
        }
        m_listRelease(tmp_hash);
    }
}

void TairHashTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    exHashObj *o = (exHashObj *)value;
    RedisModuleString *skey;

    m_dictIterator *di;
    m_dictEntry *de;

    if (o->hash) {
        di = m_dictGetIterator(o->hash);
        while ((de = m_dictNext(di)) != NULL) {
            exHashVal *val = (exHashVal *)dictGetVal(de);
            skey = (RedisModuleString *)dictGetKey(de);
            if (val->expire) {
                if (isExpire(val->expire)) {
                    /* For expired field, we do not REWRITE it in the AOF file */
                    continue;
                }
                RedisModule_EmitAOF(aof, "EXHSET", "sssclcl", key, skey, val->value, "PXAT", val->expire, "ABS", val->version);
            } else {
                RedisModule_EmitAOF(aof, "EXHSET", "ssscl", key, skey, val->value, "ABS", val->version);
            }
        }
        m_dictReleaseIterator(di);
    }
}

size_t TairHashTypeMemUsage(const void *value) {
    exHashObj *o = (exHashObj *)value;

    uint64_t size = 0;
    RedisModuleString *skey;
    size_t skeylen = 0;

    if (!o) {
        return size;
    }

    m_dictIterator *di;
    m_dictEntry *de;

    if (o->hash) {
        size += sizeof(*o);

        di = m_dictGetIterator(o->hash);
        while ((de = m_dictNext(di)) != NULL) {
            exHashVal *val = (exHashVal *)dictGetVal(de);
            skey = dictGetKey(de);
            size += sizeof(*val);
            RedisModule_StringPtrLen(skey, &skeylen);
            size += skeylen;
            size_t len;
            RedisModule_StringPtrLen(val->value, &len);
            size += len;
        }
        m_dictReleaseIterator(di);
    }

    // TODO: include skiplist size

    return size;
}

void TairHashTypeFree(void *value) {
    if (value) {
        exHashTypeReleaseObject(value);
    }
}

void TairHashTypeDigest(RedisModuleDigest *md, void *value) {
    exHashObj *o = (exHashObj *)value;

    RedisModuleString *skey;

    if (!o) {
        return;
    }

    m_dictIterator *di;
    m_dictEntry *de;

    /* TODO: Need to exclude expired fields? */
    if (o->hash) {
        di = m_dictGetIterator(o->hash);
        while ((de = m_dictNext(di)) != NULL) {
            exHashVal *val = (exHashVal *)dictGetVal(de);
            skey = (RedisModuleString *)dictGetKey(de);
            size_t val_len, skey_len;
            const char *val_ptr = RedisModule_StringPtrLen(val->value, &val_len);
            const char *skey_ptr = RedisModule_StringPtrLen(skey, &skey_len);
            RedisModule_DigestAddStringBuffer(md, (unsigned char *)skey_ptr, skey_len);
            RedisModule_DigestAddStringBuffer(md, (unsigned char *)val_ptr, val_len);
        }
        m_dictReleaseIterator(di);
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

    if (RedisModule_Init(ctx, "exhash---", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

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
    if (TairHashType == NULL)
        return REDISMODULE_ERR;

    if (REDISMODULE_ERR == Module_CreateCommands(ctx)) {
        return REDISMODULE_ERR;
    }

    if (enable_active_expire) {
        for (int i = 0; i < DB_NUM; i++) {
            zsl[i] = m_zslCreate();
        }
        RedisModuleCtx *ctx2 = RedisModule_GetThreadSafeContext(NULL);
        startExpireTimer(ctx2, NULL);
        RedisModule_FreeThreadSafeContext(ctx2);
    }

    return REDISMODULE_OK;
}
