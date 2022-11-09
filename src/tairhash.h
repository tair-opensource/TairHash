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
#pragma once

#include <stdio.h>

#include "dict.h"
#include "list.h"
#include "redismodule.h"
#include "skiplist.h"
#include "slabapi.h"
#include "util.h"

#define TAIRHASH_ERRORMSG_SYNTAX "ERR syntax error"
#define TAIRHASH_ERRORMSG_VERSION "ERR update version is stale"
#define TAIRHASH_ERRORMSG_NOT_INTEGER "ERR value is not an integer"
#define TAIRHASH_ERRORMSG_NOT_FLOAT "ERR value is not an float"
#define TAIRHASH_ERRORMSG_OVERFLOW "ERR increment or decrement would overflow"
#define TAIRHASH_ERRORMSG_INTERNAL_ERR "ERR internal error"
#define TAIRHASH_ERRORMSG_INT_MIN_MAX "ERR min or max is specified, but value is not an integer"
#define TAIRHASH_ERRORMSG_FLOAT_MIN_MAX "ERR min or max is specified, but value is not a float"
#define TAIRHASH_ERRORMSG_MIN_MAX "ERR min value is bigger than max value"

#define TAIR_HASH_SET_NO_FLAGS 0
#define TAIR_HASH_SET_NX (1 << 0)
#define TAIR_HASH_SET_XX (1 << 1)
#define TAIR_HASH_SET_EX (1 << 2)
#define TAIR_HASH_SET_PX (1 << 3)
#define TAIR_HASH_SET_ABS_EXPIRE (1 << 4)
#define TAIR_HASH_SET_WITH_VER (1 << 5)
#define TAIR_HASH_SET_WITH_ABS_VER (1 << 6)
#define TAIR_HASH_SET_WITH_GT_VER (1 << 7)
#define TAIR_HASH_SET_WITH_BOUNDARY (1 << 8)
#define TAIR_HASH_SET_KEEPTTL (1 << 9)

#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1
#define DB_NUM 16 /* This value must be equal to the db_dum of redis. */

#define TAIR_HASH_ACTIVE_EXPIRE_PERIOD 1000
#define TAIR_HASH_ACTIVE_EXPIRE_KEYS_PER_LOOP 1000
#define TAIR_HASH_ACTIVE_DBS_PER_CALL 16
#define TAIR_HASH_PASSIVE_EXPIRE_KEYS_PER_LOOP 3
#define TAIR_HASH_SCAN_DEFAULT_COUNT 10

#define Module_Assert(_e) ((_e) ? (void)0 : (_moduleAssert(#_e, __FILE__, __LINE__), abort()))

/*
 * We use `version` and `expire` as part of the tairhash value. This may be different
 * from the expire on the redis key. Redis regards `expire` as part of the database, not
 * part of the key. For example, after you perform a restore on a key, the original expire
 * will be Lost unless you specify ttl again. The `version` and `expire` of tairhash will
 * be completely recovered after the restore.
 */
typedef struct TairHashVal {
    long long version;
    long long expire;
    RedisModuleString *value;
} TairHashVal;

typedef struct tairHashObj {
    dict *hash;
#if defined SLAB_MODE
    tairhash_zskiplist *expire_index;
#else
    m_zskiplist *expire_index;
#endif
    RedisModuleString *key;
} tairHashObj;

typedef struct ExpireAlgorithm {
    void (*insert)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire);
    void (*update)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long cur_expire, long long new_expire);
    void (*delete)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire);
    void (*activeExpire)(RedisModuleCtx *ctx, int dbid, uint64_t keys);
    void (*passiveExpire)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key_per_loop);
    int (*expireIfNeeded)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, int is_timer);

    int enable_active_expire;
    uint64_t active_expire_period;
    uint64_t dbs_per_active_loop;
    uint64_t keys_per_active_loop;
    uint64_t keys_per_passive_loop;
    uint64_t stat_active_expired_field[DB_NUM];
    uint64_t stat_passive_expired_field[DB_NUM];
    uint64_t stat_last_active_expire_time_msec;
    uint64_t stat_avg_active_expire_time_msec;
    uint64_t stat_max_active_expire_time_msec;
} ExpireAlgorithm;

void _moduleAssert(const char *estr, const char *file, int line);
RedisModuleString *takeAndRef(RedisModuleString *str);
int delEmptyTairHashIfNeeded(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *raw_key, tairHashObj *obj);
void notifyFieldSpaceEvent(char *event, RedisModuleString *key, RedisModuleString *field, int dbid);
int isExpire(long long when);
