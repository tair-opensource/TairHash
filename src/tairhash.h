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

#include <limits.h>
#include <stdio.h>

#include "dict.h"
#include "alloc_size.h"
#include "fieldtable.h"
#include "list.h"
#include "redismodule.h"
#include "skiplist.h"
#include "slabapi.h"
#include "util.h"

/* Reported to the server (MODULE LIST, INFO modules) as major * 10000 + minor * 100 + patch. */
#define TAIRHASH_MODULE_VERSION 20100

/* Encoding version of the RDB format; RDB load rejects newer encodings. Encoding 1 adds the
 * synced flag and each field's write sequence; encoding 0 still loads. */
#define TAIRHASH_ENCVER 1

#define TAIRHASH_ERRORMSG_SYNTAX "ERR syntax error"
#define TAIRHASH_ERRORMSG_VERSION "ERR update version is stale"
#define TAIRHASH_ERRORMSG_INTERNAL_CMD "ERR only accepted from the replication stream or while loading"
#define TAIRHASH_ERRORMSG_REPL_LAYOUT "ERR unknown replication layout: upgrade this node to the master's TairHash version"
#define TAIRHASH_ERRORMSG_REPL_NO_FIELD "ERR the field to update does not exist: this node has diverged from its master"
#define TAIRHASH_ERRORMSG_NOT_INTEGER "ERR value is not an integer"
#define TAIRHASH_ERRORMSG_NOT_FLOAT "ERR value is not an float"
#define TAIRHASH_ERRORMSG_OVERFLOW "ERR increment or decrement would overflow"
#define TAIRHASH_ERRORMSG_INTERNAL_ERR "ERR internal error"
#define TAIRHASH_ERRORMSG_INT_MIN_MAX "ERR min or max is specified, but value is not an integer"
#define TAIRHASH_ERRORMSG_FLOAT_MIN_MAX "ERR min or max is specified, but value is not a float"
#define TAIRHASH_ERRORMSG_MIN_MAX "ERR min value is bigger than max value"
#define TAIRHASH_ERRORMSG_INVALID_EXPIRE "ERR invalid expire time"
#define TAIRHASH_ERRORMSG_VERSION_OVERFLOW "ERR version would overflow"
#define TAIRHASH_ERRORMSG_FIELD_TOO_LONG "ERR field name or value is too long"
#define TAIRHASH_ERRORMSG_SYNC_LEASE "ERR no sync lease for this token: call EXHSYNCBEGIN again"

/* Highest write sequence accepted from the replication stream or an RDB. Sequences are
 * microsecond timestamps, so this is far beyond any real one, and far enough below LLONG_MAX
 * that assigning later sequences cannot overflow. */
#define TAIRHASH_SEQ_MAX (LLONG_MAX / 2)
/* Longest lww_tombstone_ttl and sync_lease_ttl, in seconds: one whose deadline, the clock
 * plus the TTL in milliseconds, still fits. */
#define TAIRHASH_RETENTION_MAX_SEC (LLONG_MAX / 2 / 1000)
/* Most fields RDB load sizes the table for before reading them; larger hashes grow as they load. */
#define TAIRHASH_RDB_PRESIZE_MAX (1UL << 24)

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

#define TAIR_HASH_ACTIVE_EXPIRE_PERIOD 1000
#define TAIR_HASH_ACTIVE_EXPIRE_KEYS_PER_LOOP 1000
#define TAIR_HASH_ACTIVE_DBS_PER_CALL 16
#define TAIR_HASH_PASSIVE_EXPIRE_KEYS_PER_LOOP 3
#define TAIR_HASH_SCAN_DEFAULT_COUNT 10

#define Module_Assert(_e) ((_e) ? (void)0 : (_moduleAssert(#_e, __FILE__, __LINE__), abort()))

/*
 * Each field (see TairHashField in fieldtable.h) carries `version` and `expire` as part
 * of the tairhash value. This may be different from the expire on the redis key. Redis
 * regards `expire` as part of the database, not part of the key. For example, after you
 * perform a restore on a key, the original expire will be Lost unless you specify ttl
 * again. The `version` and `expire` of tairhash will be completely recovered after the
 * restore.
 *
 * `seq` is the server-assigned write sequence of the field's last value write. It is
 * strictly increasing per server across key lifetimes (see tairHashNextSeq), so EXHSYNC
 * can tell a field written after a reader took its sync token apart from one the source
 * of truth no longer has, independently of the caller-supplied `version`.
 */

/*
 * `expire_index` is allocated on the first field that gets a TTL, so a hash that never
 * uses field expiration pays nothing for it. Use TAIRHASH_EXPIRE_INDEX_LEN to read its
 * length.
 *
 * `lww` holds the state of the last-write-wins and sync commands (see EXHSYNC), allocated
 * when a hash first uses them, so other hashes pay one pointer for it.
 */
typedef struct lwwState {
    /* Sync epoch in which EXHSYNC last made the hash an authoritative copy of the source of
     * truth, 0 if never. The hash is synced only while this is the current epoch, which a
     * master renews when it loads its data or is promoted, and a replica takes from its
     * master. A synced hash is kept even without fields, so "the source has nothing for this
     * key" is cacheable. */
    long long synced_epoch;
    /* Token of the sync EXHSYNCBEGIN opened and EXHSYNC has not completed, 0 if none. Not
     * persisted or replicated: a sync that spans a restart or a failover starts again. */
    long long lease;
    /* When the lease ends (unix time in milliseconds). Kept apart from the token, which only
     * orders the sync among the writes: it is a sequence, not a time. */
    long long lease_deadline;
    /* Sync epoch the lease was opened in. It ends with the epoch: after a failover and back
     * the hash holds fields another master sequenced, which the token cannot be compared with. */
    long long lease_epoch;
    /* The key's TTL was set by EXHSYNCBEGIN (a placeholder) or EXHDELLWW (a hash left with
     * only tombstones), so that an abandoned one goes away. It lasts only while the hash has
     * no fields: the first write that adds one, or EXHSYNC, removes it. */
    int owns_key_ttl;
    /* Fields deleted by EXHDELLWW: the name, the delete version in `version` and the end of
     * the retention in `expire` (unix time in milliseconds). Writes and snapshot entries
     * with a version up to a tombstone's lose to it. */
    fieldTable tombstones;
    /* Tombstone count at which, and time (unix time in milliseconds) from which, the next
     * write drops the tombstones whose retention ended. */
    size_t purge_at;
    long long purge_due;
} lwwState;

typedef struct tairHashObj {
    fieldTable hash;
#if defined SLAB_MODE
    tairhash_zskiplist *expire_index;
#else
    m_zskiplist *expire_index;
#endif
    RedisModuleString *key;
    lwwState *lww;
} tairHashObj;

#define TAIRHASH_EXPIRE_INDEX_LEN(o) ((o)->expire_index ? (o)->expire_index->length : 0)

#if defined(SLAB_MODE)
#define TAIRHASH_EXPIRE_MODE_NAME "slab"
#elif defined(SORT_MODE)
#define TAIRHASH_EXPIRE_MODE_NAME "sort"
#else
#define TAIRHASH_EXPIRE_MODE_NAME "scan"
#endif

/* Hashes, across all databases, with at least one field waiting to expire. */
unsigned long long expiringKeyCount(void);

#if defined(SORT_MODE) || defined(SLAB_MODE)
/* UNLINK already took the key out of its db expire index. */
static inline void expiringKeyReleased(const tairHashObj *o) {
    (void)o;
}
#else
/* Called as a hash is freed, possibly on a lazy-free thread. */
void expiringKeyReleased(const tairHashObj *o);
#endif

typedef struct ExpireAlgorithm {
    void (*insert)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire);
    void (*update)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long cur_expire, long long new_expire);
    void (*delete)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire);
    /* Returns whether the field was deleted: the server can refuse a delete that goes through a command. */
    int (*deleteAndPropagate)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire, int is_timer);
    void (*activeExpire)(RedisModuleCtx *ctx, int dbid, uint64_t keys);
    void (*passiveExpire)(RedisModuleCtx *ctx, int dbid, RedisModuleString *key_per_loop);

    int enable_active_expire;
    uint64_t active_expire_period;
    uint64_t dbs_per_active_loop;
    uint64_t keys_per_active_loop;
    uint64_t keys_per_passive_loop;
    int db_num; /* databases of the server, counted at load; the arrays below have one entry per db */
    uint64_t *stat_active_expired_field;
    uint64_t *stat_passive_expired_field;
    uint64_t stat_last_active_expire_time_msec;
    uint64_t stat_avg_active_expire_time_msec;
    uint64_t stat_max_active_expire_time_msec;
} ExpireAlgorithm;

void _moduleAssert(const char *estr, const char *file, int line);
RedisModuleString *ownString(RedisModuleString *str);
RedisModuleString *expireIndexMember(const tairHashObj *obj);
void tairHashEnsureExpireIndex(tairHashObj *o);
int delEmptyTairHashIfNeeded(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *raw_key, tairHashObj *obj);
void notifyFieldSpaceEvent(char *event, RedisModuleString *key, RedisModuleString *field, int dbid);
int isExpire(long long when);
int isReadOnlyStatus(RedisModuleCtx *ctx);
int fieldExpireIfNeeded(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *field, int is_timer);

#if defined(SORT_MODE) || defined(SLAB_MODE)
/* One per database, NULL until the db gets an expiring field; use dbExpireIndex. */
extern m_zskiplist **g_expire_index;
m_zskiplist *dbExpireIndex(int dbid);

/* An entry taken out of a db expire index: the key name it held and its score. */
typedef struct expiringKey {
    RedisModuleString *key;
    long long score;
} expiringKey;

int tairHashMinExpire(const tairHashObj *o, long long *min_expire);
list *popDueExpireIndexEntries(int dbid, long long now, long limit);
tairHashObj *openIndexedTairHash(RedisModuleCtx *ctx, const expiringKey *ek, int mode, RedisModuleKey **key);
void releaseExpiringKey(list *keys, m_listNode *node);
#endif
