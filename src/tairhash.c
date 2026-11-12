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

static int hashIsSynced(const tairHashObj *o);
static long long liveTombstoneVersion(tairHashObj *o, RedisModuleString *name);
static void releaseOwnedKeyTtl(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *keyname, tairHashObj *o);
static int lwwKeepsEmptyHash(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *keyname, tairHashObj *o);
static void lwwStateFree(lwwState *s);

int delEmptyTairHashIfNeeded(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *raw_key, tairHashObj *obj) {
    /* A replica gets its master's decision, and a load replays the one in the file: whether an
     * emptied hash stays depends on its sync state and lease, which neither of them has as the
     * master had it. */
    int follows_log = RedisModule_GetContextFlags(ctx) & (REDISMODULE_CTX_FLAGS_SLAVE | REDISMODULE_CTX_FLAGS_LOADING);
    if (!obj || hashIsSynced(obj) || follows_log || (ftSize(&obj->hash) != 0)) {
        return 0;
    }
    /* raw_key is still used after the key is deleted, so it must not be owned by obj. */
    Module_Assert(raw_key != obj->key);

    int opened = 0;
    if (key == NULL) {
        key = RedisModule_OpenKey(ctx, raw_key, REDISMODULE_WRITE);
        if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
            RedisModule_CloseKey(key);
            return 0;
        }
        opened = 1;
    }
    /* Before Redis 6.2 a callback context that has propagated keeps its MULTI open until it
     * is freed, and a command RedisModule_Call runs before that, as the expire timer does for
     * the next field, opens another one inside it (see the bugfixes below). A thread safe
     * context propagates at once and wraps nothing. */
    RedisModuleCtx *ctx2 = NULL;
    if (redis_major_ver < 6 || (redis_major_ver == 6 && redis_minor_ver < 2)) {
        ctx2 = RedisModule_GetThreadSafeContext(NULL);
        RedisModule_SelectDb(ctx2, RedisModule_GetSelectedDb(ctx));
    }
    if (lwwKeepsEmptyHash(ctx2 ? ctx2 : ctx, key, raw_key, obj)) {
        if (opened) {
            RedisModule_CloseKey(key);
        }
        if (ctx2) {
            RedisModule_FreeThreadSafeContext(ctx2);
        }
        return 0;
    }

    if (ctx2) {
        /* See bugfix: https://github.com/redis/redis/pull/8617
                       https://github.com/redis/redis/pull/8097
                       https://github.com/redis/redis/pull/7037
        */
        RedisModule_CloseKey(key);
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
    lwwStateFree(o->lww);
    RedisModule_Free(o);
}

/* The server treats a free effort of 0 as "always free asynchronously", which would hand
 * every emptied TairHash to a background thread. Count the object itself so small values
 * are freed synchronously. */
static size_t tairHashFreeEffort(const struct tairHashObj *o) {
    return 1 + ftSize(&o->hash) + TAIRHASH_EXPIRE_INDEX_LEN(o) + (o->lww ? ftSize(&o->lww->tombstones) : 0);
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

/* Adds a field with this name and value; expire and seq start at 0, and so does the version
 * unless the field replaces a tombstone (a field and its tombstone never exist together).
 * It then starts at the delete's version while the tombstone's retention lasts, so that a
 * command that increments the version, as it does on an existing field, puts it above every
 * write the tombstone held off. */
static TairHashField *fieldInsert(tairHashObj *o, RedisModuleString *name, RedisModuleString *value) {
    size_t name_len, value_len;
    const char *name_ptr = RedisModule_StringPtrLen(name, &name_len);
    const char *value_ptr = RedisModule_StringPtrLen(value, &value_len);
    TairHashField *f = fieldCreate(name_ptr, name_len, value_ptr, value_len);
    ftAdd(&o->hash, f);
    if (o->lww && ftSize(&o->lww->tombstones)) {
        TairHashField *t = ftUnlink(&o->lww->tombstones, name_ptr, name_len);
        if (t != NULL) {
            if (t->expire > RedisModule_Milliseconds()) {
                /* Short of the end by more than one command can increment it. */
                f->version = t->version < LLONG_MAX - INT_MAX ? t->version : LLONG_MAX - INT_MAX;
            }
            fieldFree(t);
        }
    }
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

static RedisModuleString *fieldValueString(RedisModuleCtx *ctx, const TairHashField *f) {
    return RedisModule_CreateString(ctx, fieldValue(f), f->value_len);
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

/* ========================== Write sequence =============================*/

/* Highest write sequence assigned or observed (replication, RDB, AOF) by this server. */
static long long g_last_seq = 0;

/* Set at load time when the server provides RedisModule_CachedMicroseconds (Redis 7.2+). */
static long long (*g_cached_microseconds)(void) = NULL;

static long long ustime(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((long long)tv.tv_sec) * 1000000 + tv.tv_usec;
}

/* Wall-clock microseconds for sequence assignment. Read it once per command and pass
 * it to every tairHashNextSeq of that command: a clock read per field was a measurable
 * share of a write. The server's cached command time is used where available. */
static long long seqClockNow(void) {
    return g_cached_microseconds ? g_cached_microseconds() : ustime();
}

/* A hybrid logical clock: max(last + 1, wall-clock microseconds). It is strictly
 * increasing within a process, and because it tracks wall time it stays above the
 * sequences a previous process (or a failed-over master) handed out, even when no
 * state was persisted. The state format replicates sequences explicitly. */
static long long tairHashNextSeq(long long now) {
    g_last_seq = now > g_last_seq ? now : g_last_seq + 1;
    return g_last_seq;
}

static void tairHashObserveSeq(long long seq) {
    if (seq > g_last_seq) {
        g_last_seq = seq;
    }
}

/* How a field's state is written as a command TairHash 1.x also accepts (the compat format).
 * EXHSET ... ABS <version> [PXAT <expire>] sets value, version and TTL exactly, but
 * cannot set version 0. Only a field that TairHash 1.x created with EXHSETNX and nothing
 * has written since has version 0 (every other write sets or increments the version), so
 * it has no TTL either. It is written as EXHSETNX, which 1.x applies at version 0 and so
 * does this version when the command comes from the replication stream or the AOF. */
typedef enum {
    FIELD_STATE_EXHSETNX,
    FIELD_STATE_EXHSET,
    FIELD_STATE_EXHSET_PXAT,
} FieldStateCommand;

static FieldStateCommand fieldStateCommand(long long version, long long expire) {
    if (version == 0) {
        return FIELD_STATE_EXHSETNX;
    }
    return expire ? FIELD_STATE_EXHSET_PXAT : FIELD_STATE_EXHSET;
}

/* The resulting state of one field a command wrote. */
typedef struct {
    RedisModuleString *field;
    RedisModuleString *value;
    long long version;
    long long expire;
    long long seq;
} FieldState;

/* How write commands and AOF rewrite propagate field state (replication_format). */
typedef enum {
    REPLICATION_FORMAT_STATE,  /* one EXHMSETREPL per write */
    REPLICATION_FORMAT_COMPAT, /* one EXHSET or EXHSETNX per field, which TairHash 1.x also accepts */
} ReplicationFormat;

static int g_replication_format = REPLICATION_FORMAT_STATE;

/* EXHMSETREPL <key> <layout> <field> <value> <version> <expire> [<seq>] [...]: the layout
 * names the per-field arguments, so a replica rejects a layout it does not know instead
 * of misreading it. Layout 1 (TairHash 2.0) is field, value, version, expire; layout 2
 * adds the write sequence. */
#define FIELD_STATE_LAYOUT 2
#define FIELD_STATE_ARGS 5
#define FIELD_STATE_ARGS_LAYOUT_1 4

/* Fields per propagated EXHMSETREPL. A write is one command, split only so that a huge one
 * stays within what a replica accepts (Redis 6.2 and older reject commands of more than
 * 1048576 arguments); the parts of one write still propagate together, in one MULTI/EXEC. */
#define FIELD_STATE_FIELDS_PER_CMD 8192

/* EXHMETAREPL <key> <layout> <field> <version> <expire> [...]: the version and expire of
 * fields whose value a write left alone (the EXHEXPIRE family, EXHPERSIST, EXHSETVER), so
 * that a TTL refresh does not carry the value. Layout 1 is field, version, expire. */
#define FIELD_META_LAYOUT 1
#define FIELD_META_ARGS 3

/* Shared FIELD_STATE_LAYOUT and "0" (the expire of a field without a TTL); created at load time. */
static RedisModuleString *g_layout_str = NULL;
static RedisModuleString *g_zero_str = NULL;

/* Set at load time when the server reports the REPLICATED and LOADING context flags
 * (RedisModule_GetContextFlagsAll, Redis 6.0.9+). */
static int g_can_identify_restoring = 0;

/* Whether the command comes from the replication stream or AOF loading, which carry state
 * the master already decided, on a server that can tell. */
static int isRestoringState(RedisModuleCtx *ctx) {
    if (!g_can_identify_restoring) {
        return 0;
    }
    int flags = RedisModule_GetContextFlags(ctx);
    return (flags & (REDISMODULE_CTX_FLAGS_REPLICATED | REDISMODULE_CTX_FLAGS_LOADING)) != 0;
}

/* Whether the caller may restore exact state (EXHMSETREPL): a regular client must not be
 * able to forge it. A server that cannot report the restoring contexts cannot tell callers
 * apart, so restoring stays allowed there. */
static int mayRestoreState(RedisModuleCtx *ctx) {
    return !g_can_identify_restoring || isRestoringState(ctx);
}

/* Collects the resulting state of the fields one command writes and propagates it.
 * Propagating state rather than the request makes replicas and the AOF converge
 * regardless of their clock (relative TTLs) and their own state (KEEPTTL, GT/VER checks,
 * version increments). In the state format a write is one EXHMSETREPL, which costs the
 * replica one command lookup and argv per write instead of one per field. */
typedef struct {
    RedisModuleCtx *ctx;
    RedisModuleString *key;
    FieldState *fields;
    int len;
    int cap;
    int format; /* ReplicationFormat */
} FieldStateBatch;

static void fieldStateBatchInit(FieldStateBatch *b, RedisModuleCtx *ctx, RedisModuleString *key, int fields_hint) {
    b->ctx = ctx;
    b->key = key;
    b->len = 0;
    b->cap = fields_hint > 0 ? fields_hint : 1;
    b->fields = RedisModule_Alloc(sizeof(FieldState) * b->cap);
    b->format = g_replication_format;
}

/* Snapshots the state of field f. `name` and `value` are the field's name and value as
 * strings the caller already has (usually client arguments), so propagating copies
 * nothing. The batch holds its own references, so a later write to the same field
 * within the command (repeated fields) cannot invalidate it. */
static void fieldStateBatchAdd(FieldStateBatch *b, RedisModuleString *name, RedisModuleString *value, const TairHashField *f) {
    if (b->len == b->cap) {
        b->cap *= 2;
        b->fields = RedisModule_Realloc(b->fields, sizeof(FieldState) * b->cap);
    }
    FieldState *st = &b->fields[b->len++];
    /* Retained, not copied: the batch is propagated and released before the command returns. */
    RedisModule_RetainString(NULL, name);
    RedisModule_RetainString(NULL, value);
    st->field = name;
    st->value = value;
    st->version = f->version;
    st->expire = f->expire;
    st->seq = f->seq;
}

static void replicateOneFieldState(RedisModuleCtx *ctx, RedisModuleString *key, const FieldState *st) {
    switch (fieldStateCommand(st->version, st->expire)) {
    case FIELD_STATE_EXHSETNX:
        RedisModule_Replicate(ctx, "EXHSETNX", "sss", key, st->field, st->value);
        break;
    case FIELD_STATE_EXHSET:
        RedisModule_Replicate(ctx, "EXHSET", "ssscl", key, st->field, st->value, "ABS", st->version);
        break;
    case FIELD_STATE_EXHSET_PXAT:
        RedisModule_Replicate(ctx, "EXHSET", "sssclcl", key, st->field, st->value, "ABS", st->version, "PXAT", st->expire);
        break;
    }
}

static void replicateFieldStates(RedisModuleCtx *ctx, RedisModuleString *key, const FieldState *fields, int n) {
    while (n > FIELD_STATE_FIELDS_PER_CMD) {
        replicateFieldStates(ctx, key, fields, FIELD_STATE_FIELDS_PER_CMD);
        fields += FIELD_STATE_FIELDS_PER_CMD;
        n -= FIELD_STATE_FIELDS_PER_CMD;
    }
    int argc = 2 + FIELD_STATE_ARGS * n;
    RedisModuleString **argv = RedisModule_Alloc(sizeof(*argv) * argc);
    argv[0] = key;
    argv[1] = g_layout_str;
    for (int i = 0; i < n; i++) {
        RedisModuleString **a = argv + 2 + FIELD_STATE_ARGS * i;
        a[0] = fields[i].field;
        a[1] = fields[i].value;
        a[2] = RedisModule_CreateStringFromLongLong(NULL, fields[i].version);
        a[3] = fields[i].expire ? RedisModule_CreateStringFromLongLong(NULL, fields[i].expire) : g_zero_str;
        a[4] = RedisModule_CreateStringFromLongLong(NULL, fields[i].seq);
    }
    RedisModule_Replicate(ctx, "EXHMSETREPL", "v", argv, (size_t)argc);
    for (int i = 0; i < n; i++) {
        RedisModuleString **a = argv + 2 + FIELD_STATE_ARGS * i;
        RedisModule_FreeString(NULL, a[2]);
        if (a[3] != g_zero_str) {
            RedisModule_FreeString(NULL, a[3]);
        }
        RedisModule_FreeString(NULL, a[4]);
    }
    RedisModule_Free(argv);
}

/* Propagates the collected state, if any, and releases the batch (the key belongs to
 * the caller). */
static void fieldStateBatchFlush(FieldStateBatch *b) {
    if (b->len > 0) {
        if (b->format == REPLICATION_FORMAT_STATE) {
            replicateFieldStates(b->ctx, b->key, b->fields, b->len);
        } else {
            for (int i = 0; i < b->len; i++) {
                replicateOneFieldState(b->ctx, b->key, &b->fields[i]);
            }
        }
    }
    for (int i = 0; i < b->len; i++) {
        RedisModule_FreeString(NULL, b->fields[i].field);
        RedisModule_FreeString(NULL, b->fields[i].value);
    }
    RedisModule_Free(b->fields);
    b->fields = NULL;
}

static void replicateFieldState(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *name, RedisModuleString *value,
                                const TairHashField *f) {
    FieldStateBatch batch;
    fieldStateBatchInit(&batch, ctx, key, 1);
    fieldStateBatchAdd(&batch, name, value, f);
    fieldStateBatchFlush(&batch);
}

/* Propagates the version and expire of field f after a write that left its value alone. The
 * compat format has no command for that and sends the value as well. */
static void replicateFieldMeta(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *name, const TairHashField *f) {
    if (g_replication_format != REPLICATION_FORMAT_STATE) {
        replicateFieldState(ctx, key, name, fieldValueString(ctx, f), f);
        return;
    }
    RedisModule_Replicate(ctx, "EXHMETAREPL", "slsll", key, (long long)FIELD_META_LAYOUT, name, f->version, f->expire);
}

/* Sets the expire of field f (0 for none) and keeps the expire indexes in step, leaving them
 * alone when the expire does not change. */
static void setFieldExpire(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *name, TairHashField *f,
                           long long expire) {
    if (f->expire == expire) {
        return;
    }
    if (f->expire == 0) {
        g_expire_algorithm.insert(ctx, dbid, key, o, name, expire);
    } else if (expire == 0) {
        g_expire_algorithm.delete(ctx, dbid, key, o, name, f->expire);
    } else {
        g_expire_algorithm.update(ctx, dbid, key, o, name, f->expire, expire);
    }
    ftSetExpire(&o->hash, f, expire);
}

/* A state command from the replication stream or the AOF that is rejected drops the write it
 * carries, and nobody reads the error reply there, so the rejection is logged too. */
static int rejectStateCommand(RedisModuleCtx *ctx, const char *command, const char *err) {
    RedisModule_Log(ctx, "warning", "%s rejected, the write it carries is lost: %s", command, err);
    RedisModule_ReplyWithError(ctx, err);
    return REDISMODULE_ERR;
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

        /* The resulting expire and version, not EXHPEXPIREAT: a node that judges the field by its
         * own clock (TairHash 1.x, or a server that does not say where a command comes from) would
         * drop the write once the field's previous TTL had passed. */
        replicateFieldMeta(ctx, argv[1], argv[2], tair_hash_val);
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

    /* Read before any key or field lookup: the clock read serializes the pipeline
     * (ISB on arm64, lfence/rdtscp on x86), stalling on any cache miss still in flight. */
    long long now = seqClockNow();

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
        /* The current version of a deleted field is its tombstone's. */
        if ((ex_flags & TAIR_HASH_SET_WITH_GT_VER) && version <= liveTombstoneVersion(tair_hash_obj, skey)) {
            abortWrite(ctx, key, pkey, tair_hash_obj);
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
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

    tair_hash_val->seq = tairHashNextSeq(now);
    RedisModule_ReplyWithLongLong(ctx, nokey ? 1 : 0);

    replicateFieldState(ctx, argv[1], argv[2], argv[3], tair_hash_val);
    releaseOwnedKeyTtl(ctx, key, argv[1], tair_hash_obj);
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

    /* Read before any key or field lookup: the clock read serializes the pipeline
     * (ISB on arm64, lfence/rdtscp on x86), stalling on any cache miss still in flight. */
    long long now = seqClockNow();

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

    TairHashField *tair_hash_val = fieldInsert(tair_hash_obj, skey, svalue);
    /* A new field starts at version 1, as with EXHSET, or one above the version of the
     * tombstone it replaces (see fieldInsert); version 0 means "no version". From the
     * replication stream or the AOF the command states a field at version 0 instead: TairHash
     * 1.x and the compat format have no other way to write one. */
    if (isRestoringState(ctx)) {
        tair_hash_val->version = 0;
    } else {
        tair_hash_val->version++;
    }
    tair_hash_val->seq = tairHashNextSeq(now);

    replicateFieldState(ctx, pkey, skey, svalue, tair_hash_val);
    releaseOwnedKeyTtl(ctx, key, pkey, tair_hash_obj);
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

    /* Read before any key or field lookup: the clock read serializes the pipeline
     * (ISB on arm64, lfence/rdtscp on x86), stalling on any cache miss still in flight. */
    long long now = seqClockNow();

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
    FieldStateBatch batch;
    fieldStateBatchInit(&batch, ctx, argv[1], (argc - 2) / 2);
    for (int i = 2; i < argc; i += 2) {
        fieldExpireIfNeeded(ctx, dbid, argv[1], tair_hash_obj, argv[i], 0);
        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[i]);
        if (tair_hash_val == NULL) {
            tair_hash_val = fieldInsert(tair_hash_obj, argv[i], argv[i + 1]);
        } else {
            tair_hash_val = fieldSetValue(tair_hash_obj, tair_hash_val, argv[i + 1]);
        }
        tair_hash_val->version++;
        tair_hash_val->seq = tairHashNextSeq(now);
        fieldStateBatchAdd(&batch, argv[i], argv[i + 1], tair_hash_val);
    }
    fieldStateBatchFlush(&batch);
    releaseOwnedKeyTtl(ctx, key, argv[1], tair_hash_obj);

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

    /* Read before any key or field lookup: the clock read serializes the pipeline
     * (ISB on arm64, lfence/rdtscp on x86), stalling on any cache miss still in flight. */
    long long seq_now = seqClockNow();

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

    FieldStateBatch batch;
    fieldStateBatchInit(&batch, ctx, argv[1], (argc - 2) / 4);
    for (int i = 2; i < argc; i += 4) {
        /* Validated by the loop above. */
        RedisModule_StringToLongLong(argv[i + 3], &when);

        TairHashField *tair_hash_val = fieldLookup(tair_hash_obj, argv[i]);
        if (tair_hash_val == NULL) {
            tair_hash_val = fieldInsert(tair_hash_obj, argv[i], argv[i + 1]);
            nokey = 1;
        } else {
            tair_hash_val = fieldSetValue(tair_hash_obj, tair_hash_val, argv[i + 1]);
            nokey = 0;
        }
        tair_hash_val->version++;
        tair_hash_val->seq = tairHashNextSeq(seq_now);

        /* Cannot overflow: the loop above checked it. */
        expireToAbsoluteMs(when, 1, now, &when);
        if (nokey || tair_hash_val->expire == 0) {
            g_expire_algorithm.insert(ctx, dbid, argv[1], tair_hash_obj, argv[i], when);
        } else {
            g_expire_algorithm.update(ctx, dbid, argv[1], tair_hash_obj, argv[i], tair_hash_val->expire, when);
        }
        ftSetExpire(&tair_hash_obj->hash, tair_hash_val, when);

        fieldStateBatchAdd(&batch, argv[i], argv[i + 1], tair_hash_val);
    }
    fieldStateBatchFlush(&batch);
    releaseOwnedKeyTtl(ctx, key, argv[1], tair_hash_obj);

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
        replicateFieldMeta(ctx, argv[1], argv[2], tair_hash_val);
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
    replicateFieldMeta(ctx, argv[1], argv[2], tair_hash_val);
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

    /* Read before any key or field lookup: the clock read serializes the pipeline
     * (ISB on arm64, lfence/rdtscp on x86), stalling on any cache miss still in flight. */
    long long now = seqClockNow();

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
    } else if (tair_hash_obj && (ex_flags & TAIR_HASH_SET_WITH_GT_VER) && version <= liveTombstoneVersion(tair_hash_obj, argv[2])) {
        /* The current version of a deleted field is its tombstone's. */
        abortWrite(ctx, key, argv[1], tair_hash_obj);
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
        return REDISMODULE_ERR;
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

    tair_hash_val->seq = tairHashNextSeq(now);
    replicateFieldState(ctx, argv[1], argv[2], value, tair_hash_val);
    releaseOwnedKeyTtl(ctx, key, argv[1], tair_hash_obj);
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

    /* Read before any key or field lookup: the clock read serializes the pipeline
     * (ISB on arm64, lfence/rdtscp on x86), stalling on any cache miss still in flight. */
    long long now = seqClockNow();

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
    } else if (tair_hash_obj && (ex_flags & TAIR_HASH_SET_WITH_GT_VER) && version <= liveTombstoneVersion(tair_hash_obj, argv[2])) {
        /* The current version of a deleted field is its tombstone's. */
        abortWrite(ctx, key, argv[1], tair_hash_obj);
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_VERSION);
        return REDISMODULE_ERR;
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

    tair_hash_val->seq = tairHashNextSeq(now);
    replicateFieldState(ctx, argv[1], argv[2], value, tair_hash_val);
    releaseOwnedKeyTtl(ctx, key, argv[1], tair_hash_obj);
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

/* Returns the tairhash at an open key, creating it when the key is empty, or NULL
 * (after replying WRONGTYPE) when the key holds another type. */
static tairHashObj *getOrCreateTairHash(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *keyname) {
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        tairHashObj *o = createTairHashTypeObject();
        o->key = RedisModule_CreateStringFromString(NULL, keyname);
        RedisModule_ModuleTypeSetValue(key, TairHashType, o);
        return o;
    }
    if (RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return NULL;
    }
    return RedisModule_ModuleTypeGetValue(key);
}

/* ===================== Last-write-wins writes and source sync ===================== */

/* The sync epoch: a hash is synced only while its synced_epoch is this, and never while it
 * is 0. A master renews it when the module loads, after an RDB or AOF is loaded and when it
 * is promoted, because each of those can lose writes that synced hashes reflected, and puts
 * the renewal in the replication stream (EXHSYNCEPOCH). A replica's epoch is part of the
 * data it replicates: it takes it from the aux data of an RDB it loads and from EXHSYNCEPOCH
 * in the stream or its own AOF, so it has its master's as of the point of the stream it has
 * applied, and none while it has yet to get it. */
static long long g_sync_epoch = 0;

/* The master's epoch is not in the replication stream yet. */
static int g_sync_epoch_unannounced = 0;

/* This node's data has held a synced hash, so a replica may hold one too and has to learn of
 * a renewal at once. Until then a renewal is announced only ahead of the first synced hash,
 * which keeps the stream of a master that never used the sync commands free of a command
 * that TairHash 2.0 and 1.x do not know. */
static int g_sync_state_seen = 0;

static int g_sync_epoch_timer_armed = 0;

/* The role the node had at the last role change, to tell a demoted master from a replica that
 * only changes master. */
static int g_sync_role_replica = 0;

/* Module options lww_tombstone_ttl and sync_lease_ttl, in milliseconds. */
static long long g_tombstone_ttl_ms = 3600 * 1000LL;
static long long g_sync_lease_ttl_ms = 60 * 1000LL;

/* Whether this node replicates another one. A cluster replica loads its data at startup
 * before the cluster bus has given it the address of its master, which is when the server
 * starts to report it as a replica. */
static int nodeIsReplica(RedisModuleCtx *ctx) {
    int flags = RedisModule_GetContextFlags(ctx);
    if (flags & REDISMODULE_CTX_FLAGS_SLAVE) {
        return 1;
    }
    if (!(flags & REDISMODULE_CTX_FLAGS_CLUSTER) || RedisModule_GetMyClusterID == NULL || RedisModule_GetClusterNodeInfo == NULL) {
        return 0;
    }
    const char *myid = RedisModule_GetMyClusterID();
    if (myid == NULL) {
        return 0;
    }
    /* The server looks a node up by a terminated string, which its own ID is not. */
    char id[REDISMODULE_NODE_ID_LEN + 1];
    memcpy(id, myid, REDISMODULE_NODE_ID_LEN);
    id[REDISMODULE_NODE_ID_LEN] = '\0';
    int node_flags = 0;
    return RedisModule_GetClusterNodeInfo(ctx, id, NULL, NULL, NULL, &node_flags) == REDISMODULE_OK && (node_flags & REDISMODULE_NODE_SLAVE) != 0;
}

/* Starts a new epoch on a master: a sequence, so it differs from every epoch this server has
 * seen (tairHashObserveSeq). */
static void renewSyncEpoch(void) {
    g_sync_epoch = tairHashNextSeq(seqClockNow());
    g_sync_epoch_unannounced = 1;
}

/* Takes the epoch of the data a replica loads or applies. */
static void adoptSyncEpoch(RedisModuleCtx *ctx, long long epoch) {
    tairHashObserveSeq(epoch);
    if (nodeIsReplica(ctx)) {
        g_sync_epoch = epoch;
    }
}

static void announceSyncEpochTimer(RedisModuleCtx *ctx, void *data);

/* Announces the epoch from a timer. A server event can fire inside a command (REPLICAOF NO
 * ONE, DEBUG RELOAD), and propagating there would make the server take that command for a
 * write and propagate it as well; a timer runs outside any command. */
static void announceSyncEpochSoon(RedisModuleCtx *ctx, mstime_t delay) {
    if (!g_sync_epoch_timer_armed) {
        g_sync_epoch_timer_armed = 1;
        RedisModule_CreateTimer(ctx, delay, announceSyncEpochTimer, NULL);
    }
}

/* Puts the master's epoch in the replication stream and the AOF, after everything propagated
 * under the previous one: a replica that follows the stream, replays it from the backlog after
 * a partial resync or receives it through another replica then ends at this epoch. */
static void announceSyncEpoch(RedisModuleCtx *ctx) {
    if (!g_sync_epoch_unannounced) {
        return;
    }
    /* Nothing may be propagated while clients are paused. Write commands do not run then
     * either, so no hash is synced in the new epoch before the retry announces it. */
    if (RedisModule_AvoidReplicaTraffic && RedisModule_AvoidReplicaTraffic()) {
        announceSyncEpochSoon(ctx, 100);
        return;
    }
    if (RedisModule_Replicate(ctx, "EXHSYNCEPOCH", "l", g_sync_epoch) == REDISMODULE_OK) {
        g_sync_epoch_unannounced = 0;
    }
}

static void announceSyncEpochTimer(RedisModuleCtx *ctx, void *data) {
    REDISMODULE_NOT_USED(data);
    g_sync_epoch_timer_armed = 0;
    if (!nodeIsReplica(ctx)) {
        announceSyncEpoch(ctx);
    }
}

static lwwState *lwwStateOf(tairHashObj *o) {
    if (o->lww == NULL) {
        o->lww = RedisModule_Calloc(1, sizeof(*o->lww));
        ftInit(&o->lww->tombstones);
    }
    return o->lww;
}

static void lwwStateFree(lwwState *s) {
    if (s) {
        ftClear(&s->tombstones);
        RedisModule_Free(s);
    }
}

static int hashIsSynced(const tairHashObj *o) {
    return o->lww != NULL && o->lww->synced_epoch != 0 && o->lww->synced_epoch == g_sync_epoch;
}

/* Whether the hash has a sync lease that has not ended at `now_ms`. */
static int leaseActive(const lwwState *s, long long now_ms) {
    return s != NULL && s->lease != 0 && now_ms < s->lease_deadline && s->lease_epoch == g_sync_epoch;
}

/* The epoch to persist and replicate: an old one means "not synced" and is written as 0. */
static long long currentSyncedEpoch(const tairHashObj *o) {
    return hashIsSynced(o) ? o->lww->synced_epoch : 0;
}

/* The tombstone of a field whose retention has not ended at `now_ms`; one that ended is
 * dropped. Dropping is not replicated: each node drops its own by its clock, and the result
 * of a write that a dropped tombstone no longer stops is replicated as usual. */
static TairHashField *liveTombstone(tairHashObj *o, const char *name, size_t len, long long now_ms) {
    if (o->lww == NULL || ftSize(&o->lww->tombstones) == 0) {
        return NULL;
    }
    TairHashField *t = ftFind(&o->lww->tombstones, name, len);
    if (t != NULL && t->expire <= now_ms) {
        ftDelete(&o->lww->tombstones, name, len);
        return NULL;
    }
    return t;
}

/* The delete version of a field's tombstone, or 0 if it has none. */
static long long liveTombstoneVersion(tairHashObj *o, RedisModuleString *name) {
    if (o->lww == NULL || ftSize(&o->lww->tombstones) == 0) {
        return 0;
    }
    size_t len;
    const char *ptr = RedisModule_StringPtrLen(name, &len);
    TairHashField *t = liveTombstone(o, ptr, len, RedisModule_Milliseconds());
    return t ? t->version : 0;
}

static void setTombstone(tairHashObj *o, const char *name, size_t len, long long version, long long deadline) {
    lwwState *s = lwwStateOf(o);
    TairHashField *t = ftFind(&s->tombstones, name, len);
    if (t == NULL) {
        t = fieldCreate(name, len, "", 0);
        ftAdd(&s->tombstones, t);
    }
    t->version = version;
    ftSetExpire(&s->tombstones, t, deadline);
}

/* Drops every tombstone whose retention ended at `now_ms`. */
static void purgeTombstones(tairHashObj *o, long long now_ms);

/* Drops the tombstones whose retention ended, once their count doubled since the last time
 * or an eighth of the retention has passed: a hash that is never synced again must not keep
 * them, and a sweep per write would make each write linear in the number of tombstones. */
#define TOMBSTONE_PURGE_MIN 64
static void maybePurgeTombstones(tairHashObj *o, long long now_ms) {
    lwwState *s = o->lww;
    if (s == NULL || ftSize(&s->tombstones) == 0 || (ftSize(&s->tombstones) < s->purge_at && now_ms < s->purge_due)) {
        return;
    }
    purgeTombstones(o, now_ms);
    s->purge_at = ftSize(&s->tombstones) * 2 > TOMBSTONE_PURGE_MIN ? ftSize(&s->tombstones) * 2 : TOMBSTONE_PURGE_MIN;
    s->purge_due = now_ms + g_tombstone_ttl_ms / 8;
}

static void purgeTombstones(tairHashObj *o, long long now_ms) {
    if (o->lww == NULL || ftSize(&o->lww->tombstones) == 0) {
        return;
    }
    fieldTableIterator it;
    TairHashField *t;
    ftIterInit(&it, &o->lww->tombstones);
    while ((t = ftIterNext(&it)) != NULL) {
        if (t->expire <= now_ms) {
            ftDelete(&o->lww->tombstones, fieldName(t), t->name_len);
        }
    }
    ftIterRelease(&it);
}

/* Propagates a hash's sync state, creating the hash if needed. */
static void replicateSyncState(RedisModuleCtx *ctx, RedisModuleString *keyname, tairHashObj *o) {
    lwwState *s = lwwStateOf(o);
    long long epoch = currentSyncedEpoch(o);
    if (epoch != 0) {
        /* A replica must have the epoch before a hash that is synced in it. */
        g_sync_state_seen = 1;
        announceSyncEpoch(ctx);
    }
    RedisModule_Replicate(ctx, "EXHSYNCREPL", "sll", keyname, epoch, (long long)s->owns_key_ttl);
}

/* Absolute expire time of the key in milliseconds, or REDISMODULE_NO_EXPIRE. */
static long long keyExpireAt(RedisModuleKey *key, long long now_ms) {
    long long ttl = RedisModule_GetExpire(key);
    return ttl == REDISMODULE_NO_EXPIRE ? REDISMODULE_NO_EXPIRE : now_ms + ttl;
}

/* Sets the key's TTL to end at `deadline` (unix time in milliseconds) as a TTL the module
 * owns, and propagates it. */
static void setOwnedKeyTtl(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *keyname, tairHashObj *o, long long deadline) {
    lwwState *s = lwwStateOf(o);
    if (!s->owns_key_ttl) {
        s->owns_key_ttl = 1;
        /* First, so that the hash exists on a replica when the PEXPIREAT gets there. */
        replicateSyncState(ctx, keyname, o);
    }
    long long ttl = deadline - RedisModule_Milliseconds();
    RedisModule_SetExpire(key, ttl > 0 ? ttl : 1);
    RedisModule_Replicate(ctx, "PEXPIREAT", "sl", keyname, deadline);
}

/* Removes a TTL the module owns once the hash has fields (see lwwState.owns_key_ttl). Called
 * by every command that can add a field; a hash that never used the LWW commands pays one
 * pointer test. */
static void releaseOwnedKeyTtl(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *keyname, tairHashObj *o) {
    lwwState *s = o->lww;
    if (s == NULL || !s->owns_key_ttl || ftSize(&o->hash) == 0) {
        return;
    }
    s->owns_key_ttl = 0;
    RedisModule_SetExpire(key, REDISMODULE_NO_EXPIRE);
    replicateSyncState(ctx, keyname, o);
    RedisModule_Replicate(ctx, "PERSIST", "s", keyname);
}

/* Gives a hash that has tombstones but no fields a key TTL ending with its last tombstone,
 * unless the key already has a TTL the module does not own. */
static void expireWithTombstones(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *keyname, tairHashObj *o) {
    lwwState *s = o->lww;
    if (s == NULL || ftSize(&s->tombstones) == 0 || hashIsSynced(o)) {
        return;
    }
    if (!s->owns_key_ttl && RedisModule_GetExpire(key) != REDISMODULE_NO_EXPIRE) {
        return;
    }
    long long deadline = 0;
    fieldTableIterator it;
    TairHashField *t;
    ftIterInitReadOnly(&it, &s->tombstones);
    while ((t = ftIterNext(&it)) != NULL) {
        if (t->expire > deadline) {
            deadline = t->expire;
        }
    }
    ftIterRelease(&it);
    long long current = keyExpireAt(key, RedisModule_Milliseconds());
    if (s->owns_key_ttl && current != REDISMODULE_NO_EXPIRE && current >= deadline) {
        return;
    }
    setOwnedKeyTtl(ctx, key, keyname, o, deadline);
}

/* Whether a hash without fields stays for its LWW state (delEmptyTairHashIfNeeded): it holds
 * tombstones, which it then expires with; it is a placeholder, which expires by itself; or a
 * sync of it is open, which EXHSYNC must find, and it then expires when the lease ends, as a
 * placeholder does, unless the key has a TTL already. */
static int lwwKeepsEmptyHash(RedisModuleCtx *ctx, RedisModuleKey *key, RedisModuleString *keyname, tairHashObj *o) {
    lwwState *s = o->lww;
    if (s == NULL) {
        return 0;
    }
    if (ftSize(&s->tombstones) > 0) {
        expireWithTombstones(ctx, key, keyname, o);
        return 1;
    }
    if (s->owns_key_ttl) {
        return 1;
    }
    if (!leaseActive(s, RedisModule_Milliseconds())) {
        return 0;
    }
    if (RedisModule_GetExpire(key) == REDISMODULE_NO_EXPIRE) {
        setOwnedKeyTtl(ctx, key, keyname, o, s->lease_deadline);
    }
    return 1;
}

/* Byte-wise order with a shorter prefix sorting first: the order Cassandra/Scylla use
 * to break write-timestamp ties between cell values. */
static int compareValues(RedisModuleString *a, const TairHashField *b) {
    size_t alen, blen = b->value_len;
    const char *aptr = RedisModule_StringPtrLen(a, &alen);
    const char *bptr = fieldValue(b);
    int cmp = memcmp(aptr, bptr, alen < blen ? alen : blen);
    if (cmp != 0) {
        return cmp;
    }
    return (alen > blen) - (alen < blen);
}

/* Rejects the command before it changes anything unless every version in argv[start..argc),
 * the last of each group of `step` arguments, is a positive integer. */
static int validateLwwVersions(RedisModuleCtx *ctx, RedisModuleString **argv, int start, int argc, int step) {
    long long version;
    for (int i = start; i < argc; i += step) {
        if (RedisModule_StringToLongLong(argv[i + step - 1], &version) != REDISMODULE_OK || version <= 0) {
            RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
            return REDISMODULE_ERR;
        }
    }
    return REDISMODULE_OK;
}

/* The field, unless it is absent or has expired; an expired field is deleted on a writable
 * master, as fieldExpireIfNeeded does. */
static TairHashField *liveFieldForWrite(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *name) {
    TairHashField *f = fieldLookup(o, name);
    if (f != NULL && f->expire != 0 && fieldExpireIfNeeded(ctx, dbid, key, o, name, 0)) {
        /* Gone on a master; a writable replica keeps it until the master's EXHDEL. */
        f = fieldLookup(o, name);
    }
    return f;
}

/* Applies one last-write-wins write and returns 1 if it landed: the field is absent and no
 * tombstone holds the same or a higher version, the field's version is lower, or the
 * versions tie and the value is byte-wise greater. The tie-break makes the result
 * independent of arrival order. A landed write clears the field's TTL, as EXHSET does, and
 * is added to `batch` for replication. */
static int lwwApply(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *o, RedisModuleString *name, RedisModuleString *value,
                    long long version, long long now, long long now_ms, FieldStateBatch *batch) {
    TairHashField *f = liveFieldForWrite(ctx, dbid, key, o, name);
    if (f != NULL) {
        if (version < f->version || (version == f->version && compareValues(value, f) <= 0)) {
            return 0;
        }
        setFieldExpire(ctx, dbid, key, o, name, f, 0);
        f = fieldSetValue(o, f, value);
    } else {
        size_t len;
        const char *ptr = RedisModule_StringPtrLen(name, &len);
        TairHashField *t = liveTombstone(o, ptr, len, now_ms);
        if (t != NULL && version <= t->version) {
            return 0;
        }
        f = fieldInsert(o, name, value); /* drops the tombstone */
    }

    f->version = version;
    f->seq = tairHashNextSeq(now);
    fieldStateBatchAdd(batch, name, value, f);
    return 1;
}

/* EXHMSETLWW <key> <field> <value> <version> [<field> <value> <version> ...]
 *
 * Reply: one integer per triple, 1 if the write landed and 0 if the field already held
 * a newer (or tie-winning) write or was deleted at the same or a higher version. A losing
 * write is an expected outcome of last-write-wins, not an error, so it never fails a
 * pipeline or a MULTI. */
int TairHashTypeHmsetLww_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 5 || (argc - 2) % 3 != 0) {
        return RedisModule_WrongArity(ctx);
    }

    /* Read before any key or field lookup: the clock read serializes the pipeline
     * (ISB on arm64, lfence/rdtscp on x86), stalling on any cache miss still in flight. */
    long long now = seqClockNow();
    if (validateLwwVersions(ctx, argv, 2, argc, 3) != REDISMODULE_OK || !fieldArgsFit(ctx, argv, 2, argc, 3, 1)) {
        return REDISMODULE_ERR;
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, dbid, argv[1]);
    tairHashObj *o = getOrCreateTairHash(ctx, key, argv[1]);
    if (o == NULL) {
        return REDISMODULE_ERR;
    }

    long long version, now_ms = RedisModule_Milliseconds();
    FieldStateBatch batch;
    fieldStateBatchInit(&batch, ctx, argv[1], (argc - 2) / 3);
    /* Last-write-wins state includes the write sequence, which only the state format
     * carries, and no TairHash version without these commands could apply them. */
    batch.format = REPLICATION_FORMAT_STATE;
    RedisModule_ReplyWithArray(ctx, (argc - 2) / 3);
    for (int i = 2; i < argc; i += 3) {
        RedisModule_StringToLongLong(argv[i + 2], &version);
        RedisModule_ReplyWithLongLong(ctx, lwwApply(ctx, dbid, argv[1], o, argv[i], argv[i + 1], version, now, now_ms, &batch));
    }
    fieldStateBatchFlush(&batch);
    maybePurgeTombstones(o, now_ms);
    releaseOwnedKeyTtl(ctx, key, argv[1], o);
    /* Every write lost to a tombstone in a hash created for them: leave nothing behind. */
    delEmptyTairHashIfNeeded(ctx, key, argv[1], o);
    return REDISMODULE_OK;
}

/* Fields per propagated EXHTOMBREPL, as for EXHMSETREPL. */
#define TOMBSTONE_FIELDS_PER_CMD 8192
/* EXHTOMBREPL <key> <layout> <field> <version> <deadline> [...]: layout 1. */
#define TOMBSTONE_LAYOUT 1
#define TOMBSTONE_ARGS 3

/* EXHDELLWW <key> <field> <version> [<field> <version> ...]
 *
 * Deletes each field unless it holds a write with a higher version, and leaves a tombstone
 * that writes and EXHSYNC snapshot entries with the same or a lower version lose to, until
 * lww_tombstone_ttl has passed. A hash left with only tombstones expires with them.
 *
 * Reply: one integer per pair, 1 if the field is deleted at that version (now or by an
 * earlier delete with the same or a higher version) and 0 if a newer write keeps it. */
int TairHashTypeHdelLww_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4 || (argc - 2) % 2 != 0) {
        return RedisModule_WrongArity(ctx);
    }
    if (validateLwwVersions(ctx, argv, 2, argc, 2) != REDISMODULE_OK || !fieldArgsFit(ctx, argv, 2, argc, 2, 0)) {
        return REDISMODULE_ERR;
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, dbid, argv[1]);
    tairHashObj *o = getOrCreateTairHash(ctx, key, argv[1]);
    if (o == NULL) {
        return REDISMODULE_ERR;
    }

    long long version, now_ms = RedisModule_Milliseconds(), deadline = now_ms + g_tombstone_ttl_ms;
    int pairs = (argc - 2) / 2, applied = 0;
    /* Arguments of the EXHTOMBREPL that propagates the deletes, field j at 2 + 3j. */
    RedisModuleString **repl = RedisModule_Alloc(sizeof(*repl) * (2 + TOMBSTONE_ARGS * pairs));
    RedisModuleString *deadline_str = RedisModule_CreateStringFromLongLong(ctx, deadline);
    RedisModule_ReplyWithArray(ctx, pairs);
    for (int i = 2; i < argc; i += 2) {
        RedisModule_StringToLongLong(argv[i + 1], &version);
        size_t len;
        const char *ptr = RedisModule_StringPtrLen(argv[i], &len);
        TairHashField *f = liveFieldForWrite(ctx, dbid, argv[1], o, argv[i]);
        if (f != NULL && version < f->version) {
            RedisModule_ReplyWithLongLong(ctx, 0);
            continue;
        }
        TairHashField *t = f == NULL ? liveTombstone(o, ptr, len, now_ms) : NULL;
        if (t != NULL && version <= t->version) {
            RedisModule_ReplyWithLongLong(ctx, 1);
            continue;
        }
        if (f != NULL) {
            if (f->expire) {
                g_expire_algorithm.delete(ctx, dbid, argv[1], o, argv[i], f->expire);
            }
            ftDelete(&o->hash, ptr, len);
        }
        setTombstone(o, ptr, len, version, deadline);
        repl[2 + TOMBSTONE_ARGS * applied] = argv[i];
        repl[2 + TOMBSTONE_ARGS * applied + 1] = argv[i + 1];
        repl[2 + TOMBSTONE_ARGS * applied + 2] = deadline_str;
        applied++;
        RedisModule_ReplyWithLongLong(ctx, 1);
    }
    if (applied) {
        RedisModuleString *layout = RedisModule_CreateStringFromLongLong(ctx, TOMBSTONE_LAYOUT);
        for (int first = 0; first < applied; first += TOMBSTONE_FIELDS_PER_CMD) {
            int n = applied - first < TOMBSTONE_FIELDS_PER_CMD ? applied - first : TOMBSTONE_FIELDS_PER_CMD;
            /* A later part's key and layout overwrite the end of the part before it, which
             * has been propagated already. */
            RedisModuleString **a = repl + TOMBSTONE_ARGS * first;
            a[0] = argv[1];
            a[1] = layout;
            RedisModule_Replicate(ctx, "EXHTOMBREPL", "v", a, (size_t)(2 + TOMBSTONE_ARGS * n));
        }
    }
    RedisModule_Free(repl);
    maybePurgeTombstones(o, now_ms);
    if (ftSize(&o->hash) == 0) {
        expireWithTombstones(ctx, key, argv[1], o);
    }
    return REDISMODULE_OK;
}

/* EXHSYNCGET <key> [<field> ...]
 *
 * Reply: [synced, [field, value, version, ...]]. Entries are returned only when the hash
 * is synced in the current sync epoch (an authoritative copy, see EXHSYNC); otherwise the
 * caller reads the source of truth inside EXHSYNCBEGIN and EXHSYNC. With fields given,
 * only those that exist are returned. */
int TairHashTypeSyncGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int expired = 0;
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }
    tairHashObj *o = type == REDISMODULE_KEYTYPE_EMPTY ? NULL : RedisModule_ModuleTypeGetValue(key);

    RedisModule_ReplyWithArray(ctx, 2);
    if (o == NULL || !hashIsSynced(o)) {
        RedisModule_ReplyWithLongLong(ctx, 0);
        RedisModule_ReplyWithArray(ctx, 0);
        /* A hash that was synced without fields in an earlier epoch holds nothing now, and
         * nothing else would ever remove it. */
        if (o != NULL && ftSize(&o->hash) == 0 && !isReadOnlyStatus(ctx)) {
            RedisModule_CloseKey(key);
            delEmptyTairHashIfNeeded(ctx, NULL, argv[1], o);
        }
        return REDISMODULE_OK;
    }
    RedisModule_ReplyWithLongLong(ctx, 1);

    int dbid = RedisModule_GetSelectedDb(ctx);
    long len = 0;
    TairHashField *val;
    /* For a full read without expiring fields nothing is filtered out, so the length is known up front
     * and the reply avoids a deferred length (an extra reply-list node per read). Once that length
     * is sent every field follows: none is checked against its expire. */
    int exact = argc == 2 && ftExpiring(&o->hash) == 0;
    RedisModule_ReplyWithArray(ctx, exact ? (long)ftSize(&o->hash) * 3 : REDISMODULE_POSTPONED_ARRAY_LEN);
    if (argc == 2) {
        fieldTableIterator it;
        long long now = RedisModule_Milliseconds();
        fullReadIterInit(&it, o);
        while ((val = ftIterNext(&it)) != NULL) {
            if (!exact && fullReadSkips(ctx, &expired, dbid, argv[1], o, val, now)) {
                continue;
            }
            replyWithFieldName(ctx, val);
            replyWithFieldValue(ctx, val);
            RedisModule_ReplyWithLongLong(ctx, val->version);
            len += 3;
        }
        ftIterRelease(&it);
    } else {
        for (int i = 2; i < argc; i++) {
            val = fieldLookup(o, argv[i]);
            if (val == NULL || (val->expire && readExpireIfNeeded(ctx, &expired, dbid, argv[1], o, argv[i]))) {
                continue;
            }
            RedisModule_ReplyWithString(ctx, argv[i]);
            replyWithFieldValue(ctx, val);
            RedisModule_ReplyWithLongLong(ctx, val->version);
            len += 3;
        }
    }
    if (!exact) {
        RedisModule_ReplySetArrayLength(ctx, len);
    }
    finishRead(ctx, key, argv[1], o, expired);
    return REDISMODULE_OK;
}

/* EXHSYNCBEGIN <key>
 *
 * Opens a sync of the hash with the source of truth and replies its token: read the source
 * after this call and hand the snapshot and the token to EXHSYNC. The token is valid for
 * sync_lease_ttl, until EXHSYNC uses it or until the key goes away, and concurrent syncs of a
 * hash share it. A
 * missing key is created as an empty, unsynced placeholder with a TTL of sync_lease_ttl, so
 * that an abandoned sync does not leave it behind; a write that adds a field, or EXHSYNC,
 * removes that TTL. */
int TairHashTypeSyncBegin_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }
    long long now = seqClockNow(), now_ms = RedisModule_Milliseconds();

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    tairHashObj *o = getOrCreateTairHash(ctx, key, argv[1]);
    if (o == NULL) {
        return REDISMODULE_ERR;
    }
    lwwState *s = lwwStateOf(o);
    if (!leaseActive(s, now_ms)) {
        /* Every write after this sorts above the token. */
        s->lease = tairHashNextSeq(now);
        s->lease_deadline = now_ms + g_sync_lease_ttl_ms;
        s->lease_epoch = g_sync_epoch;
    }
    /* A hash without fields that is not synced is here only for this sync and for its
     * tombstones: a new one, or one that lost its sync with an earlier epoch. It must not stay
     * behind if the sync is abandoned, so it lives as long as the lease and the tombstones,
     * unless the key has a TTL of its own. */
    if (ftSize(&o->hash) == 0 && !hashIsSynced(o)) {
        expireWithTombstones(ctx, key, argv[1], o);
        long long current = keyExpireAt(key, now_ms);
        if (current == REDISMODULE_NO_EXPIRE || (s->owns_key_ttl && current < s->lease_deadline)) {
            setOwnedKeyTtl(ctx, key, argv[1], o, s->lease_deadline);
        }
    }
    RedisModule_ReplyWithLongLong(ctx, s->lease);
    return REDISMODULE_OK;
}

/* EXHSYNC <key> <token> [<field> <value> <version> ...]
 *
 * Completes the sync EXHSYNCBEGIN opened with `token`, using a full snapshot of the source
 * of truth read after that call: snapshot fields are merged with last-write-wins
 * (EXHMSETLWW semantics, tombstones included), fields absent from the snapshot are removed
 * unless they were written after the token (those raced the snapshot read), and the hash is
 * marked synced in the current epoch. An empty snapshot leaves an empty, synced hash: "the
 * source has nothing" is a cacheable answer. Fails, changing nothing, unless the hash still
 * holds that token's lease: the lease ended, the key went away or another sync completed in
 * the meantime.
 *
 * Reply: [applied, removed]. */
int TairHashTypeSync_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3 || (argc - 3) % 3 != 0) {
        return RedisModule_WrongArity(ctx);
    }

    /* Read before any key or field lookup: the clock read serializes the pipeline
     * (ISB on arm64, lfence/rdtscp on x86), stalling on any cache miss still in flight. */
    long long now = seqClockNow();

    long long token;
    if (RedisModule_StringToLongLong(argv[2], &token) != REDISMODULE_OK || token <= 0) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNTAX);
        return REDISMODULE_ERR;
    }
    if (validateLwwVersions(ctx, argv, 3, argc, 3) != REDISMODULE_OK || !fieldArgsFit(ctx, argv, 3, argc, 3, 1)) {
        return REDISMODULE_ERR;
    }

    int dbid = RedisModule_GetSelectedDb(ctx);
    RedisModuleKey *key = openKeyAfterPassiveExpire(ctx, dbid, argv[1]);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }
    tairHashObj *o = type == REDISMODULE_KEYTYPE_EMPTY ? NULL : RedisModule_ModuleTypeGetValue(key);
    long long now_ms = RedisModule_Milliseconds();
    if (o == NULL || !leaseActive(o->lww, now_ms) || o->lww->lease != token) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_SYNC_LEASE);
        return REDISMODULE_ERR;
    }
    lwwState *s = o->lww;

    long long applied = 0, removed = 0, version;
    purgeTombstones(o, now_ms);
    /* The snapshot's field names, as value-less fields, to test membership by bytes. */
    fieldTable snapshot;
    ftInit(&snapshot);
    FieldStateBatch batch;
    fieldStateBatchInit(&batch, ctx, argv[1], (argc - 3) / 3);
    /* Last-write-wins state includes the write sequence, which only the state format
     * carries, and no TairHash version without these commands could apply them. */
    batch.format = REPLICATION_FORMAT_STATE;
    for (int i = 3; i < argc; i += 3) {
        RedisModule_StringToLongLong(argv[i + 2], &version);
        applied += lwwApply(ctx, dbid, argv[1], o, argv[i], argv[i + 1], version, now, now_ms, &batch);
        size_t name_len;
        const char *name_ptr = RedisModule_StringPtrLen(argv[i], &name_len);
        if (ftFind(&snapshot, name_ptr, name_len) == NULL) {
            ftAdd(&snapshot, fieldCreate(name_ptr, name_len, "", 0));
        }
    }
    fieldStateBatchFlush(&batch);

    /* Marked synced before the removals are propagated: replaying them must not find an
     * unsynced hash that they empty, which would delete the key and its TTL. */
    int owned_ttl = s->owns_key_ttl;
    s->synced_epoch = g_sync_epoch;
    s->lease = 0;
    s->owns_key_ttl = 0;
    replicateSyncState(ctx, argv[1], o);
    if (owned_ttl) {
        RedisModule_SetExpire(key, REDISMODULE_NO_EXPIRE);
        RedisModule_Replicate(ctx, "PERSIST", "s", argv[1]);
    }

    /* Fields absent from the snapshot are gone from the source, unless they were written
     * after the token, while the snapshot was being read; they are propagated as one EXHDEL. */
    if (ftSize(&o->hash) > 0) {
        size_t cap = 1 + ftSize(&o->hash), ndel = 0;
        RedisModuleString **del = RedisModule_Alloc(sizeof(*del) * cap);
        del[ndel++] = argv[1];
        fieldTableIterator it;
        TairHashField *f;
        ftIterInit(&it, &o->hash);
        while ((f = ftIterNext(&it)) != NULL) {
            if (f->seq > token || ftFind(&snapshot, fieldName(f), f->name_len) != NULL) {
                continue;
            }
            /* Outlives the field deleted below. */
            RedisModuleString *name = RedisModule_CreateString(NULL, fieldName(f), f->name_len);
            if (f->expire) {
                g_expire_algorithm.delete(ctx, dbid, argv[1], o, name, f->expire);
            }
            ftDelete(&o->hash, fieldName(f), f->name_len);
            del[ndel++] = name;
            removed++;
        }
        ftIterRelease(&it);
        if (ndel > 1) {
            RedisModule_Replicate(ctx, "EXHDEL", "v", del, ndel);
        }
        for (size_t i = 1; i < ndel; i++) {
            RedisModule_FreeString(NULL, del[i]);
        }
        RedisModule_Free(del);
    }
    ftClear(&snapshot);

    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, applied);
    RedisModule_ReplyWithLongLong(ctx, removed);
    return REDISMODULE_OK;
}

/* EXHSYNCREPL <key> <synced_epoch> <owns_key_ttl>
 *
 * Internal: sets the sync state of <key>, creating an empty hash if needed. Emitted by the
 * sync commands and EXHDELLWW for replicas and the AOF, and by AOF rewrite; rejected from
 * regular clients. The hash reads as synced where <synced_epoch> is the node's epoch, which a
 * replica has from EXHSYNCEPOCH, not from here. */
int TairHashTypeSyncRepl_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }
    if (!mayRestoreState(ctx)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_CMD);
        return REDISMODULE_ERR;
    }
    long long epoch, owns;
    /* The epoch is observed as a sequence, so it is bounded like one. */
    if (RedisModule_StringToLongLong(argv[2], &epoch) != REDISMODULE_OK || epoch < 0 || epoch > TAIRHASH_SEQ_MAX ||
        RedisModule_StringToLongLong(argv[3], &owns) != REDISMODULE_OK || (owns != 0 && owns != 1)) {
        return rejectStateCommand(ctx, "EXHSYNCREPL", TAIRHASH_ERRORMSG_SYNTAX);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    tairHashObj *o = getOrCreateTairHash(ctx, key, argv[1]);
    if (o == NULL) {
        return REDISMODULE_ERR;
    }
    lwwState *s = lwwStateOf(o);
    s->synced_epoch = epoch;
    s->owns_key_ttl = (int)owns;
    tairHashObserveSeq(epoch);
    if (epoch != 0) {
        g_sync_state_seen = 1;
    }
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* EXHSYNCEPOCH <epoch>
 *
 * Internal: the master started sync epoch <epoch>. Emitted by a master that renewed its
 * epoch; rejected from regular clients. A replica takes the epoch as its own, from the
 * replication stream and when it replays its AOF; a master replaying its AOF does not, as it
 * starts a new one after the load. */
int TairHashTypeSyncEpoch_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }
    if (!mayRestoreState(ctx)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_CMD);
        return REDISMODULE_ERR;
    }
    long long epoch;
    if (RedisModule_StringToLongLong(argv[1], &epoch) != REDISMODULE_OK || epoch <= 0 || epoch > TAIRHASH_SEQ_MAX) {
        return rejectStateCommand(ctx, "EXHSYNCEPOCH", TAIRHASH_ERRORMSG_SYNTAX);
    }
    adoptSyncEpoch(ctx, epoch);
    g_sync_state_seen = 1;
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* EXHTOMBREPL <key> <layout> <field> <version> <deadline> [...]
 *
 * Internal: deletes each field and sets its tombstone to the delete version and retention
 * deadline the master decided, creating the hash if needed. Emitted by EXHDELLWW and AOF
 * rewrite; rejected from regular clients. */
int TairHashTypeTombRepl_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3 + TOMBSTONE_ARGS || (argc - 3) % TOMBSTONE_ARGS != 0) {
        return RedisModule_WrongArity(ctx);
    }
    if (!mayRestoreState(ctx)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_CMD);
        return REDISMODULE_ERR;
    }
    long long layout, version, deadline;
    if (RedisModule_StringToLongLong(argv[2], &layout) != REDISMODULE_OK || layout != TOMBSTONE_LAYOUT) {
        return rejectStateCommand(ctx, "EXHTOMBREPL", TAIRHASH_ERRORMSG_REPL_LAYOUT);
    }
    /* No delete leaves a negative version or deadline, and a field that replaces the tombstone
     * starts at its version. */
    for (int i = 3; i < argc; i += TOMBSTONE_ARGS) {
        if (RedisModule_StringToLongLong(argv[i + 1], &version) != REDISMODULE_OK || RedisModule_StringToLongLong(argv[i + 2], &deadline) != REDISMODULE_OK ||
            version < 0 || deadline < 0) {
            return rejectStateCommand(ctx, "EXHTOMBREPL", TAIRHASH_ERRORMSG_SYNTAX);
        }
    }
    if (!fieldArgsFit(ctx, argv, 3, argc, TOMBSTONE_ARGS, 0)) {
        RedisModule_Log(ctx, "warning", "EXHTOMBREPL rejected, the write it carries is lost: %s", TAIRHASH_ERRORMSG_FIELD_TOO_LONG);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    tairHashObj *o = getOrCreateTairHash(ctx, key, argv[1]);
    if (o == NULL) {
        return REDISMODULE_ERR;
    }
    int dbid = RedisModule_GetSelectedDb(ctx);
    for (int i = 3; i < argc; i += TOMBSTONE_ARGS) {
        RedisModule_StringToLongLong(argv[i + 1], &version);
        RedisModule_StringToLongLong(argv[i + 2], &deadline);
        size_t len;
        const char *ptr = RedisModule_StringPtrLen(argv[i], &len);
        TairHashField *f = fieldLookup(o, argv[i]);
        if (f != NULL) {
            if (f->expire) {
                g_expire_algorithm.delete(ctx, dbid, argv[1], o, argv[i], f->expire);
            }
            ftDelete(&o->hash, ptr, len);
        }
        setTombstone(o, ptr, len, version, deadline);
    }
    maybePurgeTombstones(o, RedisModule_Milliseconds());
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* EXHMSETREPL <key> <layout> <field> <value> <version> <expire> [<seq>] [...]
 *
 * Internal: sets each field to exactly the given state (expire is an absolute unix time
 * in milliseconds, 0 for none), creating the hash if needed. Emitted by write commands
 * and AOF rewrite in the state replication format; rejected from regular clients.
 * Layout 1, from TairHash 2.0, carries no write sequence: this node assigns one.
 * Version 0 is accepted: fields written before EXHSETNX started at version 1 carry it. */
int TairHashTypeHmsetRepl_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }
    if (!mayRestoreState(ctx)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_CMD);
        return REDISMODULE_ERR;
    }
    long long layout;
    if (RedisModule_StringToLongLong(argv[2], &layout) != REDISMODULE_OK || (layout != 1 && layout != FIELD_STATE_LAYOUT)) {
        return rejectStateCommand(ctx, "EXHMSETREPL", TAIRHASH_ERRORMSG_REPL_LAYOUT);
    }
    int nargs = layout == 1 ? FIELD_STATE_ARGS_LAYOUT_1 : FIELD_STATE_ARGS;
    if (argc < 3 + nargs || (argc - 3) % nargs != 0) {
        return RedisModule_WrongArity(ctx);
    }

    /* The master decided these values; they are restored as they are. No write leaves a
     * negative version or expire, and the expire indexes and EXHVER's replies rely on that. */
    long long version, expire, seq;
    for (int i = 3; i < argc; i += nargs) {
        if (RedisModule_StringToLongLong(argv[i + 2], &version) != REDISMODULE_OK || RedisModule_StringToLongLong(argv[i + 3], &expire) != REDISMODULE_OK ||
            version < 0 || expire < 0 ||
            (nargs == FIELD_STATE_ARGS && (RedisModule_StringToLongLong(argv[i + 4], &seq) != REDISMODULE_OK || seq < 0 || seq > TAIRHASH_SEQ_MAX))) {
            return rejectStateCommand(ctx, "EXHMSETREPL", TAIRHASH_ERRORMSG_SYNTAX);
        }
    }
    if (!fieldArgsFit(ctx, argv, 3, argc, nargs, 1)) {
        RedisModule_Log(ctx, "warning", "EXHMSETREPL rejected, the write it carries is lost: %s", TAIRHASH_ERRORMSG_FIELD_TOO_LONG);
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    tairHashObj *o = getOrCreateTairHash(ctx, key, argv[1]);
    if (o == NULL) {
        RedisModule_Log(ctx, "warning", "EXHMSETREPL rejected, the write it carries is lost: %s", REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }

    long long now = nargs == FIELD_STATE_ARGS ? 0 : seqClockNow();
    int dbid = RedisModule_GetSelectedDb(ctx);
    for (int i = 3; i < argc; i += nargs) {
        RedisModuleString *field = argv[i];
        RedisModule_StringToLongLong(argv[i + 2], &version);
        RedisModule_StringToLongLong(argv[i + 3], &expire);
        if (nargs == FIELD_STATE_ARGS) {
            RedisModule_StringToLongLong(argv[i + 4], &seq);
            tairHashObserveSeq(seq);
        } else {
            seq = tairHashNextSeq(now);
        }

        TairHashField *f = fieldLookup(o, field);
        if (f != NULL) {
            f = fieldSetValue(o, f, argv[i + 1]);
        } else {
            f = fieldInsert(o, field, argv[i + 1]);
        }
        f->version = version;
        f->seq = seq;
        setFieldExpire(ctx, dbid, argv[1], o, field, f, expire);
    }
    if (o->lww && ftSize(&o->lww->tombstones)) {
        maybePurgeTombstones(o, RedisModule_Milliseconds());
    }

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* EXHMETAREPL <key> <layout> <field> <version> <expire> [...]
 *
 * Internal: sets the version and expire (an absolute unix time in milliseconds, 0 for none) of
 * existing fields to what a write that left their values alone decided. Emitted by the
 * EXHEXPIRE family, EXHPERSIST and EXHSETVER in the state replication format; rejected from
 * regular clients. A field that does not exist is an error: a node holds every field its
 * master or its AOF goes on to update. */
int TairHashTypeHmetaRepl_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3 + FIELD_META_ARGS || (argc - 3) % FIELD_META_ARGS != 0) {
        return RedisModule_WrongArity(ctx);
    }
    if (!mayRestoreState(ctx)) {
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_INTERNAL_CMD);
        return REDISMODULE_ERR;
    }
    long long layout, version, expire;
    if (RedisModule_StringToLongLong(argv[2], &layout) != REDISMODULE_OK || layout != FIELD_META_LAYOUT) {
        return rejectStateCommand(ctx, "EXHMETAREPL", TAIRHASH_ERRORMSG_REPL_LAYOUT);
    }
    for (int i = 3; i < argc; i += FIELD_META_ARGS) {
        if (RedisModule_StringToLongLong(argv[i + 1], &version) != REDISMODULE_OK || RedisModule_StringToLongLong(argv[i + 2], &expire) != REDISMODULE_OK ||
            version < 0 || expire < 0) {
            return rejectStateCommand(ctx, "EXHMETAREPL", TAIRHASH_ERRORMSG_SYNTAX);
        }
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TairHashType) {
        return rejectStateCommand(ctx, "EXHMETAREPL", REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    int missing = 0;
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        tairHashObj *o = RedisModule_ModuleTypeGetValue(key);
        int dbid = RedisModule_GetSelectedDb(ctx);
        for (int i = 3; i < argc; i += FIELD_META_ARGS) {
            TairHashField *f = fieldLookup(o, argv[i]);
            if (f == NULL) {
                missing++;
                continue;
            }
            RedisModule_StringToLongLong(argv[i + 1], &version);
            RedisModule_StringToLongLong(argv[i + 2], &expire);
            f->version = version;
            setFieldExpire(ctx, dbid, argv[1], o, argv[i], f, expire);
        }
    } else {
        missing = (argc - 3) / FIELD_META_ARGS;
    }
    RedisModule_ReplicateVerbatim(ctx);
    /* The master changed fields this node does not have: the two have diverged. The error reply
     * hands that to the server, which counts it and stops the node if it is configured to
     * (propagation-error-behavior); the fields that do exist are updated all the same. */
    if (missing) {
        RedisModule_Log(ctx, "warning", "EXHMETAREPL: %d of the fields it updates do not exist on this node", missing);
        RedisModule_ReplyWithError(ctx, TAIRHASH_ERRORMSG_REPL_NO_FIELD);
        return REDISMODULE_ERR;
    }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* ========================== "tairhashtype" type methods ======================= */


/* Whether the value is read as part of a dataset the server is loading (an RDB file, the
 * RDB of a full sync), rather than from the payload of a RESTORE. */
static int loadingDataset(RedisModuleIO *rdb) {
    RedisModuleCtx *ctx = RedisModule_GetContextFromIO ? RedisModule_GetContextFromIO(rdb) : NULL;
    return ctx == NULL || (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_LOADING) != 0;
}

void *TairHashTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver > TAIRHASH_ENCVER) {
        RedisModule_LogIOError(rdb, "warning", "tairhash: unsupported encoding version %d (this module reads up to %d)", encver, TAIRHASH_ENCVER);
        return NULL;
    }

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
    if (encver >= 1) {
        uint64_t epoch = RedisModule_LoadUnsigned(rdb);
        uint64_t owns = RedisModule_LoadUnsigned(rdb);
        uint64_t tombstones = RedisModule_LoadUnsigned(rdb);
        if (epoch > TAIRHASH_SEQ_MAX) {
            RedisModule_LogIOError(rdb, "warning", "tairhash: a sync epoch is out of range, the value is corrupt");
            tairHashTypeReleaseObject(o);
            return NULL;
        }
        tairHashObserveSeq((long long)epoch);
        /* A load restores the dataset as it was saved, a synced hash included. RESTORE does
         * not: its payload can be older than writes the hash has taken since. */
        if (epoch != 0 && !loadingDataset(rdb)) {
            epoch = 0;
        }
        if (epoch || owns || tombstones) {
            lwwState *s = lwwStateOf(o);
            s->synced_epoch = (long long)epoch;
            s->owns_key_ttl = owns != 0;
            if (epoch != 0) {
                g_sync_state_seen = 1;
            }
            if (tombstones) {
                ftExpand(&s->tombstones, tombstones < TAIRHASH_RDB_PRESIZE_MAX ? tombstones : TAIRHASH_RDB_PRESIZE_MAX);
            }
            while (tombstones--) {
                size_t name_len;
                char *name = RedisModule_LoadStringBuffer(rdb, &name_len);
                long long version = RedisModule_LoadUnsigned(rdb);
                long long deadline = RedisModule_LoadUnsigned(rdb);
                if (name_len > FT_NAME_MAX_LEN || ftFind(&s->tombstones, name, name_len) != NULL) {
                    RedisModule_LogIOError(rdb, "warning", "tairhash: a tombstone is repeated or too long, the value is corrupt");
                    RedisModule_Free(name);
                    tairHashTypeReleaseObject(o);
                    return NULL;
                }
                setTombstone(o, name, name_len, version, deadline);
                RedisModule_Free(name);
            }
        }
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
        uint64_t seq = encver >= 1 ? RedisModule_LoadUnsigned(rdb) : 0;
        if (name_len > FT_NAME_MAX_LEN || value_len > FT_VALUE_MAX_LEN || ftFind(&o->hash, name, name_len) != NULL) {
            RedisModule_LogIOError(rdb, "warning", "tairhash: a field is repeated or too long, the value is corrupt");
            RedisModule_Free(name);
            RedisModule_Free(value);
            tairHashTypeReleaseObject(o);
            return NULL;
        }
        if (seq > TAIRHASH_SEQ_MAX) {
            RedisModule_LogIOError(rdb, "warning", "tairhash: a write sequence is out of range, the value is corrupt");
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
        f->seq = (long long)seq;
        tairHashObserveSeq(f->seq);
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
    lwwState *s = o->lww;
    RedisModule_SaveUnsigned(rdb, currentSyncedEpoch(o));
    RedisModule_SaveUnsigned(rdb, s ? s->owns_key_ttl : 0);
    /* Tombstones whose retention ended stay out of the file, and of a full sync: nothing may
     * have dropped them from memory yet. The count comes first, hence the two passes. */
    long long now = RedisModule_Milliseconds();
    uint64_t tombstones = 0;
    fieldTableIterator tit;
    TairHashField *t;
    if (s) {
        ftIterInitReadOnly(&tit, &s->tombstones);
        while ((t = ftIterNext(&tit)) != NULL) {
            tombstones += t->expire > now;
        }
        ftIterRelease(&tit);
    }
    RedisModule_SaveUnsigned(rdb, tombstones);
    if (tombstones) {
        ftIterInitReadOnly(&tit, &s->tombstones);
        while ((t = ftIterNext(&tit)) != NULL) {
            if (t->expire > now) {
                RedisModule_SaveStringBuffer(rdb, fieldName(t), t->name_len);
                RedisModule_SaveUnsigned(rdb, t->version);
                RedisModule_SaveUnsigned(rdb, t->expire);
            }
        }
        ftIterRelease(&tit);
    }

    fieldTableIterator it;
    TairHashField *f;
    ftIterInitReadOnly(&it, &o->hash);
    while ((f = ftIterNext(&it)) != NULL) {
        RedisModule_SaveStringBuffer(rdb, fieldName(f), f->name_len);
        RedisModule_SaveUnsigned(rdb, f->version);
        RedisModule_SaveUnsigned(rdb, f->expire);
        RedisModule_SaveStringBuffer(rdb, fieldValue(f), f->value_len);
        RedisModule_SaveUnsigned(rdb, f->seq);
    }
    ftIterRelease(&it);
}

/* Fields per EXHMSETREPL emitted by AOF rewrite in the state format: one command per
 * field made the AOF large and its replay slow, an unbounded one would make a huge hash a
 * huge command. */
#define AOF_REWRITE_FIELDS_PER_CMD 64

/* Emits one internal state command and frees its per-field arguments (argv[0] and argv[1],
 * the key and the layout, are the caller's). */
static void emitAofCommand(RedisModuleIO *aof, const char *command, RedisModuleString **argv, int argc) {
    RedisModule_EmitAOF(aof, command, "v", argv, (size_t)argc);
    for (int i = 2; i < argc; i++) {
        if (argv[i] != g_zero_str) {
            RedisModule_FreeString(NULL, argv[i]);
        }
    }
}

static void aofRewriteState(RedisModuleIO *aof, RedisModuleString *key, tairHashObj *o) {
    RedisModuleString *argv[2 + FIELD_STATE_ARGS * AOF_REWRITE_FIELDS_PER_CMD];
    int argc = 2;
    /* The server passes a stack-allocated key, which the "v" format would try to retain. */
    argv[0] = RedisModule_CreateStringFromString(NULL, key);
    argv[1] = g_layout_str;

    fieldTableIterator it;
    TairHashField *f;
    ftIterInitReadOnly(&it, &o->hash);
    while ((f = ftIterNext(&it)) != NULL) {
        argv[argc++] = RedisModule_CreateString(NULL, fieldName(f), f->name_len);
        argv[argc++] = RedisModule_CreateString(NULL, fieldValue(f), f->value_len);
        argv[argc++] = RedisModule_CreateStringFromLongLong(NULL, f->version);
        argv[argc++] = f->expire ? RedisModule_CreateStringFromLongLong(NULL, f->expire) : g_zero_str;
        argv[argc++] = RedisModule_CreateStringFromLongLong(NULL, f->seq);
        if (argc == (int)(sizeof(argv) / sizeof(argv[0]))) {
            emitAofCommand(aof, "EXHMSETREPL", argv, argc);
            argc = 2;
        }
    }
    ftIterRelease(&it);
    if (argc > 2) {
        emitAofCommand(aof, "EXHMSETREPL", argv, argc);
    }
    RedisModule_FreeString(NULL, argv[0]);
}

static void aofRewriteCompat(RedisModuleIO *aof, RedisModuleString *key, tairHashObj *o) {
    fieldTableIterator it;
    TairHashField *f;
    ftIterInitReadOnly(&it, &o->hash);
    while ((f = ftIterNext(&it)) != NULL) {
        switch (fieldStateCommand(f->version, f->expire)) {
        case FIELD_STATE_EXHSETNX:
            RedisModule_EmitAOF(aof, "EXHSETNX", "sbb", key, fieldName(f), (size_t)f->name_len, fieldValue(f), (size_t)f->value_len);
            break;
        case FIELD_STATE_EXHSET:
            RedisModule_EmitAOF(aof, "EXHSET", "sbbcl", key, fieldName(f), (size_t)f->name_len, fieldValue(f), (size_t)f->value_len, "ABS", f->version);
            break;
        case FIELD_STATE_EXHSET_PXAT:
            RedisModule_EmitAOF(aof, "EXHSET", "sbbclcl", key, fieldName(f), (size_t)f->name_len, fieldValue(f), (size_t)f->value_len, "ABS", f->version,
                                "PXAT", f->expire);
            break;
        }
    }
    ftIterRelease(&it);
}

/* Emits a hash's sync state and live tombstones. No TairHash version without the LWW
 * commands has either, so this does not depend on the replication format. */
static void aofRewriteLww(RedisModuleIO *aof, RedisModuleString *key, tairHashObj *o) {
    lwwState *s = o->lww;
    if (s == NULL) {
        return;
    }
    /* First, so that a hash without fields is recreated too. */
    if (currentSyncedEpoch(o) || s->owns_key_ttl) {
        RedisModule_EmitAOF(aof, "EXHSYNCREPL", "sll", key, currentSyncedEpoch(o), (long long)s->owns_key_ttl);
    }
    if (ftSize(&s->tombstones) == 0) {
        return;
    }
    RedisModuleString *argv[2 + TOMBSTONE_ARGS * AOF_REWRITE_FIELDS_PER_CMD];
    int argc = 2;
    /* The server passes a stack-allocated key, which the "v" format would try to retain. */
    argv[0] = RedisModule_CreateStringFromString(NULL, key);
    argv[1] = RedisModule_CreateStringFromLongLong(NULL, TOMBSTONE_LAYOUT);
    long long now = RedisModule_Milliseconds();
    fieldTableIterator it;
    TairHashField *t;
    ftIterInitReadOnly(&it, &s->tombstones);
    while ((t = ftIterNext(&it)) != NULL) {
        if (t->expire <= now) {
            continue;
        }
        argv[argc++] = RedisModule_CreateString(NULL, fieldName(t), t->name_len);
        argv[argc++] = RedisModule_CreateStringFromLongLong(NULL, t->version);
        argv[argc++] = RedisModule_CreateStringFromLongLong(NULL, t->expire);
        if (argc == (int)(sizeof(argv) / sizeof(argv[0]))) {
            emitAofCommand(aof, "EXHTOMBREPL", argv, argc);
            argc = 2;
        }
    }
    ftIterRelease(&it);
    if (argc > 2) {
        emitAofCommand(aof, "EXHTOMBREPL", argv, argc);
    }
    RedisModule_FreeString(NULL, argv[0]);
    RedisModule_FreeString(NULL, argv[1]);
}

/* Emits every field, expired or not: the commands logged after the fork apply to the hash as
 * it was at the fork, where a field that expires while the rewrite runs still exists and may
 * since have had its TTL extended. Fields that did expire are removed after loading. */
void TairHashTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    tairHashObj *o = (tairHashObj *)value;
    aofRewriteLww(aof, key, o);
    if (g_replication_format == REPLICATION_FORMAT_STATE) {
        aofRewriteState(aof, key, value);
    } else {
        aofRewriteCompat(aof, key, value);
    }
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
    if (o->lww) {
        size += allocSize(o->lww, sizeof(*o->lww)) + ftMemUsage(&o->lww->tombstones);
    }
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

    /* The copy gets no LWW state: its sync and tombstones were about another key. */
    new->key = RedisModule_CreateStringFromString(NULL, tokey);
    ftExpand(&new->hash, ftSize(&old->hash));

    fieldTableIterator it;
    TairHashField *f;
    ftIterInitReadOnly(&it, &old->hash);
    while ((f = ftIterNext(&it)) != NULL) {
        TairHashField *copy = fieldCreate(fieldName(f), f->name_len, fieldValue(f), f->value_len);
        copy->version = f->version;
        copy->expire = f->expire;
        copy->seq = f->seq;
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

    /* Everything replication and persistence must reproduce, so DEBUG DIGEST catches a
     * replica or a reload that diverged in version or TTL too. The write sequence is left out:
     * the compat format and EXHMSETREPL layout 1 carry none, so each node assigns its own, and
     * a node compares sequences only with the sync tokens it issued itself. */
    fieldTableIterator it;
    TairHashField *f;
    ftIterInitReadOnly(&it, &o->hash);
    while ((f = ftIterNext(&it)) != NULL) {
        RedisModule_DigestAddStringBuffer(md, (unsigned char *)fieldName(f), f->name_len);
        RedisModule_DigestAddStringBuffer(md, (unsigned char *)fieldValue(f), f->value_len);
        RedisModule_DigestAddLongLong(md, f->version);
        RedisModule_DigestAddLongLong(md, f->expire);
        RedisModule_DigestEndSequence(md);
    }
    ftIterRelease(&it);

    /* Tombstones are left out: each node drops expired ones by its own clock. */
    RedisModule_DigestAddLongLong(md, currentSyncedEpoch(o));
    RedisModule_DigestAddLongLong(md, o->lww ? o->lww->owns_key_ttl : 0);
    RedisModule_DigestEndSequence(md);

    RedisModule_DigestEndSequence(md);
}

/* The sync epoch travels with the RDB: a replica takes the one its master had when it wrote the
 * RDB of a full sync, and after a restart the one it had itself when it saved. */
static void TairHashTypeAuxSave(RedisModuleIO *rdb, int when) {
    REDISMODULE_NOT_USED(when);
    RedisModule_SaveUnsigned(rdb, (uint64_t)g_sync_epoch);
}

static int TairHashTypeAuxLoad(RedisModuleIO *rdb, int encver, int when) {
    REDISMODULE_NOT_USED(when);
    if (encver > TAIRHASH_ENCVER) {
        RedisModule_LogIOError(rdb, "warning", "tairhash: the RDB was written by a newer TairHash, encoding version %d", encver);
        return REDISMODULE_ERR;
    }
    uint64_t epoch = RedisModule_LoadUnsigned(rdb);
    if (RedisModule_IsIOError && RedisModule_IsIOError(rdb)) {
        return REDISMODULE_ERR;
    }
    if (epoch > TAIRHASH_SEQ_MAX) {
        RedisModule_LogIOError(rdb, "warning", "tairhash: the sync epoch is out of range, the RDB is corrupt");
        return REDISMODULE_ERR;
    }
    RedisModuleCtx *ctx = RedisModule_GetContextFromIO ? RedisModule_GetContextFromIO(rdb) : NULL;
    if (ctx) {
        adoptSyncEpoch(ctx, (long long)epoch);
    } else {
        tairHashObserveSeq((long long)epoch);
    }
    return REDISMODULE_OK;
}

/* A replica takes the epoch of the data it loads, its master's dataset or its own files, and
 * has none if they hold none. A master that loaded an RDB or an AOF in place of its dataset
 * (a restart, DEBUG RELOAD, an older file) can have lost writes that synced hashes reflected:
 * it starts a new epoch once the load is over, when it has seen every epoch in the file. */
static void loadingSyncEpochCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    REDISMODULE_NOT_USED(e);
    REDISMODULE_NOT_USED(data);
    int replica = nodeIsReplica(ctx);
    if (sub == REDISMODULE_SUBEVENT_LOADING_RDB_START || sub == REDISMODULE_SUBEVENT_LOADING_AOF_START ||
        sub == REDISMODULE_SUBEVENT_LOADING_REPL_START) {
        if (replica) {
            g_sync_epoch = 0;
        }
    } else if ((sub == REDISMODULE_SUBEVENT_LOADING_ENDED || sub == REDISMODULE_SUBEVENT_LOADING_FAILED) && !replica) {
        renewSyncEpoch();
        if (g_sync_state_seen) {
            announceSyncEpochSoon(ctx, 0);
        }
    }
}

/* A replica promoted to master may lack writes its old master had acknowledged. A master that
 * becomes a replica has no epoch until it loads or applies its new master's, which a
 * promoted master always sends it. A replica that only changes master keeps the epoch of the
 * data it has: if the stream it continues holds no later one, that is still its epoch.
 * Decided by the event rather than the context flags, which need not reflect the new role
 * yet. */
static void roleChangedSyncEpochCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    REDISMODULE_NOT_USED(e);
    REDISMODULE_NOT_USED(data);
    if (sub == REDISMODULE_EVENT_REPLROLECHANGED_NOW_MASTER) {
        g_sync_role_replica = 0;
        renewSyncEpoch();
        if (g_sync_state_seen) {
            announceSyncEpochSoon(ctx, 0);
        }
    } else {
        if (!g_sync_role_replica) {
            g_sync_epoch = 0;
        }
        g_sync_role_replica = 1;
        g_sync_epoch_unannounced = 0;
    }
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
    CREATE_WRCMD("exhmsetrepl", TairHashTypeHmsetRepl_RedisCommand)
    CREATE_WRCMD("exhmetarepl", TairHashTypeHmetaRepl_RedisCommand)
    CREATE_WRCMD("exhmsetlww", TairHashTypeHmsetLww_RedisCommand)
    CREATE_WRCMD("exhdellww", TairHashTypeHdelLww_RedisCommand)
    /* A synced hash is only as good as the epoch, which needs the loading and role change
     * events (Redis 6.0+) to be renewed; without them the sync commands are left out. */
    if (RedisModule_SubscribeToServerEvent) {
        CREATE_WRCMD("exhsyncbegin", TairHashTypeSyncBegin_RedisCommand)
        CREATE_WRCMD("exhsync", TairHashTypeSync_RedisCommand)
        /* O(N) without a field list, so not "fast". */
        CREATE_CMD("exhsyncget", TairHashTypeSyncGet_RedisCommand, "readonly", 1, 1, 1)
    }
    CREATE_WRCMD("exhsyncrepl", TairHashTypeSyncRepl_RedisCommand)
    CREATE_CMD("exhsyncepoch", TairHashTypeSyncEpoch_RedisCommand, "write", 0, 0, 0)
    CREATE_WRCMD("exhtombrepl", TairHashTypeTombRepl_RedisCommand)

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

/* Module configs (Redis 7.0+) are resolved at load time: the vendored redismodule.h predates them. */
typedef int (*ConfigGetEnumFunc)(const char *name, void *privdata);
typedef int (*ConfigSetEnumFunc)(const char *name, int val, void *privdata, RedisModuleString **err);
typedef int (*ConfigApplyFunc)(RedisModuleCtx *ctx, void *privdata, RedisModuleString **err);
typedef int (*RegisterEnumConfigFunc)(RedisModuleCtx *ctx, const char *name, int default_val, unsigned int flags, const char **enum_values,
                                      const int *int_values, int num_enum_vals, ConfigGetEnumFunc getfn, ConfigSetEnumFunc setfn,
                                      ConfigApplyFunc applyfn, void *privdata);
typedef int (*LoadConfigsFunc)(RedisModuleCtx *ctx);

static const char *g_replication_format_names[] = {"state", "compat"};
static const int g_replication_format_values[] = {REPLICATION_FORMAT_STATE, REPLICATION_FORMAT_COMPAT};

static int parseReplicationFormat(RedisModuleString *arg, int *format) {
    for (size_t i = 0; i < sizeof(g_replication_format_names) / sizeof(g_replication_format_names[0]); i++) {
        if (!mstrcasecmp(arg, g_replication_format_names[i])) {
            *format = g_replication_format_values[i];
            return REDISMODULE_OK;
        }
    }
    return REDISMODULE_ERR;
}

static int getReplicationFormat(const char *name, void *privdata) {
    REDISMODULE_NOT_USED(name);
    REDISMODULE_NOT_USED(privdata);
    return g_replication_format;
}

static int setReplicationFormat(const char *name, int val, void *privdata, RedisModuleString **err) {
    REDISMODULE_NOT_USED(name);
    REDISMODULE_NOT_USED(privdata);
    REDISMODULE_NOT_USED(err);
    g_replication_format = val;
    return REDISMODULE_OK;
}

/* On Redis 7.0+ (and Valkey) replication_format is also a module config,
 * tairhash.replication_format, so CONFIG SET can switch it at runtime: a rolling upgrade
 * from 1.x runs in compat until every node runs 2.x, then switches to state. */
static int registerReplicationFormatConfig(RedisModuleCtx *ctx) {
    RegisterEnumConfigFunc register_enum_config = NULL;
    LoadConfigsFunc load_configs = NULL;
    if (RedisModule_GetApi("RedisModule_RegisterEnumConfig", (void **)&register_enum_config) != REDISMODULE_OK || register_enum_config == NULL ||
        RedisModule_GetApi("RedisModule_LoadConfigs", (void **)&load_configs) != REDISMODULE_OK || load_configs == NULL) {
        return REDISMODULE_OK;
    }
    if (register_enum_config(ctx, "replication_format", g_replication_format, 0, g_replication_format_names, g_replication_format_values,
                             (int)(sizeof(g_replication_format_names) / sizeof(g_replication_format_names[0])), getReplicationFormat,
                             setReplicationFormat, NULL, NULL) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "Failed to register the replication_format config");
        return REDISMODULE_ERR;
    }
    return load_configs(ctx);
}

int __attribute__((visibility("default"))) RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "tairhash", TAIRHASH_MODULE_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
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
        RedisModule_Log(ctx, "warning",
                        "Redis version (%d.%d.%d) is too old for the sort and slab expire modes: upgrade to 7.0.0 or above, or build with "
                        "-DTAIRHASH_EXPIRE_MODE=scan",
                        redis_major_ver, redis_minor_ver, redis_patch_ver);
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
        } else if (!mstrcasecmp(argv[ii], "lww_tombstone_ttl") || !mstrcasecmp(argv[ii], "sync_lease_ttl")) {
            long long v;
            if (RedisModule_StringToLongLong(argv[ii + 1], &v) == REDISMODULE_ERR || v <= 0 || v > TAIRHASH_RETENTION_MAX_SEC) {
                RedisModule_Log(ctx, "warning", "Invalid argument for %s: expected a number of seconds from 1 to %lld",
                                RedisModule_StringPtrLen(argv[ii], NULL), (long long)TAIRHASH_RETENTION_MAX_SEC);
                return REDISMODULE_ERR;
            }
            if (!mstrcasecmp(argv[ii], "lww_tombstone_ttl")) {
                g_tombstone_ttl_ms = v * 1000;
            } else {
                g_sync_lease_ttl_ms = v * 1000;
            }
        } else if (!mstrcasecmp(argv[ii], "replication_format")) {
            if (parseReplicationFormat(argv[ii + 1], &g_replication_format) != REDISMODULE_OK) {
                RedisModule_Log(ctx, "warning", "Invalid argument for replication_format: expected state or compat");
                return REDISMODULE_ERR;
            }
        } else {
            RedisModule_Log(ctx, "warning", "Unrecognized option");
            return REDISMODULE_ERR;
        }
    }
    /* A sync must not accept a snapshot read before a delete once the delete's tombstone is
     * gone: the tombstone has to outlive any lease that was open when it was set. */
    if (g_tombstone_ttl_ms < g_sync_lease_ttl_ms) {
        RedisModule_Log(ctx, "warning", "Invalid arguments: lww_tombstone_ttl (%lld) must not be shorter than sync_lease_ttl (%lld)",
                        g_tombstone_ttl_ms / 1000, g_sync_lease_ttl_ms / 1000);
        return REDISMODULE_ERR;
    }

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = TairHashTypeRdbLoad,
        .rdb_save = TairHashTypeRdbSave,
        .aof_rewrite = TairHashTypeAofRewrite,
        .aux_load = TairHashTypeAuxLoad,
        .aux_save = TairHashTypeAuxSave,
        .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB,
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

    TairHashType = RedisModule_CreateDataType(ctx, "tairhash-", TAIRHASH_ENCVER, &tm);
    if (TairHashType == NULL)
        return REDISMODULE_ERR;

    ftModuleInit();
    if (RedisModule_GetApi("RedisModule_MallocSizeString", (void **)&g_malloc_size_string) != REDISMODULE_OK) {
        g_malloc_size_string = NULL;
    }
    if (RedisModule_GetApi("RedisModule_CachedMicroseconds", (void **)&g_cached_microseconds) != REDISMODULE_OK) {
        g_cached_microseconds = NULL;
    }
    g_layout_str = RedisModule_CreateStringFromLongLong(NULL, FIELD_STATE_LAYOUT);
    g_zero_str = RedisModule_CreateString(NULL, "0", 1);

    int (*get_context_flags_all)(void) = NULL;
    if (RedisModule_GetApi("RedisModule_GetContextFlagsAll", (void **)&get_context_flags_all) == REDISMODULE_OK && get_context_flags_all) {
        int restoring_flags = REDISMODULE_CTX_FLAGS_REPLICATED | REDISMODULE_CTX_FLAGS_LOADING;
        g_can_identify_restoring = (get_context_flags_all() & restoring_flags) == restoring_flags;
    }

    if (registerReplicationFormatConfig(ctx) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    if (REDISMODULE_ERR == Module_CreateCommands(ctx)) {
        return REDISMODULE_ERR;
    }

    /* Nothing below fails: a load that returns an error above has allocated nothing to leak. */
    g_expire_algorithm.db_num = serverDbCount();
    g_expire_algorithm.stat_active_expired_field = RedisModule_Calloc(g_expire_algorithm.db_num, sizeof(uint64_t));
    g_expire_algorithm.stat_passive_expired_field = RedisModule_Calloc(g_expire_algorithm.db_num, sizeof(uint64_t));

    g_sync_role_replica = nodeIsReplica(ctx);
    if (!g_sync_role_replica) {
        renewSyncEpoch();
    }
    if (RedisModule_SubscribeToServerEvent) {
        RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading, loadingSyncEpochCallback);
        RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ReplicationRoleChanged, roleChangedSyncEpochCallback);
    }

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
