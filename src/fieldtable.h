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
#include <stddef.h>
#include <stdint.h>

/* Longest field name: hashing and glob matching take the length as an int. */
#define FT_NAME_MAX_LEN INT_MAX
/* Longest field value: a field stores its lengths in 32 bits. */
#define FT_VALUE_MAX_LEN UINT32_MAX

/*
 * One field of a tairhash: a single allocation holding the field's metadata, its name
 * and its value, and itself the node of its hash bucket's chain. A dict entry pointing
 * at a value struct and two string objects took four allocations and four cache lines
 * per field; this takes one of each for short fields.
 */
typedef struct TairHashField {
    struct TairHashField *next;
    long long version;
    long long expire; /* absolute unix time in milliseconds, 0 for none; ftSetExpire once linked */
    uint32_t name_len;
    uint32_t value_len;
    char data[]; /* name bytes, then value bytes */
} TairHashField;

static inline const char *fieldName(const TairHashField *f) {
    return f->data;
}

static inline const char *fieldValue(const TairHashField *f) {
    return f->data + f->name_len;
}

/* A new, unlinked field holding copies of name and value; version and expire are 0. The
 * caller checked the lengths against FT_NAME_MAX_LEN and FT_VALUE_MAX_LEN. */
TairHashField *fieldCreate(const char *name, size_t name_len, const char *value, size_t value_len);
void fieldFree(TairHashField *f);

/*
 * A chained hash table of fields, keyed by name, with the incremental rehashing of
 * Redis's dict: growing a large table is spread over later operations instead of
 * blocking. Iterating pauses rehashing, so the field an iterator just returned may be
 * removed (and only that one) before asking for the next.
 */
typedef struct fieldTable {
    TairHashField **table[2];
    unsigned long size[2]; /* 0 or a power of two */
    unsigned long used[2];
    long rehashidx;         /* next bucket of table[0] to move, -1 when not rehashing */
    unsigned long pause;    /* iterators running; no rehash steps while non-zero */
    unsigned long expiring; /* fields with an expire */
} fieldTable;

typedef struct fieldTableIterator {
    fieldTable *t;
    int table;
    unsigned long index;
    TairHashField *next;
    int started;
    int paused; /* this iterator holds t->pause */
} fieldTableIterator;

/* Resolves the allocator calls the table uses; call once at module load. */
void ftModuleInit(void);

void ftInit(fieldTable *t);
/* Frees every field and the bucket arrays, leaving an empty table. */
void ftClear(fieldTable *t);

static inline unsigned long ftSize(const fieldTable *t) {
    return t->used[0] + t->used[1];
}

/* How many fields have an expire. The table counts them itself (ftAdd, ftUnlink,
 * ftSetExpire), so zero means that no field of it can have expired. */
static inline unsigned long ftExpiring(const fieldTable *t) {
    return t->expiring;
}

/* A name longer than FT_NAME_MAX_LEN is in no table; looking it up or unlinking it is safe. */
TairHashField *ftFind(fieldTable *t, const char *name, size_t len);
/* Links f, whose name must not be in the table yet. */
void ftAdd(fieldTable *t, TairHashField *f);
/* Unlinks and returns the field, or NULL; the caller owns it. */
TairHashField *ftUnlink(fieldTable *t, const char *name, size_t len);
/* Unlinks and frees the field; returns 1 if it existed. */
int ftDelete(fieldTable *t, const char *name, size_t len);
/* Grows the table to hold at least `size` fields without rehashing. */
void ftExpand(fieldTable *t, unsigned long size);

/*
 * Replaces f's value and returns the field, which moves when the value no longer fits its
 * allocation (or now fits a much smaller one): callers must use the returned pointer.
 * `value` must not point into f.
 */
TairHashField *ftSetValue(fieldTable *t, TairHashField *f, const char *value, size_t len);
/* Sets the expire of f, a field linked in t (0 for none). Assigning f->expire instead
 * would leave ftExpiring wrong. */
void ftSetExpire(fieldTable *t, TairHashField *f, long long expire);

void ftIterInit(fieldTableIterator *it, fieldTable *t);
/*
 * Iterates without pausing rehashing, so it writes nothing to the table: for loops that
 * neither look up, add nor remove fields until ftIterRelease, such as RDB and AOF rewrite in
 * a fork child, where writing to the table would copy its page.
 */
void ftIterInitReadOnly(fieldTableIterator *it, const fieldTable *t);
TairHashField *ftIterNext(fieldTableIterator *it);
void ftIterRelease(fieldTableIterator *it);

/*
 * Calls fn for the fields of one bucket and returns the next cursor (0 when done), with
 * Redis's SCAN guarantees: every field present for the whole scan is returned at least
 * once, even across rehashing. fn must not modify the table.
 */
unsigned long ftScan(fieldTable *t, unsigned long cursor, void (*fn)(void *privdata, TairHashField *f), void *privdata);

/* Bytes held in the allocator by the bucket arrays and the fields. */
size_t ftMemUsage(const fieldTable *t);
