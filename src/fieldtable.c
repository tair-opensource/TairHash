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

/* Rehashing, resizing and the scan cursor follow dep/dict.c (Redis's dict). */

#include "fieldtable.h"

#include <limits.h>
#include <stdint.h>
#include <string.h>

#include "alloc_size.h"
#include "dict.h"
#include "redismodule.h"

#define FT_INITIAL_SIZE 4

/* A field keeps its allocation for a smaller value unless that wastes more than this many
 * bytes and more than a quarter of the allocation: allocator size classes are up to a quarter
 * apart, so a smaller share would not free anything. */
#define FT_SHRINK_SLACK 64

/* RedisModule_MallocUsableSize (Redis 7.0+), resolved by ftModuleInit. RedisModule_MallocSize
 * is no measure of capacity: where the server's allocator keeps a size header in front of each
 * block, it includes the header. */
static size_t (*ft_usable_size)(void *ptr) = NULL;

void ftModuleInit(void) {
    if (RedisModule_GetApi("RedisModule_MallocUsableSize", (void **)&ft_usable_size) != REDISMODULE_OK) {
        ft_usable_size = NULL;
    }
}

/* How many bytes of f may be written: the allocation's usable size where the server reports
 * it, and otherwise the size the field was allocated with. */
static size_t fieldCapacity(TairHashField *f) {
    return ft_usable_size ? ft_usable_size(f) : sizeof(*f) + f->name_len + f->value_len;
}

TairHashField *fieldCreate(const char *name, size_t name_len, const char *value, size_t value_len) {
    TairHashField *f = RedisModule_Alloc(sizeof(*f) + name_len + value_len);
    f->next = NULL;
    f->version = 0;
    f->expire = 0;
    f->name_len = (uint32_t)name_len;
    f->value_len = (uint32_t)value_len;
    memcpy(f->data, name, name_len);
    memcpy(f->data + name_len, value, value_len);
    return f;
}

void fieldFree(TairHashField *f) {
    RedisModule_Free(f);
}

static uint64_t hashName(const char *name, size_t len) {
    return m_dictGenHashFunction(name, (int)len);
}

static int nameEquals(const TairHashField *f, const char *name, size_t len) {
    return f->name_len == len && memcmp(f->data, name, len) == 0;
}

static int isRehashing(const fieldTable *t) {
    return t->rehashidx != -1;
}

void ftInit(fieldTable *t) {
    memset(t, 0, sizeof(*t));
    t->rehashidx = -1;
}

void ftClear(fieldTable *t) {
    for (int i = 0; i < 2; i++) {
        for (unsigned long b = 0; b < t->size[i]; b++) {
            TairHashField *f = t->table[i][b];
            while (f) {
                TairHashField *next = f->next;
                fieldFree(f);
                f = next;
            }
        }
        if (t->table[i]) {
            RedisModule_Free(t->table[i]);
        }
    }
    ftInit(t);
}

static unsigned long nextPower(unsigned long size) {
    unsigned long i = FT_INITIAL_SIZE;
    if (size >= LONG_MAX) {
        return LONG_MAX + 1LU;
    }
    while (i < size) {
        i *= 2;
    }
    return i;
}

void ftExpand(fieldTable *t, unsigned long size) {
    if (isRehashing(t) || t->used[0] > size) {
        return;
    }
    unsigned long realsize = nextPower(size);
    if (realsize == t->size[0] || realsize > SIZE_MAX / sizeof(TairHashField *)) {
        return;
    }
    TairHashField **table = RedisModule_Calloc(realsize, sizeof(*table));
    if (t->table[0] == NULL) {
        t->table[0] = table;
        t->size[0] = realsize;
        return;
    }
    t->table[1] = table;
    t->size[1] = realsize;
    t->used[1] = 0;
    t->rehashidx = 0;
}

/* Moves up to n buckets of table[0] to table[1], visiting at most n * 10 empty ones. */
static void rehash(fieldTable *t, int n) {
    int empty_visits = n * 10;
    while (n-- && t->used[0] != 0) {
        while (t->table[0][t->rehashidx] == NULL) {
            t->rehashidx++;
            if (--empty_visits == 0) {
                return;
            }
        }
        TairHashField *f = t->table[0][t->rehashidx];
        while (f) {
            TairHashField *next = f->next;
            unsigned long idx = hashName(f->data, f->name_len) & (t->size[1] - 1);
            f->next = t->table[1][idx];
            t->table[1][idx] = f;
            t->used[0]--;
            t->used[1]++;
            f = next;
        }
        t->table[0][t->rehashidx] = NULL;
        t->rehashidx++;
    }
    if (t->used[0] == 0) {
        RedisModule_Free(t->table[0]);
        t->table[0] = t->table[1];
        t->size[0] = t->size[1];
        t->used[0] = t->used[1];
        t->table[1] = NULL;
        t->size[1] = 0;
        t->used[1] = 0;
        t->rehashidx = -1;
    }
}

static void rehashStep(fieldTable *t) {
    if (isRehashing(t) && t->pause == 0) {
        rehash(t, 1);
    }
}

/* The slot holding the field with this name (or the empty slot ending its chain). */
static TairHashField **findSlot(fieldTable *t, const char *name, size_t len, uint64_t hash) {
    TairHashField **slot = NULL;
    for (int i = 0; i <= 1; i++) {
        if (t->size[i] == 0) {
            break;
        }
        slot = &t->table[i][hash & (t->size[i] - 1)];
        while (*slot) {
            if (nameEquals(*slot, name, len)) {
                return slot;
            }
            slot = &(*slot)->next;
        }
        if (!isRehashing(t)) {
            break;
        }
    }
    return slot;
}

TairHashField *ftFind(fieldTable *t, const char *name, size_t len) {
    if (ftSize(t) == 0 || len > FT_NAME_MAX_LEN) {
        return NULL;
    }
    rehashStep(t);
    TairHashField **slot = findSlot(t, name, len, hashName(name, len));
    return slot ? *slot : NULL;
}

void ftAdd(fieldTable *t, TairHashField *f) {
    rehashStep(t);
    if (!isRehashing(t)) {
        if (t->size[0] == 0) {
            ftExpand(t, FT_INITIAL_SIZE);
        } else if (t->used[0] >= t->size[0]) {
            ftExpand(t, t->used[0] * 2);
        }
    }
    /* While rehashing, new fields go to the new table. */
    int i = isRehashing(t) ? 1 : 0;
    unsigned long idx = hashName(f->data, f->name_len) & (t->size[i] - 1);
    f->next = t->table[i][idx];
    t->table[i][idx] = f;
    t->used[i]++;
    if (f->expire) {
        t->expiring++;
    }
}

TairHashField *ftUnlink(fieldTable *t, const char *name, size_t len) {
    if (ftSize(t) == 0 || len > FT_NAME_MAX_LEN) {
        return NULL;
    }
    rehashStep(t);
    uint64_t hash = hashName(name, len);
    for (int i = 0; i <= 1; i++) {
        if (t->size[i] == 0) {
            break;
        }
        TairHashField **slot = &t->table[i][hash & (t->size[i] - 1)];
        while (*slot) {
            if (nameEquals(*slot, name, len)) {
                TairHashField *f = *slot;
                *slot = f->next;
                f->next = NULL;
                t->used[i]--;
                if (f->expire) {
                    t->expiring--;
                }
                return f;
            }
            slot = &(*slot)->next;
        }
        if (!isRehashing(t)) {
            break;
        }
    }
    return NULL;
}

int ftDelete(fieldTable *t, const char *name, size_t len) {
    TairHashField *f = ftUnlink(t, name, len);
    if (f == NULL) {
        return 0;
    }
    fieldFree(f);
    return 1;
}

TairHashField *ftSetValue(fieldTable *t, TairHashField *f, const char *value, size_t len) {
    size_t need = sizeof(*f) + f->name_len + len;
    size_t have = fieldCapacity(f);
    if (need <= have && (have - need <= FT_SHRINK_SLACK || have - need < have / 4)) {
        memcpy(f->data + f->name_len, value, len);
        f->value_len = (uint32_t)len;
        return f;
    }

    /* The field moves: repoint the slot that holds it. Rehashing never moves fields in
     * memory, only between chains, so the slot found now stays valid. */
    TairHashField **slot = findSlot(t, f->data, f->name_len, hashName(f->data, f->name_len));
    TairHashField *moved = RedisModule_Realloc(f, need);
    memcpy(moved->data + moved->name_len, value, len);
    moved->value_len = (uint32_t)len;
    *slot = moved;
    return moved;
}

void ftSetExpire(fieldTable *t, TairHashField *f, long long expire) {
    if (f->expire == 0 && expire != 0) {
        t->expiring++;
    } else if (f->expire != 0 && expire == 0) {
        t->expiring--;
    }
    f->expire = expire;
}

void ftIterInit(fieldTableIterator *it, fieldTable *t) {
    it->t = t;
    it->table = 0;
    it->index = 0;
    it->next = NULL;
    it->started = 0;
    it->paused = 1;
    t->pause++;
}

void ftIterInitReadOnly(fieldTableIterator *it, const fieldTable *t) {
    it->t = (fieldTable *)t;
    it->table = 0;
    it->index = 0;
    it->next = NULL;
    it->started = 0;
    it->paused = 0;
}

TairHashField *ftIterNext(fieldTableIterator *it) {
    fieldTable *t = it->t;
    if (!it->started) {
        it->started = 1;
        it->next = t->size[0] ? t->table[0][0] : NULL;
    }
    while (it->next == NULL) {
        it->index++;
        if (it->index >= t->size[it->table]) {
            if (it->table == 1 || !isRehashing(t)) {
                return NULL;
            }
            it->table = 1;
            it->index = 0;
        }
        if (t->size[it->table] == 0) {
            return NULL;
        }
        it->next = t->table[it->table][it->index];
    }
    TairHashField *f = it->next;
    /* Saved now so the caller may remove f. */
    it->next = f->next;
    return f;
}

void ftIterRelease(fieldTableIterator *it) {
    if (it->paused) {
        it->t->pause--;
    }
}

static unsigned long rev(unsigned long v) {
    unsigned long s = 8 * sizeof(v);
    unsigned long mask = ~0UL;
    while ((s >>= 1) > 0) {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

static void emitBucket(TairHashField *f, void (*fn)(void *privdata, TairHashField *f), void *privdata) {
    while (f) {
        TairHashField *next = f->next;
        fn(privdata, f);
        f = next;
    }
}

unsigned long ftScan(fieldTable *t, unsigned long v, void (*fn)(void *privdata, TairHashField *f), void *privdata) {
    if (ftSize(t) == 0) {
        return 0;
    }
    if (!isRehashing(t)) {
        unsigned long m0 = t->size[0] - 1;
        emitBucket(t->table[0][v & m0], fn, privdata);
        v |= ~m0;
        v = rev(v);
        v++;
        return rev(v);
    }

    /* Visit the bucket of the smaller table, then every bucket of the larger table it
     * expands to. */
    int small = t->size[0] <= t->size[1] ? 0 : 1, large = 1 - small;
    unsigned long m0 = t->size[small] - 1, m1 = t->size[large] - 1;
    emitBucket(t->table[small][v & m0], fn, privdata);
    do {
        emitBucket(t->table[large][v & m1], fn, privdata);
        v |= ~m1;
        v = rev(v);
        v++;
        v = rev(v);
    } while (v & (m0 ^ m1));
    return v;
}

size_t ftMemUsage(const fieldTable *t) {
    size_t size = 0;
    for (int i = 0; i < 2; i++) {
        if (t->table[i] == NULL) {
            continue;
        }
        size += allocSize(t->table[i], t->size[i] * sizeof(*t->table[i]));
        for (unsigned long b = 0; b < t->size[i]; b++) {
            for (TairHashField *f = t->table[i][b]; f; f = f->next) {
                size += allocSize(f, sizeof(*f) + f->name_len + f->value_len);
            }
        }
    }
    return size;
}
