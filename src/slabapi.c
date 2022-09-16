#include "slabapi.h"

#include <cpuid.h>
#include <immintrin.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "assert.h"
#include "tairhash_skiplist.h"

#define RELAXATION 10
#ifdef __AVX2__
__m256i shuffle_timeout_mask_4x64[16], shuffle_ontime_mask_4x64[16];
#endif

int partition(Slab *slab, int low, int high) {
    long long expire = slab->expires[low];
    RedisModuleString *key = slab->keys[low];
    while (low < high) {
        while (low < high && (slab->expires[high] > expire || (slab->expires[high] == expire && RedisModule_StringCompare(slab->keys[high], key) >= 0)))
            high--;
        if (low < high)
            slab->expires[low] = slab->expires[high], slab->keys[low++] = slab->keys[high];
        while (low < high && (slab->expires[low] < expire || (slab->expires[low] == expire && RedisModule_StringCompare(slab->keys[low], key) < 0)))
            low++;
        if (low < high)
            slab->expires[high] = slab->expires[low], slab->keys[high--] = slab->keys[low];
    }
    slab->expires[low] = expire, slab->keys[low] = key;
    return low;
}
void quick_sort(Slab *slab, int low, int high) {
    int pos;
    if (low < high) {
        pos = partition(slab, low, high);
        quick_sort(slab, low, pos - 1);
        quick_sort(slab, pos + 1, high);
    }

    return;
}
int quick_selectRelaxtopk(Slab *slab, int left, int right, int kth) {
    if (left > right)
        return -1;
    int mid = (right + left) / 2, i = left, j = right;
    long long pivot_expire = slab->expires[mid];
    RedisModuleString *pivot_key = slab->keys[mid];
    slab_swap(slab, left, mid);
    while (i != j) {
        while (j > i && (slab->expires[j] > pivot_expire || (slab->expires[j] == pivot_expire && RedisModule_StringCompare(slab->keys[j], pivot_key) >= 0)))
            --j;
        slab->expires[i] = slab->expires[j], slab->keys[i] = slab->keys[j];
        while (i < j && (slab->expires[i] < pivot_expire || (slab->expires[i] == pivot_expire && RedisModule_StringCompare(slab->keys[i], pivot_key) <= 0)))
            ++i;
        slab->expires[j] = slab->expires[i], slab->keys[j] = slab->keys[i];
    }
    slab->expires[j] = pivot_expire, slab->keys[j] = pivot_key;
    int left_partition_len = j - left;
    int temp = kth + left - 1;
    if (temp - RELAXATION <= j && j <= temp + RELAXATION) {
        return j;
    } else if (kth <= left_partition_len) {
        return quick_selectRelaxtopk(slab, left, j - 1, kth);
    } else {
        return quick_selectRelaxtopk(slab, j + 1, right, kth - left_partition_len - 1);
    }
}
void slab_ifNeedMerge(tairhash_zskiplist *zsl, tairhash_zskiplistNode *tair_hash_node) {
    if (tair_hash_node == NULL || tair_hash_node->slab == NULL || tair_hash_node->slab->num_keys == 0
        || tair_hash_node->slab->num_keys >= SLABMAXN) {
        return;
    }
    Slab *cur_slab = tair_hash_node->slab;
    int i, j;
    if (tair_hash_node->level[0].forward != NULL && tair_hash_node->level[0].forward->slab != NULL) {  // try merge next node
        tairhash_zskiplistNode *next_tair_hash_node = tair_hash_node->level[0].forward;
        Slab *next_slab = next_tair_hash_node->slab;
        int merge_sum = next_slab->num_keys + cur_slab->num_keys;
        if (merge_sum <= SLABMERGENUM && next_slab->num_keys > 0) {
            memcpy(&(cur_slab->expires[cur_slab->num_keys]), &(next_slab->expires[0]), next_slab->num_keys * sizeof(&(cur_slab->expires[0])));
            memcpy(&(cur_slab->keys[cur_slab->num_keys]), &(next_slab->keys[0]), next_slab->num_keys * sizeof(&(cur_slab->keys[0])));
            cur_slab->num_keys = merge_sum;
            RedisModule_Free(next_slab);
            int delete_ans = tairhash_zslDelete(zsl, next_tair_hash_node->key_min, next_tair_hash_node->expire_min);
            assert(delete_ans == 1);
        }
    }
    if (tair_hash_node->backward != NULL && tair_hash_node->backward->slab != NULL) {  // try merge pre node
        tairhash_zskiplistNode *pre_tair_hash_node = tair_hash_node->backward;
        Slab *pre_slab = pre_tair_hash_node->slab;
        int merge_sum = pre_slab->num_keys + cur_slab->num_keys;
        if (merge_sum <= SLABMERGENUM && pre_slab->num_keys > 0) {
            memcpy(&(cur_slab->expires[cur_slab->num_keys]), &(pre_slab->expires[0]), pre_slab->num_keys * sizeof(&(cur_slab->expires[0])));
            memcpy(&(cur_slab->keys[cur_slab->num_keys]), &(pre_slab->keys[0]), pre_slab->num_keys * sizeof(&(cur_slab->keys[0])));
            cur_slab->num_keys = merge_sum;
            tair_hash_node->expire_min = pre_tair_hash_node->expire_min, tair_hash_node->key_min = pre_tair_hash_node->key_min;
            RedisModule_Free(pre_slab);
            int delete_ans = tairhash_zslDelete(zsl, pre_tair_hash_node->key_min, pre_tair_hash_node->expire_min);
            assert(delete_ans == 1);
        }
    }
    return;
}
tairhash_zskiplistNode *slab_spilt(tairhash_zskiplist *zsl, tairhash_zskiplistNode *tair_hash_node) {
    if (tair_hash_node == NULL || tair_hash_node->slab == NULL || tair_hash_node->slab->num_keys != SLABMAXN)
        return NULL;

    Slab *new_slab = slab_createNode(), *slab = tair_hash_node->slab;
    /*
      // sort slab
      quick_sort(slab, 0, SLABMAXN - 1);

      int i, j;
      long long start_move_start = RedisModule_Milliseconds();
      for (i = 0, j = SLABMAXN / 2; j < SLABMAXN; j++, i++) {
          new_slab->expires[i] = slab->expires[j], new_slab->keys[i] = slab->keys[j];
          slab->expires[j] = 0, slab->keys[j] = NULL;
      }
      slab->num_keys = SLABMAXN / 2, new_slab->num_keys = SLABMAXN - SLABMAXN / 2;
     */

    int spilt_subscript = quick_selectRelaxtopk(slab, 0, SLABMAXN - 1, SLABMAXN / 4 * 3);
    memcpy(&(new_slab->expires[0]), &(slab->expires[spilt_subscript]), (SLABMAXN - spilt_subscript) * sizeof(&(slab->expires[0])));
    memcpy(&(new_slab->keys[0]), &(slab->keys[spilt_subscript]), (SLABMAXN - spilt_subscript) * sizeof(&(slab->keys[0])));
    slab->num_keys = spilt_subscript, new_slab->num_keys = SLABMAXN - spilt_subscript;

    long long new_expire_min = new_slab->expires[0];
    RedisModuleString *new_key_min = new_slab->keys[0];
    tairhash_zskiplistNode *new_tair_hash_node = tairhash_zslInsertNode(zsl, new_slab, new_key_min, new_expire_min);
    return new_tair_hash_node;
}

void slab_expireInsert(tairhash_zskiplist *zsl, RedisModuleString *key, long long expire) {
    tairhash_zskiplistNode *find_node = tairhash_zslGetNode(zsl, key, expire);
    int insert_ans = FALSE;
    if (find_node == zsl->header) {                 // no node insert
        find_node = zsl->header->level[0].forward;  // try to insert first skiplistNode
    }

    if (find_node != NULL && find_node->slab != NULL && find_node->slab->num_keys == SLABMAXN)  // slab full, running slab spilt
    {
        tairhash_zskiplistNode *new_tair_hash_node = slab_spilt(zsl, find_node);
        if (new_tair_hash_node->expire_min < expire || (new_tair_hash_node->expire_min == expire  // insert  new slab
                                                        && RedisModule_StringCompare(new_tair_hash_node->key_min, key) < 0)) {
            find_node = new_tair_hash_node;
            Slab *new_slab = find_node->slab;
            insert_ans = slab_insertNode(new_slab, key, expire);
            assert(insert_ans == TRUE);
        }
    }

    if (insert_ans == FALSE && find_node != NULL && find_node->slab != NULL && find_node->slab->num_keys < SLABMAXN) {  // slab not full, insert current slab
        Slab *find_slab = find_node->slab;
        insert_ans = slab_insertNode(find_slab, key, expire);
        assert(insert_ans == TRUE);
        if (find_node->expire_min > expire || (find_node->expire_min == expire && RedisModule_StringCompare(find_node->key_min, key) > 0)) {  // update tairhashskiplist
            find_node->expire_min = expire, find_node->key_min = key;
        }
    }

    if (insert_ans == FALSE) {  // no node insert, create node
        Slab *new_slab = slab_createNode();
        insert_ans = slab_insertNode(new_slab, key, expire);
        assert(insert_ans == TRUE);
        tairhash_zskiplistNode *new_node = tairhash_zslInsertNode(zsl, new_slab, key, expire);
    }
    return;
}
void slab_expireDelete(tairhash_zskiplist *zsl, RedisModuleString *key, long long expire) {
    tairhash_zskiplistNode *find_node = tairhash_zslGetNode(zsl, key, expire);
    assert(find_node != NULL);
    assert(find_node->slab != NULL);
    Slab *find_slab = find_node->slab;
    assert(find_slab->num_keys != 0);
    if (find_slab->num_keys == 1) {
        assert(find_slab->expires[0] == expire && RedisModule_StringCompare(find_slab->keys[0], key) == 0);
        Slab *new_slab = find_slab;
        int delete_ans = tairhash_zslDelete(zsl, key, expire);
        assert(delete_ans == 1);
        slab_delete(new_slab);
        return;
    }

    int update_findNode = FALSE, delete_ans = FALSE;
    if (find_node->expire_min == expire && RedisModule_StringCompare(find_node->key_min, key) == 0)
        update_findNode = TRUE;
    delete_ans = slab_deleteNode(find_slab, key, expire);
    assert(delete_ans == TRUE);

    if (update_findNode) {  // update min value
        int smallest_subscript = slab_minExpireTimeIndex(find_slab);
        find_node->expire_min = find_slab->expires[smallest_subscript], find_node->key_min = find_slab->keys[smallest_subscript];
    }
    slab_ifNeedMerge(zsl, find_node);  // if need merge
    return;
}
void slab_expireUpdate(tairhash_zskiplist *zsl, RedisModuleString *cur_key, long long cur_expire, RedisModuleString *new_key, long long new_expire) {
    slab_expireDelete(zsl, cur_key, cur_expire);
    slab_expireInsert(zsl, new_key, new_expire);
}

int slab_expireGet(tairhash_zskiplist *zsl, RedisModuleString *key, long long expire) {
    tairhash_zskiplistNode *find_node = tairhash_zslGetNode(zsl, key, expire);

    assert(find_node != NULL);
    assert(find_node->slab != NULL);
    int has_find = FALSE;

    if (find_node != NULL) {
        has_find = slab_getNode(find_node->slab, key, expire) > -1 ? TRUE : FALSE;
    }
    return has_find;
}
void slab_free(tairhash_zskiplist *zsl) {
    tairhash_zslFree(zsl);
}
tairhash_zskiplist *slab_create() {
    return tairhash_zslCreate();
}
void slab_deleteSlabEXpire(tairhash_zskiplist *zsl, tairhash_zskiplistNode *zsl_node, int *effictive_indexs, int effictive_num) {
    if (zsl_node == NULL)
        return;
    Slab *slab = zsl_node->slab;
    if (slab == NULL || slab->num_keys == effictive_num) {
        return;
    }
    if (effictive_num == 0) {
        Slab *new_slab = slab;
        int delete_ans = tairhash_zslDelete(zsl, zsl_node->key_min, zsl_node->expire_min);
        assert(delete_ans == 1);
        slab_delete(slab);
        return;
    }
    int index = 0, min_index = 0, i;
    for (i = 0; i < effictive_num; i++) {
        index = effictive_indexs[i];
        slab->expires[i] = slab->expires[index];
        slab->keys[i] = slab->keys[index];
        if (slab->expires[i] < slab->expires[min_index] || (slab->expires[i] == slab->expires[min_index] && RedisModule_StringCompare(slab->keys[i], slab->keys[min_index]) <= 0)) {
            min_index = i;
        }
    }
    slab->num_keys = effictive_num;
    zsl_node->expire_min = slab->expires[min_index], zsl_node->key_min = slab->keys[min_index];
    slab_ifNeedMerge(zsl, zsl_node);
    return;
}
unsigned int slab_deleteTairhashRangeByRank(tairhash_zskiplist *zsl, unsigned int start, unsigned int end) {
    return tairhash_zslDeleteRangeByRank(zsl, start, end);
}

#ifdef __AVX2__
int slab_getSlabTimeoutExpireIndex(tairhash_zskiplistNode *node, int *ontime_indices, int *timeout_indices) {
    long long now = RedisModule_Milliseconds();
    if (node == NULL || node->expire_min > now) return 0;
    Slab *slab = node->slab;
    if (slab == NULL || slab->num_keys == 0)
        return 0;

    __m256i target_expire_vec = _mm256_set1_epi64x(now);
    int ontime_num = 0, timeout_num = 0, size_effictive = 0, size = slab->num_keys;
    long long *expires = slab->expires, i;
    const static int width = sizeof(__m256i) / sizeof(long long);
    const int veclen = size & ~(2 * width - 1);
    int step_size = (width << 1);
    for (i = 0; i < veclen; i += step_size) {
        const __m256i v_a = _mm256_lddqu_si256((const __m256i *)(expires + i));
        const __m256i v_b = _mm256_lddqu_si256((const __m256i *)(expires + i + width));

        _mm_prefetch(expires + i + step_size, _MM_HINT_NTA);

        __m256i v_a_gt = _mm256_cmpgt_epi64(v_a, target_expire_vec);
        __m256i v_b_gt = _mm256_cmpgt_epi64(v_b, target_expire_vec);
        int v_a_gt_mask = _mm256_movemask_pd((__m256d)v_a_gt);
        int v_b_gt_mask = _mm256_movemask_pd((__m256d)v_b_gt);
        __m256i v_a_cur_i = _mm256_set1_epi64x(i);
        __m256i v_b_cur_i = _mm256_set1_epi64x(i + width);

        __m256i v_a_offsets = _mm256_add_epi64(v_a_cur_i, shuffle_ontime_mask_4x64[v_a_gt_mask]);
        __m256i v_b_offsets = _mm256_add_epi64(v_b_cur_i, shuffle_ontime_mask_4x64[v_b_gt_mask]);
        _mm256_storeu_si256((__m256i *)(ontime_indices + ontime_num), v_a_offsets);
        ontime_num += _mm_popcnt_u64((unsigned)v_a_gt_mask);
        _mm256_storeu_si256((__m256i *)(ontime_indices + ontime_num), v_b_offsets);
        ontime_num += _mm_popcnt_u64((unsigned)v_b_gt_mask);

        v_a_offsets = _mm256_add_epi64(v_a_cur_i, shuffle_timeout_mask_4x64[v_a_gt_mask]);
        v_b_offsets = _mm256_add_epi64(v_b_cur_i, shuffle_timeout_mask_4x64[v_b_gt_mask]);
        _mm256_storeu_si256((__m256i *)(timeout_indices + timeout_num), v_a_offsets);
        timeout_num += width - _mm_popcnt_u64((unsigned)v_a_gt_mask);
        _mm256_storeu_si256((__m256i *)(timeout_indices + timeout_num), v_b_offsets);
        timeout_num += width - _mm_popcnt_u64((unsigned)v_b_gt_mask);
    }
    for (; i < size; ++i) {
        ontime_indices[ontime_num] = i, timeout_indices[timeout_num] = i;
        ontime_num += (expires[i] > now), timeout_num += (expires[i] <= now);
    }
    return timeout_num;
}
void slab_initShuffleMask() {
    __m256i shuffle_timeout_mask_array[16] = {
        (__m256i)(__v4di){0, 1, 2, 3},
        (__m256i)(__v4di){1, 2, 3, -1},
        (__m256i)(__v4di){0, 2, 3, -1},
        (__m256i)(__v4di){2, 3, -1, -1},
        (__m256i)(__v4di){0, 1, 3, -1},
        (__m256i)(__v4di){1, 3, -1, -1},
        (__m256i)(__v4di){0, 3, -1, -1},
        (__m256i)(__v4di){3, -1, -1, -1},
        (__m256i)(__v4di){0, 1, 2, -1},
        (__m256i)(__v4di){1, 2, -1, -1},
        (__m256i)(__v4di){0, 2, -1, -1},
        (__m256i)(__v4di){2, -1, -1, -1},
        (__m256i)(__v4di){0, 1, -1, -1},
        (__m256i)(__v4di){1, -1, -1, -1},
        (__m256i)(__v4di){0, -1, -1, -1},
        (__m256i)(__v4di){-1, -1, -1, -1},
    };
    __m256i shuffle_ontime_mask_array[16] = {
        (__m256i)(__v4di){-1, -1, -1, -1},
        (__m256i)(__v4di){0, -1, -1, -1},
        (__m256i)(__v4di){1, -1, -1, -1},
        (__m256i)(__v4di){0, 1, -1, -1},
        (__m256i)(__v4di){2, -1, -1, -1},
        (__m256i)(__v4di){0, 2, -1, -1},
        (__m256i)(__v4di){1, 2, -1, -1},
        (__m256i)(__v4di){0, 1, 2, -1},
        (__m256i)(__v4di){3, -1, -1, -1},
        (__m256i)(__v4di){0, 3, -1, -1},
        (__m256i)(__v4di){1, 3, -1, -1},
        (__m256i)(__v4di){0, 1, 3, -1},
        (__m256i)(__v4di){2, 3, -1, -1},
        (__m256i)(__v4di){0, 2, 3, -1},
        (__m256i)(__v4di){1, 2, 3, -1},
        (__m256i)(__v4di){0, 1, 2, 3},
    };

    int i = 0;
    for (; i < 16; i++) {
        shuffle_timeout_mask_4x64[i] = shuffle_timeout_mask_array[i];
        shuffle_ontime_mask_4x64[i] = shuffle_ontime_mask_array[i];
    }
}

#else
int slab_getSlabTimeoutExpireIndex(tairhash_zskiplistNode *node, int *ontime_indices, int *timeout_indices) {
    long long now = RedisModule_Milliseconds();
    if (node == NULL || node->expire_min > now) return 0;
    Slab *slab = node->slab;
    if (slab == NULL || slab->num_keys == 0)
        return 0;
    int ontime_num = 0, timeout_num = 0, size_effictive = 0, size = slab->num_keys, i;
    long long *expires = slab->expires;
    for (i = 0; i < size; ++i) {
        ontime_indices[ontime_num] = i, timeout_indices[timeout_num] = i;
        ontime_num += (expires[i] > now), timeout_num += (expires[i] <= now);
    }
    return timeout_num;
}
#endif