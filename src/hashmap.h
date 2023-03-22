/**
 *  Copyright (C) 2023 Masatoshi Fukunaga
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to
 *  deal in the Software without restriction, including without limitation the
 *  rights to use, copy, modify, merge, publish, distribute, sublicense,
 *  and/or sell copies of the Software, and to permit persons to whom the
 *  Software is furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *  DEALINGS IN THE SOFTWARE.
 */

#ifndef hashmap_h
#define hashmap_h

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    uint64_t hash;
    size_t key_size;
    size_t value_size;
} hashmap_record_t;

char *hm_get_record_key(hashmap_record_t *r);
char *hm_get_record_value(hashmap_record_t *r);
size_t hm_get_record_size(hashmap_record_t *r);

typedef enum {
    HM_OK = 0,
    HM_MAP_FAILED,
    HM_LOCK_FAILED,
    HM_MEMORY_SIZE_TOO_SMALL,
    HM_NO_SPACE,
    HM_NO_EMPTY_BUCKET,
    HM_NO_EMPTY_FREE_BLOCK,
    HM_NOT_FOUND
} hm_error_t;

char *hm_strerror(hm_error_t e);

typedef struct {
    size_t memory_size;
    int32_t max_bucket_flags;
    int32_t max_buckets;
    int32_t max_free_blocks;
    int32_t num_free_blocks;
    // offsets
    size_t bucket_flags_offset;
    size_t buckets_offset;
    size_t freelist_offset;
    size_t data_offset;
    size_t data_tail;
} hashmap_header_t;

typedef uintptr_t hashmap_region_t;

// typedef struct {
//     hashmap_header_t header;
//     uint64_t *bucket_flags;
//     size_t *buckets;
//     size_t *freelist;
//     char *data;
// } hashmap_region_t;

typedef struct {
    hashmap_region_t region;
    pthread_rwlock_t lock;
} hashmap_t;

hm_error_t hm_init(hashmap_t *m, size_t memory_size, size_t max_buckets,
                   size_t max_free_blocks);

hm_error_t hm_destroy(hashmap_t *m);

hm_error_t hm_insert(hashmap_t *m, const char *key, size_t key_len,
                     const char *value, size_t value_len);

hm_error_t hm_delete(hashmap_t *m, const char *key, size_t key_len);

hm_error_t hm_search(hashmap_t *m, const char *key, size_t key_len,
                     char **value, size_t *value_len);

typedef struct {
    size_t memory_size;
    int32_t max_bucket_flags;
    int32_t max_buckets;
    int32_t max_free_blocks;
    // size of each allocated memory
    size_t bucket_flags_size;
    size_t buckets_size;
    size_t free_blocks_size;
    size_t header_size;
    size_t data_size;
    size_t record_header_size;
    size_t record_size;
    // usages
    size_t used_buckets;
    size_t used_free_blocks;
    size_t used_data_size;
} hashmap_stat_t;

hm_error_t hm_calc_required_memory_size(hashmap_stat_t *s, size_t memory_size,
                                        size_t max_buckets,
                                        size_t max_free_blocks,
                                        size_t record_kv_size);

hm_error_t hm_stat(hashmap_stat_t *s, hashmap_t *m);

#endif
