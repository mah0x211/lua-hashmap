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

#include "hashmap.h"
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

char *hm_get_record_key(hashmap_record_t *r)
{
    return (char *)(r + 1);
}

char *hm_get_record_value(hashmap_record_t *r)
{
    return hm_get_record_key(r) + r->key_size + 1;
}

size_t hm_get_record_size(hashmap_record_t *r)
{
    return sizeof(hashmap_record_t) + r->key_size + r->value_size + 2;
}

char *hm_strerror(hm_error_t e)
{
    switch (e) {
    case HM_OK:
        return "success";

    case HM_MAP_FAILED:
    case HM_LOCK_FAILED:
        return strerror(errno);

    case HM_MEMORY_SIZE_TOO_SMALL:
        return "memory size too small";

    case HM_NO_SPACE:
        return "not enough space in data space";

    case HM_NO_EMPTY_BUCKET:
        return "buckets is full";

    case HM_NO_EMPTY_FREE_BLOCK:
        return "freelist is full";

    case HM_NOT_FOUND:
        return "not found";
    }
}

// clang-format off
#define ALIGNMENT_OF(t) (offsetof(struct{char a; t b;}, b))
// clang-format on

static inline size_t get_aligned_size(size_t size)
{
    const size_t align_size = ALIGNMENT_OF(hashmap_region_t);
    return (size + align_size - 1) & ~(align_size - 1);
}

#undef ALIGNMENT_OF

hm_error_t hm_init(hashmap_t *m, size_t memory_size, size_t max_buckets,
                   size_t max_free_blocks)
{
    hashmap_stat_t s = {0};

    memory_size = get_aligned_size(memory_size);
    if (hm_calc_required_memory_size(&s, memory_size, max_buckets,
                                     max_free_blocks, 0) != HM_OK ||
        memory_size < s.memory_size) {
        // Memory size is too small
        return HM_MEMORY_SIZE_TOO_SMALL;
    }

    pthread_rwlock_t *lock = &m->lock;
    pthread_rwlockattr_t attr;
    if ((errno = pthread_rwlockattr_init(&attr)) ||
        (errno =
             pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED)) ||
        (errno = pthread_rwlock_init(lock, &attr))) {
        return HM_LOCK_FAILED;
    }
    pthread_rwlockattr_destroy(&attr);

    void *memory = mmap(NULL, memory_size, PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (memory == MAP_FAILED) {
        pthread_rwlock_destroy(lock);
        return HM_MAP_FAILED;
    }

    hashmap_header_t *hdr = (hashmap_header_t *)memory;
    hdr->memory_size      = memory_size;
    hdr->max_bucket_flags = s.max_bucket_flags;
    hdr->max_buckets      = s.max_buckets;
    hdr->max_free_blocks  = s.max_free_blocks;
    hdr->num_free_blocks  = 0;

    hdr->bucket_flags_offset = sizeof(hashmap_header_t);
    hdr->buckets_offset      = hdr->bucket_flags_offset + s.bucket_flags_size;
    hdr->freelist_offset     = hdr->buckets_offset + s.buckets_size;
    hdr->data_offset         = hdr->freelist_offset + s.free_blocks_size;
    hdr->data_tail           = hdr->data_offset;

    m->region = (uintptr_t)memory;
    return HM_OK;
}

static inline hashmap_region_t lock_region_for_read(hashmap_t *m)
{
    if ((errno = pthread_rwlock_rdlock(&m->lock))) {
        return 0;
    }
    return m->region;
}

static inline hashmap_region_t lock_region_for_write(hashmap_t *m)
{
    if ((errno = pthread_rwlock_wrlock(&m->lock))) {
        return 0;
    }
    return m->region;
}

static inline void unlock_region(hashmap_t *m)
{
    pthread_rwlock_unlock(&m->lock);
}

hm_error_t hm_destroy(hashmap_t *m)
{
    hashmap_header_t *hdr = (hashmap_header_t *)lock_region_for_write(m);
    if (!hdr) {
        return HM_LOCK_FAILED;
    }
    // Free the memory allocated by mmap.
    munmap(hdr, hdr->memory_size);
    unlock_region(m);
    pthread_rwlock_destroy(&m->lock);
    return HM_OK;
}

static inline size_t *seek_free_block_size(hashmap_region_t reg, size_t offset)
{
    return (size_t *)(reg + offset);
}

static inline hashmap_record_t *seek_record(hashmap_region_t reg, size_t offset)
{
    return (hashmap_record_t *)(reg + offset);
}

static inline size_t *seek_freelist(hashmap_region_t reg)
{
    hashmap_header_t *hdr = (hashmap_header_t *)reg;
    return (size_t *)(reg + hdr->freelist_offset);
}

static inline uint64_t *seek_bucket_flags(hashmap_region_t reg)
{
    hashmap_header_t *hdr = (hashmap_header_t *)reg;
    return (uint64_t *)(reg + hdr->bucket_flags_offset);
}

static inline size_t *seek_buckets(hashmap_region_t reg)
{
    hashmap_header_t *hdr = (hashmap_header_t *)reg;
    return (size_t *)(reg + hdr->buckets_offset);
}

static inline int has_empty_free_block(hashmap_region_t reg)
{
    hashmap_header_t *hdr = (hashmap_header_t *)reg;
    // 0 = The freelist is full
    return hdr->num_free_blocks < hdr->max_free_blocks;
}

static hm_error_t add_free_block(hashmap_region_t reg, size_t offset,
                                 size_t size)
{
    hashmap_header_t *hdr = (hashmap_header_t *)reg;

    // The freelist must not be full
    assert(has_empty_free_block(reg));

    // size must be greater than 8 bytes
    assert(size >= sizeof(size_t));
    size += sizeof(size_t);

    size_t *freelist = seek_freelist(reg);
    ssize_t left     = 0;
    if (hdr->num_free_blocks > 0) {
        ssize_t right = hdr->num_free_blocks - 1;

        while (left <= right) {
            ssize_t mid       = (left + right) / 2;
            uint64_t offset   = freelist[mid];
            size_t block_size = *seek_free_block_size(reg, offset);

            if (block_size < size) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        if (offset + size == freelist[left]) {
            // merge with existing element
            size += *seek_free_block_size(reg, freelist[left]);
            freelist[left]                     = offset;
            *seek_free_block_size(reg, offset) = size;

            // sort
            ssize_t n = hdr->num_free_blocks - 1;
            for (ssize_t i = left; i < n; i++) {
                size_t next_size = *seek_free_block_size(reg, freelist[i + 1]);

                if (next_size < size) {
                    // move next_offset behind offset
                    freelist[i]     = freelist[i + 1];
                    freelist[i + 1] = offset;
                    continue;
                }
                break;
            }
            return HM_OK;
        }

        for (ssize_t i = hdr->num_free_blocks - 1; i >= left; i--) {
            freelist[i + 1] = freelist[i];
        }
    }

    // set free block offset and size
    freelist[left]                     = offset;
    *seek_free_block_size(reg, offset) = size;
    hdr->num_free_blocks++;
    return HM_OK;
}

static inline void remove_free_block(hashmap_region_t reg, int idx)
{
    hashmap_header_t *hdr = (hashmap_header_t *)reg;
    size_t *freelist      = seek_freelist(reg);
    int n                 = hdr->num_free_blocks - 1;

    // Remove the free block from the freelist
    for (int i = idx; i < n; i++) {
        freelist[i] = freelist[i + 1];
    }

    hdr->num_free_blocks--;
}

static size_t find_free_block(hashmap_region_t reg, size_t required_space)
{
    hashmap_header_t *hdr = (hashmap_header_t *)reg;
    if (hdr->num_free_blocks == 0) {
        return SIZE_MAX;
    }

    size_t *freelist = seek_freelist(reg);
    ssize_t left     = 0;
    ssize_t right    = hdr->num_free_blocks - 1;

    while (left <= right) {
        ssize_t mid       = (left + right) / 2;
        size_t offset     = freelist[mid];
        size_t block_size = *seek_free_block_size(reg, offset);

        if (block_size == required_space) {
            offset = freelist[mid];
            remove_free_block(reg, mid);
            return offset;
        } else if (block_size > required_space) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    if (left < hdr->num_free_blocks) {
        size_t offset         = freelist[left];
        size_t block_size     = *seek_free_block_size(reg, offset);
        size_t remaining_size = block_size - required_space;

        if (remaining_size == 0) {
            remove_free_block(reg, left);
            return offset;
        } else if (remaining_size < sizeof(size_t) ||
                   !has_empty_free_block(reg)) {
            // The remaining unused size cannot be managed because there is not
            // enough space to hold the size or free block information.
            // Therefore, this block cannot be used.
            return SIZE_MAX;
        }

        remove_free_block(reg, left);
        // Add a new free block for the remaining unused space
        add_free_block(reg, offset + required_space, remaining_size);
        return offset;
    }

    // No suitable free block found.
    return SIZE_MAX;
}

static uint64_t hash_string(const char *key)
{
    uint64_t hash = 5381;
    int c;

    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + c;
    }

    return hash;
}

static void set_used_bits(hashmap_region_t reg, int bucket_index)
{
    uint64_t *bucket_flags = seek_bucket_flags(reg);
    bucket_flags[bucket_index / 64] |= 1 << (bucket_index % 64);
}

static void unset_used_bits(hashmap_region_t reg, int bucket_index)
{
    uint64_t *bucket_flags = seek_bucket_flags(reg);
    bucket_flags[bucket_index / 64] &= ~(1 << (bucket_index % 64));
}

static inline int is_used_bucket(hashmap_region_t reg, int bucket_index)
{
    uint64_t *bucket_flags = seek_bucket_flags(reg);
    return (bucket_flags[bucket_index / 64] >> (bucket_index % 64)) & 1;
}

static hashmap_record_t *find_record(hashmap_region_t reg, const char *key,
                                     size_t key_len, int *found_index)
{
    hashmap_header_t *hdr = (hashmap_header_t *)reg;
    size_t *buckets       = seek_buckets(reg);
    uint64_t hash         = hash_string(key);
    int index             = hash % hdr->max_buckets;

    *found_index = hdr->max_buckets;
    for (int i = 0; i < hdr->max_buckets; ++i) {
        int bucket_index = (index + i) % hdr->max_buckets;
        size_t offset    = buckets[bucket_index];

        if (offset == 0) {
            if (*found_index == hdr->max_buckets) {
                *found_index = bucket_index;
            }
            return NULL;
        } else if (is_used_bucket(reg, bucket_index)) {
            hashmap_record_t *r = seek_record(reg, offset);
            if (r->hash == hash && r->key_size == key_len &&
                memcmp(hm_get_record_key(r), key, key_len) == 0) {
                *found_index = bucket_index;
                return r;
            }
        }
    }

    return NULL;
}

static inline size_t get_record_offset(hashmap_record_t *r,
                                       hashmap_region_t reg)
{
    return (uintptr_t)r - reg;
}

hm_error_t hm_insert(hashmap_t *m, const char *key, size_t key_len,
                     const char *value, size_t value_len)
{
    hashmap_region_t reg = lock_region_for_write(m);
    if (!reg) {
        return HM_LOCK_FAILED;
    }

    int bucket_index;
    hashmap_header_t *hdr = (hashmap_header_t *)reg;
    hashmap_record_t *r   = find_record(reg, key, key_len, &bucket_index);

    if (!r && bucket_index == hdr->max_buckets) {
        // No empty buckets available.
        unlock_region(m);
        return HM_NO_EMPTY_BUCKET;
    }

    // Find available space in the data area for the new key-value pair.
    const size_t required_space =
        sizeof(hashmap_record_t) + key_len + value_len + 2;
    size_t insert_offset = hdr->data_tail;

    if (r) {
        if (r->value_size == value_len) {
            // Update the value in-place.
            memcpy(hm_get_record_value(r), value, value_len);
            unlock_region(m);
            return HM_OK;
        } else if (!has_empty_free_block(reg)) {
            // The freelist is full
            unlock_region(m);
            return HM_NO_EMPTY_FREE_BLOCK;
        }

        // Add the old key-value pair space to the free block list.
        add_free_block(reg, get_record_offset(r, reg), hm_get_record_size(r));
    }

    // Check if there is enough space at the end of the data area.
    const size_t available_space = hdr->memory_size - hdr->data_tail;

    if (available_space < required_space) {
        // Try to find a suitable free block for the new record.
        size_t free_block_offset = find_free_block(reg, required_space);
        if (free_block_offset == SIZE_MAX) {
            // Not enough space in data space to write new records.
            unlock_region(m);
            return HM_NO_SPACE;
        }
        // Use the found free block.
        insert_offset = free_block_offset;
    }

    size_t *buckets       = seek_buckets(reg);
    buckets[bucket_index] = insert_offset;
    set_used_bits(reg, bucket_index);

    hashmap_record_t *newr = seek_record(reg, insert_offset);
    newr->hash             = hash_string(key);
    newr->key_size         = key_len;
    newr->value_size       = value_len;

    // Copy the key-value pair to the data area.
    char *ptr = hm_get_record_key(newr);
    memcpy(ptr, key, key_len);
    ptr[key_len] = '\0';
    ptr          = hm_get_record_value(newr);
    memcpy(ptr, value, value_len);
    ptr[value_len] = '\0';

    // Update the data_tail for the next insert operation only if the free block
    // was not used.
    if (available_space >= required_space) {
        hdr->data_tail += required_space;
    }

    unlock_region(m);
    return HM_OK;
}

hm_error_t hm_delete(hashmap_t *m, const char *key, size_t key_len)
{
    hashmap_region_t reg = lock_region_for_write(m);
    if (!reg) {
        return HM_LOCK_FAILED;
    }

    int bucket_index;
    hashmap_record_t *r = find_record(reg, key, key_len, &bucket_index);

    if (!r) {
        // Key not found.
        unlock_region(m);
        return HM_NOT_FOUND;
    } else if (!has_empty_free_block(reg)) {
        // The freelist is full
        unlock_region(m);
        return HM_NO_EMPTY_FREE_BLOCK;
    }

    // Add the offset of the record to the freelist.
    add_free_block(reg, get_record_offset(r, reg), hm_get_record_size(r));
    unset_used_bits(reg, bucket_index);
    unlock_region(m);

    return HM_OK;
}

hm_error_t hm_search(hashmap_t *m, const char *key, size_t key_len,
                     char **value, size_t *value_len)
{
    hashmap_region_t reg = lock_region_for_read(m);
    if (!reg) {
        return HM_LOCK_FAILED;
    }

    int bucket_index;
    hashmap_record_t *r = find_record(reg, key, key_len, &bucket_index);

    if (!r) {
        // Key not found.
        unlock_region(m);
        return HM_NOT_FOUND;
    }

    *value     = hm_get_record_value(r);
    *value_len = r->value_size;

    unlock_region(m);
    return HM_OK;
}

hm_error_t hm_calc_required_memory_size(hashmap_stat_t *s, size_t memory_size,
                                        size_t max_buckets,
                                        size_t max_free_blocks,
                                        size_t record_kv_size)
{
    if (max_buckets == 0) {
        if (memory_size == 0) {
            return HM_MEMORY_SIZE_TOO_SMALL;
        }
        max_buckets = (memory_size / 4) / sizeof(uint64_t);
    }
    if (max_free_blocks == 0) {
        max_free_blocks = max_buckets;
    }

    s->max_bucket_flags = (max_buckets + 63) / 64;
    s->max_buckets      = max_buckets;
    s->max_free_blocks  = max_free_blocks;

    s->bucket_flags_size = s->max_bucket_flags * sizeof(uint64_t);
    s->buckets_size      = max_buckets * sizeof(size_t);
    s->free_blocks_size  = max_free_blocks * sizeof(size_t);
    s->header_size       = sizeof(hashmap_header_t);
    s->memory_size = s->header_size + s->bucket_flags_size + s->buckets_size +
                     s->free_blocks_size;

    s->record_header_size = sizeof(hashmap_record_t) + 2;
    if (record_kv_size) {
        s->record_size = s->record_header_size + record_kv_size;
        s->data_size   = s->record_size * s->max_buckets;
        s->memory_size += s->data_size;
    }

    if (memory_size) {
        s->record_size = 0;
        s->data_size   = 0;
        if (memory_size > s->memory_size) {
            s->data_size   = memory_size - s->memory_size;
            s->record_size = s->data_size / s->record_header_size;
        }
    }
    s->memory_size = get_aligned_size(s->memory_size);

    return HM_OK;
}

static inline uint64_t count_bits(uint64_t x)
{
    // Count set bits in each pair of bits
    x = x - ((x >> 1) & 0x5555555555555555ULL);
    // Count set bits in each 4-bit group
    x = (x & 0x3333333333333333ULL) + ((x >> 2) & 0x3333333333333333ULL);
    // Count set bits in each nibble (8-bit group)
    x = (x + (x >> 4)) & 0x0F0F0F0F0F0F0F0FULL;
    // Calculate the sum of set bits in each byte and reduce it to the most
    // significant byte
    x = (x * 0x0101010101010101ULL) >> 56;
    return x;
}

static uint64_t count_bucket_flags(hashmap_region_t reg)
{
    hashmap_header_t *hdr  = (hashmap_header_t *)reg;
    uint64_t *bucket_flags = seek_bucket_flags(reg);
    uint64_t count         = 0;

    for (int i = 0; i < hdr->max_bucket_flags; i++) {
        count += count_bits(bucket_flags[i]);
    }
    return count;
}

hm_error_t hm_stat(hashmap_stat_t *s, hashmap_t *m)
{
    hashmap_region_t reg = lock_region_for_read(m);
    if (!reg) {
        return HM_LOCK_FAILED;
    }

    hashmap_header_t *hdr = (hashmap_header_t *)reg;
    s->memory_size        = hdr->memory_size;
    s->max_bucket_flags   = hdr->max_bucket_flags;
    s->max_buckets        = hdr->max_buckets;
    s->max_free_blocks    = hdr->max_free_blocks;
    // size of each allocated memory
    s->bucket_flags_size  = s->max_bucket_flags * sizeof(uint64_t);
    s->buckets_size       = s->max_buckets * sizeof(size_t);
    s->free_blocks_size   = s->max_free_blocks * sizeof(size_t);
    s->header_size        = sizeof(hashmap_header_t);
    s->data_size          = hdr->memory_size - hdr->data_offset;
    s->record_header_size = sizeof(hashmap_record_t) + 2;
    s->record_size        = 0;
    // usage
    s->used_buckets       = count_bucket_flags(reg);
    s->used_free_blocks   = hdr->num_free_blocks;
    s->used_data_size     = hdr->data_tail - hdr->data_offset;

    unlock_region(m);
    return HM_OK;
}
