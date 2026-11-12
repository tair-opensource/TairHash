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

#include <stddef.h>

#include "redismodule.h"

/* The allocator's size of ptr. Servers older than Redis 6.0 have no
 * RedisModule_MallocSize; they get `requested`, the size that was asked for. */
static inline size_t allocSize(const void *ptr, size_t requested) {
    return RedisModule_MallocSize ? RedisModule_MallocSize((void *)ptr) : requested;
}
