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

#if defined(SLAB_MODE)
void insert(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire);
void update(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long cur_expire, long long new_expire);
void delete(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire);
void deleteAndPropagate(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire, int is_timer);
void activeExpire(RedisModuleCtx *ctx, int dbid, uint64_t keys);
void passiveExpire(RedisModuleCtx *ctx, int dbid, RedisModuleString *key_per_loop);
#endif