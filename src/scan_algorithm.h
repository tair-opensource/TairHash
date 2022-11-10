#pragma once

#if (!defined SORT_MODE) && (!defined SLAB_MODE)

void insert(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire);
void update(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long cur_expire, long long new_expire);
void delete(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire);
void deleteAndPropagate(RedisModuleCtx *ctx, int dbid, RedisModuleString *key, tairHashObj *obj, RedisModuleString *field, long long expire, int is_timer);
void activeExpire(RedisModuleCtx *ctx, int dbid, uint64_t keys);
void passiveExpire(RedisModuleCtx *ctx, int dbid, RedisModuleString *key_per_loop);

#endif