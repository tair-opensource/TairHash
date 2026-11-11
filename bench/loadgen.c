/*
 * loadgen: a pipelined load generator for TairHash. The data shape is many keys, a few
 * dozen short fields each, short values and microsecond-timestamp versions.
 *
 *   loadgen -w <workload> [-h host] [-p port] [-k keys] [-f fields] [-m fields_per_write]
 *           [-n ops] [-P pipeline] [-v value_len] [-s stale_pct] [-t ttl] [-S seed]
 *
 * Population workloads write every field of every key once, in key order:
 *   loadset     EXHSET ... ABS <version> per field (with EX <ttl> when -t is given)
 *   loadmset    EXHMSET with every field of a key
 *   loadhset    native HSET with an 8-byte version prefix on each value: the memory
 *               baseline for the same data in a plain hash
 *
 * Steady-state workloads pick a random key (and fields) per op:
 *   gt          EXHSET ... GT <version> of one field (with EX <ttl> when -t is given);
 *               `stale_pct`% of ops carry version 1, which is older than any field, and
 *               get an error reply
 *   mset        EXHMSET of `fields_per_write` distinct random fields
 *   getallver   EXHGETALLWITHVER of a whole key
 *
 * Prints throughput, the number of error replies and of full reads that found no key. Error
 * replies are counted; they fail a population run (load*) only.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "hiredis.h"

#define MAX_FIELDS 256
#define MAX_VALUE_LEN 63
#define MAX_ARGS (3 + 3 * MAX_FIELDS + 2)

typedef struct {
    const char *host;
    int port;
    const char *workload;
    long keys;
    long fields;
    long fields_per_write;
    long ops;
    long pipeline;
    long value_len;
    long stale_pct;
    long ttl; /* seconds; 0 writes without a TTL */
    unsigned seed;
} Config;

/* Backing storage for one command's arguments. */
typedef struct {
    char key[64];
    char field[MAX_FIELDS][24];
    char value[MAX_FIELDS][8 + MAX_VALUE_LEN + 1];
    char version[MAX_FIELDS][24];
    char ttl[24];
    const char *argv[MAX_ARGS];
    size_t argvlen[MAX_ARGS];
    int argc;
} Command;

static long long now_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000000 + tv.tv_usec;
}

static void usage(void) {
    fprintf(stderr,
            "usage: loadgen -w loadset|loadmset|loadhset|gt|mset|getallver [-h host] [-p port] [-k keys] [-f fields]\n"
            "               [-m fields_per_write] [-n ops] [-P pipeline] [-v value_len] [-s stale_pct] [-t ttl] [-S seed]\n");
    exit(2);
}

static void arg(Command *cmd, const char *s, size_t len) {
    cmd->argv[cmd->argc] = s;
    cmd->argvlen[cmd->argc] = len;
    cmd->argc++;
}

static void arg_str(Command *cmd, const char *s) {
    arg(cmd, s, strlen(s));
}

static void set_value(char *buf, long len, long seed) {
    for (long i = 0; i < len; i++) {
        buf[i] = (char)('0' + (seed + i) % 10);
    }
}

/* EX <ttl> for a single-field write, when the run sets TTLs. */
static void arg_ttl(Command *cmd, const Config *cfg) {
    if (cfg->ttl > 0) {
        snprintf(cmd->ttl, sizeof(cmd->ttl), "%ld", cfg->ttl);
        arg_str(cmd, "EX");
        arg_str(cmd, cmd->ttl);
    }
}

static void set_key(Command *cmd, long k) {
    snprintf(cmd->key, sizeof(cmd->key), "bench:%ld:%ld", k % 97, k);
}

/* Field f of the key, with its value and version, into slot i. */
static void set_field(Command *cmd, int i, long f, long value_seed, long len, long long version) {
    snprintf(cmd->field[i], sizeof(cmd->field[i]), "field_%02ld", f);
    set_value(cmd->value[i], len, value_seed);
    snprintf(cmd->version[i], sizeof(cmd->version[i]), "%lld", version);
}

/* Every field of key k as one EXHMSET, or as one HSET with the version packed in front
 * of each value. */
static void build_full_key(Command *cmd, const Config *cfg, long k, long long version, int packed) {
    cmd->argc = 0;
    set_key(cmd, k);
    arg_str(cmd, packed ? "HSET" : "EXHMSET");
    arg_str(cmd, cmd->key);
    for (long f = 0; f < cfg->fields; f++) {
        set_field(cmd, (int)f, f, k + f, cfg->value_len, version);
        arg_str(cmd, cmd->field[f]);
        if (packed) {
            memmove(cmd->value[f] + 8, cmd->value[f], cfg->value_len);
            memcpy(cmd->value[f], &version, 8);
            arg(cmd, cmd->value[f], 8 + cfg->value_len);
        } else {
            arg(cmd, cmd->value[f], cfg->value_len);
        }
    }
}

/* `n` distinct fields of key k, starting at a random field, as one EXHMSET. */
static void build_mset(Command *cmd, const Config *cfg, long k, long n, long op) {
    cmd->argc = 0;
    set_key(cmd, k);
    arg_str(cmd, "EXHMSET");
    arg_str(cmd, cmd->key);
    long first = random() % cfg->fields;
    for (long i = 0; i < n; i++) {
        set_field(cmd, (int)i, (first + i) % cfg->fields, op + i, cfg->value_len, 0);
        arg_str(cmd, cmd->field[i]);
        arg(cmd, cmd->value[i], cfg->value_len);
    }
}

/* Appends the command for operation `op`. */
static void append_op(redisContext *c, const Config *cfg, Command *cmd, long op, long long base_version) {
    const char *w = cfg->workload;
    long long version = base_version + op;

    if (!strcmp(w, "loadmset") || !strcmp(w, "loadhset")) {
        build_full_key(cmd, cfg, op, version, !strcmp(w, "loadhset"));
    } else if (!strcmp(w, "loadset")) {
        long k = op / cfg->fields, f = op % cfg->fields;
        cmd->argc = 0;
        set_key(cmd, k);
        set_field(cmd, 0, f, k + f, cfg->value_len, version);
        arg_str(cmd, "EXHSET");
        arg_str(cmd, cmd->key);
        arg_str(cmd, cmd->field[0]);
        arg(cmd, cmd->value[0], cfg->value_len);
        arg_str(cmd, "ABS");
        arg_str(cmd, cmd->version[0]);
        arg_ttl(cmd, cfg);
    } else {
        long k = random() % cfg->keys;
        if (!strcmp(w, "mset")) {
            build_mset(cmd, cfg, k, cfg->fields_per_write, op);
        } else if (!strcmp(w, "getallver")) {
            cmd->argc = 0;
            set_key(cmd, k);
            arg_str(cmd, "EXHGETALLWITHVER");
            arg_str(cmd, cmd->key);
        } else if (!strcmp(w, "gt")) {
            /* Every write carries a microsecond timestamp, so version 1 is older than any field. */
            if (cfg->stale_pct > 0 && random() % 100 < cfg->stale_pct) {
                version = 1;
            }
            cmd->argc = 0;
            set_key(cmd, k);
            set_field(cmd, 0, random() % cfg->fields, op, cfg->value_len, version);
            arg_str(cmd, "EXHSET");
            arg_str(cmd, cmd->key);
            arg_str(cmd, cmd->field[0]);
            arg(cmd, cmd->value[0], cfg->value_len);
            arg_str(cmd, "GT");
            arg_str(cmd, cmd->version[0]);
            arg_ttl(cmd, cfg);
        } else {
            usage();
        }
    }
    redisAppendCommandArgv(c, cmd->argc, cmd->argv, cmd->argvlen);
}

int main(int argc, char **argv) {
    Config cfg = {"127.0.0.1", 6379, NULL, 100000, 30, 10, 1000000, 64, 8, 0, 0, 1};
    int opt;
    while ((opt = getopt(argc, argv, "h:p:w:k:f:m:n:P:v:s:t:S:")) != -1) {
        switch (opt) {
            case 'h': cfg.host = optarg; break;
            case 'p': cfg.port = atoi(optarg); break;
            case 'w': cfg.workload = optarg; break;
            case 'k': cfg.keys = atol(optarg); break;
            case 'f': cfg.fields = atol(optarg); break;
            case 'm': cfg.fields_per_write = atol(optarg); break;
            case 'n': cfg.ops = atol(optarg); break;
            case 'P': cfg.pipeline = atol(optarg); break;
            case 'v': cfg.value_len = atol(optarg); break;
            case 's': cfg.stale_pct = atol(optarg); break;
            case 't': cfg.ttl = atol(optarg); break;
            case 'S': cfg.seed = (unsigned)atol(optarg); break;
            default: usage();
        }
    }
    if (cfg.workload == NULL || cfg.keys <= 0 || cfg.fields <= 0 || cfg.fields > MAX_FIELDS || cfg.value_len <= 0 ||
        cfg.value_len > MAX_VALUE_LEN || cfg.pipeline <= 0 || cfg.fields_per_write <= 0 || cfg.fields_per_write > cfg.fields || cfg.ttl < 0) {
        usage();
    }
    if (!strcmp(cfg.workload, "loadmset") || !strcmp(cfg.workload, "loadhset")) {
        cfg.ops = cfg.keys;
    } else if (!strcmp(cfg.workload, "loadset")) {
        cfg.ops = cfg.keys * cfg.fields;
    }
    srandom(cfg.seed);

    redisContext *c = redisConnect(cfg.host, cfg.port);
    if (c == NULL || c->err) {
        fprintf(stderr, "connect %s:%d: %s\n", cfg.host, cfg.port, c ? c->errstr : "out of memory");
        return 1;
    }

    Command *cmd = calloc(1, sizeof(*cmd));
    long long base_version = now_us();
    long long errors = 0, replies = 0, misses = 0;
    long long start = now_us();

    for (long op = 0; op < cfg.ops;) {
        int pending = 0;
        for (long i = 0; i < cfg.pipeline && op < cfg.ops; i++, op++) {
            append_op(c, &cfg, cmd, op, base_version);
            pending++;
        }
        for (int i = 0; i < pending; i++) {
            redisReply *reply;
            if (redisGetReply(c, (void **)&reply) != REDIS_OK) {
                fprintf(stderr, "reply: %s\n", c->errstr);
                return 1;
            }
            if (reply->type == REDIS_REPLY_ERROR) {
                if (errors == 0) {
                    fprintf(stderr, "first error reply: %s\n", reply->str);
                }
                errors++;
            } else if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 0) {
                misses++; /* a full read of a key that does not exist */
            }
            replies++;
            freeReplyObject(reply);
        }
    }

    double secs = (now_us() - start) / 1e6;
    printf("workload=%s keys=%ld fields=%ld fields_per_write=%ld ops=%ld pipeline=%ld value_len=%ld stale_pct=%ld secs=%.3f "
           "ttl=%ld ops_per_sec=%.0f errors=%lld misses=%lld replies=%lld\n",
           cfg.workload, cfg.keys, cfg.fields, cfg.fields_per_write, cfg.ops, cfg.pipeline, cfg.value_len, cfg.stale_pct, secs,
           cfg.ttl, cfg.ops / secs, errors, misses, replies);
    free(cmd);
    redisFree(c);
    /* A population run that got error replies did not populate what the measurement assumes. */
    return (errors > 0 && !strncmp(cfg.workload, "load", 4)) ? 1 : 0;
}
