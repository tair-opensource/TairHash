#!/usr/bin/env bash
# Runs one loadgen workload against a server with tairhash loaded, optionally with a
# replica and an AOF, and reports the server-side cost per operation: CPU time, bytes
# written to the replication stream and the AOF, and the replica's CPU time and
# per-command cost of applying the stream. Records a CPU profile of the master, the
# replica or both (perf on Linux, sample on macOS). See bench/README.md.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SERVER_SRC="$ROOT/.deps/redis-7.0"
MODULE=""
MODULE_ARGS=()
WORKLOAD="gt"
PRELOAD="loadset"
KEYS=100000
FIELDS=30
FIELDS_PER_WRITE=10
OPS=4000000
PIPELINE=64
STALE=10
TTL=0
SECONDS_TO_PROFILE=5
PORT=27101
REPLICA=0
AOF=0
PROFILE="master"
OUT=""

usage() {
    cat <<'EOF'
Usage: bench/profile.sh [options]

  --server-src DIR     Built Redis or Valkey source tree (default: .deps/redis-7.0)
  --module PATH        Module to load; none for a plain server (default: lib/tairhash_module.so,
                       or build/scan/lib/tairhash_module.so where builds are kept per expire mode)
  --module-arg NAME=V  Module load option for master and replica (repeatable), e.g. active_expire_period=100
  --workload W         loadgen workload (default: gt)
  --preload W          Population workload run first: loadset, loadmset, loadhset or none (default: loadset)
  --keys N             Keys (default: 100000)
  --fields N           Fields per key (default: 30)
  --fields-per-write N Fields per mset write (default: 10)
  --ops N              Operations (default: 4000000); size it to run longer than the profile
  --pipeline N         Pipeline depth (default: 64)
  --stale PCT          Share of gt writes carrying an old version (default: 10)
  --ttl SECONDS        Give the fields loadset and gt write this TTL (default: 0, none)
  --seconds N          Profile length (default: 5)
  --port N             Master port; a replica uses N+1 (default: 27101)
  --replica            Attach a replica and measure its cost of applying the stream
  --aof                Enable the master's AOF (appendfsync no)
  --profile WHO        Process to profile: master, replica, both or none (default: master)
  --out DIR            Output directory (default: build/profile/<workload>)
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --server-src) SERVER_SRC="$2"; shift 2 ;;
        --module) MODULE="$2"; shift 2 ;;
        --module-arg) MODULE_ARGS+=("${2%%=*}" "${2#*=}"); shift 2 ;;
        --workload) WORKLOAD="$2"; shift 2 ;;
        --preload) PRELOAD="$2"; shift 2 ;;
        --keys) KEYS="$2"; shift 2 ;;
        --fields) FIELDS="$2"; shift 2 ;;
        --fields-per-write) FIELDS_PER_WRITE="$2"; shift 2 ;;
        --ops) OPS="$2"; shift 2 ;;
        --pipeline) PIPELINE="$2"; shift 2 ;;
        --stale) STALE="$2"; shift 2 ;;
        --ttl) TTL="$2"; shift 2 ;;
        --seconds) SECONDS_TO_PROFILE="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --replica) REPLICA=1; shift ;;
        --aof) AOF=1; shift ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

case "$PROFILE" in
    master|replica|both|none) ;;
    *) echo "unknown --profile: $PROFILE" >&2; exit 2 ;;
esac
if [[ $REPLICA -eq 0 && ( "$PROFILE" == replica || "$PROFILE" == both ) ]]; then
    echo "--profile $PROFILE needs --replica" >&2
    exit 2
fi

# A plain cmake build writes the module to lib/. A tree that keeps one build per expire mode
# has the SCAN build under build/scan/lib, and lib/ may then hold any mode.
if [[ -z "$MODULE" ]]; then
    MODULE="$ROOT/lib/tairhash_module.so"
    [[ -f "$ROOT/build/scan/lib/tairhash_module.so" ]] && MODULE="$ROOT/build/scan/lib/tairhash_module.so"
fi
[[ "$MODULE" == none || -f "$MODULE" ]] || { echo "no module at $MODULE; build it or pass --module (see bench/README.md)" >&2; exit 1; }

OUT="${OUT:-$ROOT/build/profile/$WORKLOAD}"
SERVER=""
CLI=""
for name in redis valkey; do
    if [[ -x "$SERVER_SRC/src/$name-server" ]]; then
        SERVER="$SERVER_SRC/src/$name-server"
        CLI="$SERVER_SRC/src/$name-cli"
        break
    fi
done
LOADGEN="$ROOT/build/loadgen"
HIREDIS="$SERVER_SRC/deps/hiredis"
[[ -n "$SERVER" ]] || { echo "no server in $SERVER_SRC/src; build one there or pass --server-src (see bench/README.md)" >&2; exit 1; }

if [[ ! -x "$LOADGEN" || "$ROOT/bench/loadgen.c" -nt "$LOADGEN" ]]; then
    mkdir -p "$(dirname "$LOADGEN")"
    "${CC:-cc}" -O2 -Wall -Wextra -I"$HIREDIS" "$ROOT/bench/loadgen.c" "$HIREDIS/libhiredis.a" -o "$LOADGEN"
fi

# Only what this script writes is replaced: --out may name a directory holding other files.
mkdir -p "$OUT"
rm -rf "$OUT/master" "$OUT/replica"
mkdir -p "$OUT/master" "$OUT/replica"
REPLICA_PORT=$((PORT + 1))
started=()
cleanup() {
    for p in "${started[@]+"${started[@]}"}"; do
        "$CLI" -p "$p" shutdown nosave >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

start_server() {
    local port="$1" dir="$2"
    shift 2
    local args=(--port "$port" --save '' --daemonize yes --logfile "$dir/server.log" --dir "$dir" --pidfile "$dir/server.pid" "$@")
    [[ "$MODULE" != none ]] && args+=(--loadmodule "$MODULE" "${MODULE_ARGS[@]+"${MODULE_ARGS[@]}"}")
    "$SERVER" "${args[@]}"
    started+=("$port")
    for _ in $(seq 50); do
        [[ -s "$dir/server.pid" ]] && "$CLI" -p "$port" ping >/dev/null 2>&1 && return
        sleep 0.1
    done
    echo "server on port $port did not start; see $dir/server.log" >&2
    exit 1
}

info_field() {
    "$CLI" -p "$1" info "$2" | awk -F: -v k="$3" '$1 == k { gsub("\r", ""); print $2 }'
}

# Process CPU seconds (user + system) as the server accounts them.
cpu_seconds() {
    local user sys
    user="$(info_field "$1" cpu used_cpu_user)"
    sys="$(info_field "$1" cpu used_cpu_sys)"
    awk -v u="$user" -v s="$sys" 'BEGIN { printf "%.6f", u + s }'
}

master_args=(--appendonly no)
if [[ $AOF -eq 1 ]]; then
    # No automatic rewrite: it would reset aof_current_size in the middle of the run.
    master_args=(--appendonly yes --appendfsync no --auto-aof-rewrite-percentage 0)
fi
start_server "$PORT" "$OUT/master" "${master_args[@]}"
MASTER_PID="$(cat "$OUT/master/server.pid")"

if [[ $REPLICA -eq 1 ]]; then
    start_server "$REPLICA_PORT" "$OUT/replica" --appendonly no
    REPLICA_PID="$(cat "$OUT/replica/server.pid")"
    "$CLI" -p "$REPLICA_PORT" replicaof 127.0.0.1 "$PORT" > /dev/null
    for _ in $(seq 100); do
        [[ "$(info_field "$REPLICA_PORT" replication master_link_status)" == up ]] && break
        sleep 0.1
    done
    [[ "$(info_field "$REPLICA_PORT" replication master_link_status)" == up ]] || { echo "replica did not connect" >&2; exit 1; }
fi

wait_for_replica() {
    [[ $REPLICA -eq 1 ]] || return 0
    local target
    target="$(info_field "$PORT" replication master_repl_offset)"
    for _ in $(seq 600); do
        [[ "$(info_field "$REPLICA_PORT" replication slave_repl_offset)" -ge "$target" ]] && return
        sleep 0.1
    done
    echo "replica did not catch up to offset $target" >&2
    exit 1
}

aof_bytes() {
    if [[ $AOF -eq 1 ]]; then
        info_field "$PORT" persistence aof_current_size
    else
        echo 0
    fi
}

# used_memory without what replication holds, the backlog and the replicas' output buffers:
# those follow the traffic, not the keys.
data_memory() {
    local used backlog replicas
    used="$(info_field "$PORT" memory used_memory)"
    backlog="$(info_field "$PORT" memory mem_replication_backlog)"
    replicas="$(info_field "$PORT" memory mem_clients_slaves)"
    echo $(( used - ${backlog:-0} - ${replicas:-0} ))
}

empty_mem="$(data_memory)"
if [[ "$PRELOAD" != none ]]; then
    "$LOADGEN" -p "$PORT" -w "$PRELOAD" -k "$KEYS" -f "$FIELDS" -t "$TTL" > "$OUT/preload.txt"
fi
wait_for_replica
# Taken before the workload, which may add keys or let fields with a TTL expire.
loaded_mem="$(data_memory)"
keys_loaded="$("$CLI" -p "$PORT" dbsize)"

"$CLI" -p "$PORT" config resetstat > /dev/null
[[ $REPLICA -eq 1 ]] && "$CLI" -p "$REPLICA_PORT" config resetstat > /dev/null
master_cpu0="$(cpu_seconds "$PORT")"
[[ $REPLICA -eq 1 ]] && replica_cpu0="$(cpu_seconds "$REPLICA_PORT")"
offset0="$(info_field "$PORT" replication master_repl_offset)"
aof0="$(aof_bytes)"

"$LOADGEN" -p "$PORT" -w "$WORKLOAD" -k "$KEYS" -f "$FIELDS" -m "$FIELDS_PER_WRITE" -n "$OPS" -P "$PIPELINE" -s "$STALE" -t "$TTL" \
    > "$OUT/loadgen.txt" 2> "$OUT/loadgen.err" &
loadgen_pid=$!

profile_pid() {
    local pid="$1" dir="$2"
    if [[ "$(uname -s)" == "Darwin" ]]; then
        sample "$pid" "$SECONDS_TO_PROFILE" 1 -file "$dir/sample.txt" > /dev/null 2>&1
    else
        perf record -F 999 -g -p "$pid" -o "$dir/perf.data" -- sleep "$SECONDS_TO_PROFILE" > /dev/null 2>&1
        perf report -i "$dir/perf.data" --no-children --stdio > "$dir/perf-flat.txt" 2> /dev/null
        perf report -i "$dir/perf.data" --children --stdio > "$dir/perf-children.txt" 2> /dev/null
    fi
}

sleep 1
profilers=()
if [[ "$PROFILE" == master || "$PROFILE" == both ]]; then
    profile_pid "$MASTER_PID" "$OUT/master" &
    profilers+=($!)
fi
if [[ "$PROFILE" == replica || "$PROFILE" == both ]]; then
    profile_pid "$REPLICA_PID" "$OUT/replica" &
    profilers+=($!)
fi
for p in "${profilers[@]+"${profilers[@]}"}"; do
    wait "$p"
done
wait "$loadgen_pid"
wait_for_replica

# The population workloads run as many operations as they need, whatever --ops says.
ran_ops="$(sed -n 's/.* ops=\([0-9]*\) .*/\1/p' "$OUT/loadgen.txt")"
per_op() {
    awk -v a="$1" -v b="$2" -v n="$ran_ops" -v scale="$3" 'BEGIN { printf "%.1f", (b - a) * scale / n }'
}

echo "$(cat "$OUT/loadgen.txt")"
master_line="master: cpu_us_per_op=$(per_op "$master_cpu0" "$(cpu_seconds "$PORT")" 1000000)"
# Without a replica the replication offset does not move, which would read as no replication cost.
[[ $REPLICA -eq 1 ]] && master_line+=" repl_bytes_per_op=$(per_op "$offset0" "$(info_field "$PORT" replication master_repl_offset)" 1)"
[[ $AOF -eq 1 ]] && master_line+=" aof_bytes_per_op=$(per_op "$aof0" "$(aof_bytes)" 1)"
echo "$master_line"
"$CLI" -p "$PORT" info commandstats | tr -d '\r' | grep -i '^cmdstat_exh' > "$OUT/master/commandstats.txt" || true
if [[ $REPLICA -eq 1 ]]; then
    echo "replica: cpu_us_per_op=$(per_op "$replica_cpu0" "$(cpu_seconds "$REPLICA_PORT")" 1000000)"
    "$CLI" -p "$REPLICA_PORT" info commandstats | tr -d '\r' | grep -i '^cmdstat_exh' > "$OUT/replica/commandstats.txt" || true
    sed 's/^/replica /' "$OUT/replica/commandstats.txt"
fi
if [[ "$keys_loaded" -gt 0 ]]; then
    echo "memory: used_memory_per_key=$(( (loaded_mem - empty_mem) / keys_loaded )) after preload of $keys_loaded keys"
fi
echo "module: $MODULE"
echo "output: $OUT"
