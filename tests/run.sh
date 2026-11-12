#!/usr/bin/env bash
# Build tairhash_module and run its TCL suites inside a server source tree's test
# framework (Redis or Valkey). CI runs every job through this script, so a local
# run and a CI job execute the same steps.
#
# The server tree is cloned and built once per (server, ref, sanitizer) under
# .deps/ and reused. Each module variant (expire mode, sanitizer, coverage) builds into its
# own directory under build/, so variants never overwrite each other.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPS="$ROOT/.deps"

SERVER="redis"
SERVER_REPO=""
SERVER_REF="7.0"
SERVER_SRC=""
MODE="scan"
SANITIZER=""
COVERAGE=0
CLIENTS=4
JOBS=""
SUITES=()
RUNTEST_ARGS=()

usage() {
    cat <<'EOF'
Usage: tests/run.sh [options] [-- extra runtest args]

  --server NAME        Server to test against: redis (default) or valkey
  --server-ref REF     Server git branch/tag (default: 7.0)
  --server-repo URL    Server repository (default: the upstream repository of --server)
  --server-src DIR     Use an already-built server source tree instead of cloning
  --mode MODE          Expire algorithm: scan (default), sort, slab
  --sanitizer KIND     Build module and server with -fsanitize=KIND (address|undefined)
  --coverage           Instrument the module and print a line/function coverage report
  --suite NAME         Run tests/NAME.tcl (repeatable; default: every tests/*.tcl)
  --clients N          Parallel test clients (default: 4)
  --jobs N             Parallel build jobs (default: CPU count, at most 8)
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --server) SERVER="$2"; shift 2 ;;
        --server-ref) SERVER_REF="$2"; shift 2 ;;
        --server-repo) SERVER_REPO="$2"; shift 2 ;;
        --server-src) SERVER_SRC="$2"; shift 2 ;;
        --mode) MODE="$2"; shift 2 ;;
        --sanitizer) SANITIZER="$2"; shift 2 ;;
        --coverage) COVERAGE=1; shift ;;
        --suite) SUITES+=("$2"); shift 2 ;;
        --clients) CLIENTS="$2"; shift 2 ;;
        --jobs) JOBS="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        --) shift; RUNTEST_ARGS=("$@"); break ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

case "$SERVER" in
    redis) DEFAULT_REPO="https://github.com/redis/redis" ;;
    valkey) DEFAULT_REPO="https://github.com/valkey-io/valkey" ;;
    *) echo "unknown server: $SERVER" >&2; exit 2 ;;
esac
SERVER_REPO="${SERVER_REPO:-$DEFAULT_REPO}"

# Name scan explicitly: it is the default only before 2.0, whose CMake ignores TAIRHASH_EXPIRE_MODE.
case "$MODE" in
    scan) MODE_FLAGS=(-DTAIRHASH_EXPIRE_MODE=scan) ;;
    sort) MODE_FLAGS=(-DSORT_MODE=ON) ;;
    slab) MODE_FLAGS=(-DSLAB_MODE=ON) ;;
    *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac

case "$SANITIZER" in
    ""|address|undefined) ;;
    *) echo "unsupported sanitizer: $SANITIZER" >&2; exit 2 ;;
esac

if [[ "$(uname -s)" == "Darwin" ]]; then
    CPUS="$(sysctl -n hw.ncpu)"
    GCOV=(xcrun llvm-cov gcov)
else
    CPUS="$(nproc)"
    GCOV=(gcov)
fi
JOBS="${JOBS:-$(( CPUS > 8 ? 8 : CPUS ))}"

build_server() {
    local src="$1"
    if [[ ! -d "$src/.git" ]]; then
        mkdir -p "$(dirname "$src")"
        git clone --quiet --depth 1 --branch "$SERVER_REF" "$SERVER_REPO" "$src"
    fi
    if [[ ! -x "$src/src/redis-server" && ! -x "$src/src/valkey-server" ]]; then
        local make_args=(-C "$src" -j"$JOBS")
        if [[ -n "$SANITIZER" ]]; then
            make_args+=(SANITIZER="$SANITIZER" MALLOC=libc)
        else
            make_args+=(MALLOC=jemalloc)
        fi
        echo "==> building $SERVER $SERVER_REF${SANITIZER:+ ($SANITIZER)}"
        make "${make_args[@]}" > "$src.build.log" 2>&1 || { tail -40 "$src.build.log" >&2; exit 1; }
    fi
}

if [[ -z "$SERVER_SRC" ]]; then
    SERVER_SRC="$DEPS/$(basename "$SERVER_REPO")-$SERVER_REF${SANITIZER:+-$SANITIZER}"
    build_server "$SERVER_SRC"
fi

# The test framework puts each server's unix socket under <server>/tests/tmp, and
# socket paths are limited to ~104 bytes, so keep that directory on a short path.
# The link is recreated when /tmp was cleaned since the last run.
if [[ ! -L "$SERVER_SRC/tests/tmp" || ! -d "$SERVER_SRC/tests/tmp/" ]]; then
    rm -rf "$SERVER_SRC/tests/tmp"
    ln -s "$(mktemp -d /tmp/tairhash-tests.XXXXXX)" "$SERVER_SRC/tests/tmp"
fi

VARIANT="$MODE${SANITIZER:+-$SANITIZER}"
[[ $COVERAGE -eq 1 ]] && VARIANT="$VARIANT-cov"
BUILD_DIR="$ROOT/build/$VARIANT"
MODULE_DIR="$BUILD_DIR/lib"

cmake_args=(-S "$ROOT" -B "$BUILD_DIR" -DTAIRHASH_OUTPUT_DIR="$MODULE_DIR" "${MODE_FLAGS[@]}")
[[ -n "$SANITIZER" ]] && cmake_args+=(-DSANITIZER_MODE="$SANITIZER")
[[ $COVERAGE -eq 1 ]] && cmake_args+=(-DGCOV_MODE=TRUE)

echo "==> building module ($VARIANT)"
cmake "${cmake_args[@]}" > /dev/null
cmake --build "$BUILD_DIR" -j"$JOBS" > "$BUILD_DIR.log" 2>&1 || { tail -40 "$BUILD_DIR.log" >&2; exit 1; }

if [[ $COVERAGE -eq 1 ]]; then
    find "$BUILD_DIR" -name '*.gcda' -delete
fi

if [[ ${#SUITES[@]} -eq 0 ]]; then
    for f in "$ROOT"/tests/*.tcl; do
        SUITES+=("$(basename "$f" .tcl)")
    done
fi

# Older harnesses (Redis 5.0) do not know --dump-logs; where it exists it prints the
# server log of a failing test, which is the only trace a CI failure leaves.
HARNESS_ARGS=()
if grep -q -- '--dump-logs' "$SERVER_SRC/tests/test_helper.tcl"; then
    HARNESS_ARGS+=(--dump-logs)
fi

status=0
for suite in "${SUITES[@]}"; do
    sed -e "s#your_path#$MODULE_DIR#g" "$ROOT/tests/$suite.tcl" > "$SERVER_SRC/tests/unit/type/$suite.tcl"
    echo "==> running $suite ($VARIANT against $(basename "$SERVER_SRC"))"
    if ! (cd "$SERVER_SRC" && ./runtest --single "unit/type/$suite" --clients "$CLIENTS" "${HARNESS_ARGS[@]+"${HARNESS_ARGS[@]}"}" "${RUNTEST_ARGS[@]+"${RUNTEST_ARGS[@]}"}"); then
        status=1
    fi
done

if [[ $COVERAGE -eq 1 ]]; then
    echo "==> coverage (module sources)"
    objdir="$(find "$BUILD_DIR" -type d -name 'tairhash_module.dir' | head -1)"
    (
        cd "$objdir"
        for gcda in *.gcda; do
            "${GCOV[@]}" -f -n "$gcda" 2>/dev/null
        done
    ) > "$BUILD_DIR/coverage.txt"
    awk '/^File .*src\/[a-z_]+\.c/{f=$2} /^Lines executed/{if (f!="") {print f, $0; f=""}}' "$BUILD_DIR/coverage.txt" | sed "s#'##g; s#$ROOT/##"
    echo "(per-function detail: $BUILD_DIR/coverage.txt)"
fi

exit $status
