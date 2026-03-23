#!/usr/bin/env bash
# ─── FluxMQ Run Script ─────────────────────────────────────────────────────
#
# Usage:
#   ./scripts/run.sh [target] [options] [-- extra_args]
#
# Targets:
#   broker       Start a standalone FluxMQ broker (default)
#   cluster      Start a 3-broker local cluster
#   tests        Build and run all C++ tests
#   benchmark    Run the Go benchmark tool
#   cli          Run the Go CLI tool (pass args after --)
#   docker:build Build the Docker image
#   docker:up    Start the Docker Compose cluster
#   docker:down  Stop and remove Docker cluster
#   docker:logs  Tail Docker cluster logs
#
# Options:
#   --build      Build before running
#   --debug      Use Debug build
#   --valgrind   Run under valgrind (broker/tests)
#   --help       Show this help message
#
# Examples:
#   ./scripts/run.sh broker --build
#   ./scripts/run.sh cluster --build
#   ./scripts/run.sh cli -- topic create --name orders --partitions 4
#   ./scripts/run.sh benchmark -- produce --topic bench --num-msgs 100000

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# ── Defaults ────────────────────────────────────────────────────────────────

TARGET="broker"
DO_BUILD=false
BUILD_TYPE="Release"
USE_VALGRIND=false
EXTRA_ARGS=()

# ── Parse arguments ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        broker|cluster|tests|benchmark|cli|docker:build|docker:up|docker:down|docker:logs)
            TARGET="$1" ;;
        --build)     DO_BUILD=true ;;
        --debug)     BUILD_TYPE="Debug" ;;
        --valgrind)  USE_VALGRIND=true ;;
        --help|-h)   sed -n '/^# Usage:/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# //'; exit 0 ;;
        --)          shift; EXTRA_ARGS=("$@"); break ;;
        *) error "Unknown argument: $1"; exit 1 ;;
    esac
    shift
done

# ── Helpers ─────────────────────────────────────────────────────────────────

ensure_binary() {
    local name="$1"
    local paths=("$BUILD_DIR/$name" "$BUILD_DIR/src/$name")
    for p in "${paths[@]}"; do
        if [ -x "$p" ]; then
            echo "$p"
            return
        fi
    done
    error "Binary not found: $name (run with --build)"
    exit 1
}

maybe_build() {
    if $DO_BUILD; then
        cmake_configure_and_build "$BUILD_TYPE"
    fi
}

# ── Targets ─────────────────────────────────────────────────────────────────

run_broker() {
    maybe_build
    local binary
    binary=$(ensure_binary fluxmq)
    info "Starting FluxMQ broker..."
    local cmd="$binary"
    if $USE_VALGRIND; then
        cmd="valgrind --leak-check=full $binary"
    fi
    exec $cmd "${EXTRA_ARGS[@]}"
}

run_cluster() {
    maybe_build
    local binary
    binary=$(ensure_binary fluxmq)

    local cluster_dir
    cluster_dir=$(mktemp -d /tmp/fluxmq-cluster-XXXXXX)
    info "Cluster coordination directory: $cluster_dir"

    # Broker 1
    local data1
    data1=$(mktemp -d /tmp/fluxmq-b1-XXXXXX)
    info "Starting broker 1 (port 9092)..."
    "$binary" --broker-id=1 --port=9092 --cluster-dir="$cluster_dir" --data-dir="$data1" &
    local pid1=$!

    sleep 1

    # Broker 2
    local data2
    data2=$(mktemp -d /tmp/fluxmq-b2-XXXXXX)
    info "Starting broker 2 (port 9094)..."
    "$binary" --broker-id=2 --port=9094 --cluster-dir="$cluster_dir" --data-dir="$data2" &
    local pid2=$!

    sleep 1

    # Broker 3
    local data3
    data3=$(mktemp -d /tmp/fluxmq-b3-XXXXXX)
    info "Starting broker 3 (port 9096)..."
    "$binary" --broker-id=3 --port=9096 --cluster-dir="$cluster_dir" --data-dir="$data3" --replication-factor=3 &
    local pid3=$!

    success "3-broker cluster started (PIDs: $pid1, $pid2, $pid3)"
    echo "  Broker 1: localhost:9092 (metrics: localhost:9093)"
    echo "  Broker 2: localhost:9094 (metrics: localhost:9095)"
    echo "  Broker 3: localhost:9096 (metrics: localhost:9097)"
    echo ""
    echo "Press Ctrl+C to stop all brokers."

    trap "kill $pid1 $pid2 $pid3 2>/dev/null; rm -rf $cluster_dir $data1 $data2 $data3; echo 'Cluster stopped.'" EXIT
    wait
}

run_tests() {
    maybe_build
    "$REPO_ROOT/scripts/test.sh" cpp
}

run_benchmark() {
    local bench="$BUILD_DIR/fluxmq-bench"
    if [ ! -x "$bench" ]; then
        info "Building benchmark tool..."
        (cd "$SDK_DIR" && go build -o "$bench" ./cmd/fluxmq-bench)
    fi
    exec "$bench" "${EXTRA_ARGS[@]}"
}

run_cli() {
    local cli="$BUILD_DIR/fluxmq-cli"
    if [ ! -x "$cli" ]; then
        info "Building CLI tool..."
        (cd "$SDK_DIR" && go build -o "$cli" ./cmd/fluxmq)
    fi
    exec "$cli" "${EXTRA_ARGS[@]}"
}

run_docker() {
    local subcmd="$1"
    case "$subcmd" in
        build) docker build -t fluxmq-broker "$REPO_ROOT" ;;
        up)    docker compose -f "$REPO_ROOT/docker-compose.yml" up --build -d ;;
        down)  docker compose -f "$REPO_ROOT/docker-compose.yml" down -v ;;
        logs)  docker compose -f "$REPO_ROOT/docker-compose.yml" logs -f ;;
    esac
}

# ── Dispatch ────────────────────────────────────────────────────────────────

case "$TARGET" in
    broker)       run_broker ;;
    cluster)      run_cluster ;;
    tests)        run_tests ;;
    benchmark)    run_benchmark ;;
    cli)          run_cli ;;
    docker:build) run_docker build ;;
    docker:up)    run_docker up ;;
    docker:down)  run_docker down ;;
    docker:logs)  run_docker logs ;;
esac
