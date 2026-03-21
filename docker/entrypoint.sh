#!/bin/sh
set -e

# Translate FLUXMQ_* environment variables into broker command-line flags.
#
# Any additional arguments passed as Docker CMD are appended verbatim,
# allowing direct flag overrides at runtime:
#   docker run fluxmq-broker --broker-timeout-ms=5000
#
# Environment variables and their corresponding flags:
#   FLUXMQ_PORT               --port
#   FLUXMQ_METRICS_PORT       --metrics-port
#   FLUXMQ_BROKER_ID          --broker-id
#   FLUXMQ_BROKER_HOST        --broker-host
#   FLUXMQ_DATA_DIR           --data-dir
#   FLUXMQ_CLUSTER_DIR        --cluster-dir
#   FLUXMQ_REPLICATION_FACTOR --replication-factor
#   FLUXMQ_REPLICA_LAG_MS     --replica-lag-ms
#   FLUXMQ_BROKER_TIMEOUT_MS  --broker-timeout-ms
#   FLUXMQ_SESSION_TIMEOUT_MS --session-timeout-ms

ARGS=""
[ -n "${FLUXMQ_PORT}" ]               && ARGS="$ARGS --port=${FLUXMQ_PORT}"
[ -n "${FLUXMQ_METRICS_PORT}" ]       && ARGS="$ARGS --metrics-port=${FLUXMQ_METRICS_PORT}"
[ -n "${FLUXMQ_BROKER_ID}" ]          && ARGS="$ARGS --broker-id=${FLUXMQ_BROKER_ID}"
[ -n "${FLUXMQ_BROKER_HOST}" ]        && ARGS="$ARGS --broker-host=${FLUXMQ_BROKER_HOST}"
[ -n "${FLUXMQ_DATA_DIR}" ]           && ARGS="$ARGS --data-dir=${FLUXMQ_DATA_DIR}"
[ -n "${FLUXMQ_CLUSTER_DIR}" ]        && ARGS="$ARGS --cluster-dir=${FLUXMQ_CLUSTER_DIR}"
[ -n "${FLUXMQ_REPLICATION_FACTOR}" ] && ARGS="$ARGS --replication-factor=${FLUXMQ_REPLICATION_FACTOR}"
[ -n "${FLUXMQ_REPLICA_LAG_MS}" ]     && ARGS="$ARGS --replica-lag-ms=${FLUXMQ_REPLICA_LAG_MS}"
[ -n "${FLUXMQ_BROKER_TIMEOUT_MS}" ]  && ARGS="$ARGS --broker-timeout-ms=${FLUXMQ_BROKER_TIMEOUT_MS}"
[ -n "${FLUXMQ_SESSION_TIMEOUT_MS}" ] && ARGS="$ARGS --session-timeout-ms=${FLUXMQ_SESSION_TIMEOUT_MS}"

exec /usr/local/bin/fluxmq $ARGS "$@"
