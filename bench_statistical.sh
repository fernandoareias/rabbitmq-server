#!/usr/bin/env bash
##
## bench_statistical.sh — 30 runs cada (baseline vs io_uring) + análise estatística.
##
## Uso:
##   bash bench_statistical.sh              # 30 runs × 10s, 4 prod, 1 KB
##   bash bench_statistical.sh 20 10 2048   # 20 runs × 10s, 10 prod, 2 KB
##
set -euo pipefail

BENCH_ROOT="$(cd "$(dirname "$0")" && pwd)"
RUNS="${1:-30}"
DURATION="${2:-10}"     # segundos por run
PRODUCERS="${3:-4}"
MSG_SIZE="${4:-1024}"

PERF_TEST_JAR="$BENCH_ROOT/perf-test.jar"
RABBITMQCTL="$BENCH_ROOT/escript/rabbitmqctl"
export ERL_LIBS="$BENCH_ROOT/plugins"

HOSTNAME_SHORT=$(hostname -s)
NODE_NAME="rabbit@${HOSTNAME_SHORT}"
# Match the TEST_TMPDIR the Makefile computes: $TMPDIR/rabbitmq-test-instances.
TEST_TMPDIR="${TMPDIR:-/tmp}/rabbitmq-test-instances"
NODE_DIR="${TEST_TMPDIR}/${NODE_NAME}"
CONF_DIR="${TEST_TMPDIR}/conf.d"
IO_URING_CONF="${CONF_DIR}/90-io-uring.conf"

RESULT_DIR="$BENCH_ROOT/bench-results"
BASELINE_CSV="$RESULT_DIR/baseline.csv"
IOURING_CSV="$RESULT_DIR/iouring.csv"
mkdir -p "$RESULT_DIR"

QUEUE_NAME="bench-stat"

##------------------------------------------------------------------------
log()  { printf '[bench] %s\n' "$*"; }
die()  { printf '[bench] ERROR: %s\n' "$*" >&2; stop_broker; exit 1; }
bar()  { printf '[bench] ─────────────────────────────────────────────\n'; }

##------------------------------------------------------------------------
generate_conf() {
    mkdir -p "$CONF_DIR"
    printf '%s\n' \
        "loopback_users = none" \
        "cluster_name = localhost" \
        "raft.data_dir = ${NODE_DIR}/mnesia/${NODE_NAME}/quorum" \
        > "${CONF_DIR}/00-base.conf"
}

# Pre-populate feature_flags so the broker skips migrations that trigger a
# known horus compilation bug (tie_binding_to_dest_with_keep_while_cond).
prefill_feature_flags() {
    mkdir -p "$NODE_DIR"
    cat > "${NODE_DIR}/feature_flags" <<'EOF'
[classic_mirrored_queue_version,
 classic_queue_type_delivery_support,
 detailed_queues_endpoint,
 direct_exchange_routing_v2,
 drop_unroutable_metric,
 empty_basic_get_metric,
 feature_flags_v2,
 implicit_default_bindings,
 khepri_db,
 listener_records_in_ets,
 maintenance_mode_status,
 message_containers,
 message_containers_deaths_v2,
 quorum_queue,
 quorum_queue_non_voters,
 rabbit_exchange_type_local_random,
 'rabbitmq_4.0.0',
 'rabbitmq_4.1.0',
 'rabbitmq_4.2.0',
 'rabbitmq_4.3.0',
 restart_streams,
 stream_filtering,
 stream_queue,
 stream_sac_coordinator_unblock_group,
 stream_single_active_consumer,
 stream_update_config_command,
 tie_binding_to_dest_with_keep_while_cond,
 topic_binding_projection_v4,
 track_qq_members_uids,
 tracking_records_in_ets,
 user_limits,
 virtual_host_metadata].
EOF
    log "feature_flags pré-populado em ${NODE_DIR}/feature_flags"
}

start_broker() {
    local label="$1"
    local enable_io_uring="${2:-false}"

    log "Iniciando broker ($label)..."
    make -C "$BENCH_ROOT" virgin-test-tmpdir > /dev/null 2>&1
    generate_conf
    prefill_feature_flags

    if [[ "$enable_io_uring" == "true" ]]; then
        printf 'message_store.io_uring = true\n' > "$IO_URING_CONF"
        log "io_uring habilitado."
    fi

    # Export as environment variable so the broker process inherits it.
    # Passing as a Make variable (FOO=bar make) does NOT propagate to
    # subshell commands in recipes — env export is required.
    RABBITMQ_CONFIG_FILES="$CONF_DIR" \
        make -C "$BENCH_ROOT" run-background-broker \
        >> "$RESULT_DIR/broker-${label}.log" 2>&1

    log "Aguardando startup (máx 90s)..."
    local deadline=$(( $(date +%s) + 90 ))
    until "$RABBITMQCTL" -n "$NODE_NAME" await_startup > /dev/null 2>&1; do
        (( $(date +%s) > deadline )) && die "Broker não ficou pronto em 90s."
        sleep 2
    done
    log "Broker pronto."
}

stop_broker() {
    log "Parando broker..."
    make -C "$BENCH_ROOT" stop-node > /dev/null 2>&1 || true
    local deadline=$(( $(date +%s) + 30 ))
    while [[ -f "${NODE_DIR}/${NODE_NAME}.pid" ]]; do
        (( $(date +%s) > deadline )) && break
        sleep 1
    done
    rm -f "$IO_URING_CONF"
    sleep 2
}

##------------------------------------------------------------------------
delete_queue() {
    curl -s -u guest:guest \
        -X DELETE "http://localhost:15672/api/queues/%2F/${QUEUE_NAME}" \
        > /dev/null 2>&1 || true
}

run_once() {
    local run_num="$1"
    local tmp_out="/tmp/bench-run-${run_num}.txt"

    java -jar "$PERF_TEST_JAR" \
        --uri "amqp://guest:guest@localhost:5672" \
        --queue "$QUEUE_NAME" \
        --queue-args "x-max-length=2000000" \
        --producers  "$PRODUCERS" \
        --consumers  0 \
        --flag       persistent \
        --size       "$MSG_SIZE" \
        --confirm    200 \
        --confirm-timeout 60 \
        --time       "$DURATION" \
        > "$tmp_out" 2>&1

    local throughput p99
    throughput=$(grep "sending rate avg" "$tmp_out" \
        | grep -oP '[\d,]+(?= msg/s)' | tr -d ',' | tail -1 || echo "")
    p99=$(grep "confirm latency min" "$tmp_out" \
        | tail -1 | grep -oP '\d+(?=\s*µs)' | tail -1 || echo "")

    if [[ -z "$throughput" || -z "$p99" ]]; then
        printf >&2 '[bench]   Run %d: parse falhou, descartando.\n' "$run_num"
        cat "$tmp_out" >> "$RESULT_DIR/parse-errors.log"
        return 1
    fi

    # Progresso para stderr; CSV para stdout (capturado pelo caller)
    printf >&2 '[bench]   run %-3d  %s msg/s  p99=%s µs\n' \
        "$run_num" "$throughput" "$p99"
    printf '%s,%s\n' "$throughput" "$p99"

    delete_queue
    sleep 1
}

collect_runs() {
    local label="$1"
    local csv_file="$2"

    bar
    log "Coletando $RUNS runs — $label"
    bar

    # Warmup (descartado)
    log "Warmup run (descartado)..."
    local tmp_warmup="/tmp/bench-warmup.txt"
    java -jar "$PERF_TEST_JAR" \
        --uri "amqp://guest:guest@localhost:5672" \
        --queue "$QUEUE_NAME" \
        --queue-args "x-max-length=2000000" \
        --producers "$PRODUCERS" --consumers 0 \
        --flag persistent --size "$MSG_SIZE" \
        --confirm 200 --time "$DURATION" \
        > "$tmp_warmup" 2>&1 || true
    delete_queue
    sleep 2

    # Limpa CSV anterior
    > "$csv_file"

    local done=0 attempt=0
    while (( done < RUNS )); do
        attempt=$(( attempt + 1 ))
        if (( attempt > RUNS * 2 )); then
            log "Muitas falhas. Abortando."
            break
        fi
        local csv_line
        if csv_line=$(run_once "$attempt"); then
            echo "$csv_line" >> "$csv_file"
            done=$(( done + 1 ))
        fi
    done
    log "Coleta finalizada: $done runs em $csv_file"
}

##------------------------------------------------------------------------
main() {
    [[ -f "$PERF_TEST_JAR" ]] || die "perf-test.jar não encontrado. Rode bench_broker.sh primeiro."
    [[ -x "$RABBITMQCTL"   ]] || die "rabbitmqctl não encontrado."

    log "Benchmark estatístico: ${RUNS} runs × ${DURATION}s | ${PRODUCERS} prod | ${MSG_SIZE}B"
    log "Total estimado: ~$(( (RUNS + 1) * DURATION * 2 / 60 )) minutos"

    bar
    log "=== FASE 1: Baseline (prim_file) ==="
    bar
    start_broker "stat-baseline" "false"
    collect_runs "baseline" "$BASELINE_CSV"
    stop_broker

    bar
    log "=== FASE 2: io_uring ==="
    bar
    start_broker "stat-iouring" "true"
    collect_runs "io_uring" "$IOURING_CSV"
    stop_broker

    bar
    log "Dados coletados. Rodando análise estatística..."
    BENCH_BASELINE_CSV="$BASELINE_CSV" \
    BENCH_IOURING_CSV="$IOURING_CSV" \
    BENCH_RUNS="$RUNS" \
    BENCH_DURATION="$DURATION" \
    BENCH_PRODUCERS="$PRODUCERS" \
    BENCH_SIZE="$MSG_SIZE" \
    BENCH_CONSUMER_MODE="0" \
        jupyter nbconvert --to notebook --execute \
            --output "$RESULT_DIR/bench_analyze.ipynb" \
            "$BENCH_ROOT/bench_analyze.ipynb" 2>&1 | grep -v "^$"
    log "Notebook salvo em: $RESULT_DIR/bench_analyze.ipynb"
}

main "$@"
