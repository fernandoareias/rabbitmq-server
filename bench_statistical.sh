#!/usr/bin/env bash
##
## bench_statistical.sh — dual-broker interleaved: baseline e io_uring sobem
## simultaneamente, sem restart entre pares. Elimina cold start por completo.
##
## Uso:
##   bash bench_statistical.sh              # 30 pares × 10s, 4 prod, 1 KB
##   bash bench_statistical.sh 20 10 4 2048 # 20 pares × 10s, 4 prod, 2 KB
##
set -euo pipefail

# Ensure Erlang 27 (required by RabbitMQ 4.x; OTP 29 breaks Horus).
# Activate kerl if the current erl is not OTP 27.x.
_otp_release=$(erl -eval 'io:format("~s",[erlang:system_info(otp_release)]),halt().' -noshell 2>/dev/null || echo "unknown")
if [[ "$_otp_release" != 27* ]]; then
    _kerl_activate="${HOME}/.kerl/installs/27.3.4.12-rabbitmq/activate"
    [[ -f "$_kerl_activate" ]] || { printf '[bench] ERROR: OTP %s detectado e kerl 27 não encontrado em %s\n' "$_otp_release" "$_kerl_activate" >&2; exit 1; }
    # shellcheck source=/dev/null
    # Disable -u temporarily: the kerl activate script references variables
    # that may be unset, which would abort the script under set -u.
    set +u
    source "$_kerl_activate"
    set -u
    printf '[bench] Ativado Erlang %s via kerl (era OTP %s).\n' \
        "$(erl -eval 'io:format("~s",[erlang:system_info(otp_release)]),halt().' -noshell 2>/dev/null)" \
        "$_otp_release"
fi
unset _otp_release _kerl_activate

BENCH_ROOT="$(cd "$(dirname "$0")" && pwd)"
RUNS="${1:-30}"
DURATION="${2:-10}"     # segundos por run
PRODUCERS="${3:-4}"
MSG_SIZE="${4:-1024}"
WARMUP_PAIRS="${5:-3}"  # pares descartados para aquecer ambos os stores

PERF_TEST_JAR="$BENCH_ROOT/perf-test.jar"
RABBITMQCTL="$BENCH_ROOT/escript/rabbitmqctl"
RABBITMQ_SERVER="$BENCH_ROOT/sbin/rabbitmq-server"
export ERL_LIBS="$BENCH_ROOT/plugins"

HOSTNAME_SHORT=$(hostname -s)
TEST_TMPDIR="${TMPDIR:-/tmp}/rabbitmq-test-instances"

NODE_BASELINE="rabbit-b@${HOSTNAME_SHORT}"
NODE_IOURING="rabbit-u@${HOSTNAME_SHORT}"
PORT_BASELINE=5672
PORT_IOURING=5673
DIR_BASELINE="${TEST_TMPDIR}/rabbit-b@${HOSTNAME_SHORT}"
DIR_IOURING="${TEST_TMPDIR}/rabbit-u@${HOSTNAME_SHORT}"
CONF_BASELINE="${TEST_TMPDIR}/conf-baseline"
CONF_IOURING="${TEST_TMPDIR}/conf-iouring"

RESULT_DIR="$BENCH_ROOT/bench-results"
BASELINE_CSV="$RESULT_DIR/baseline.csv"
IOURING_CSV="$RESULT_DIR/iouring.csv"
mkdir -p "$RESULT_DIR"

QUEUE_NAME="bench-stat"

##------------------------------------------------------------------------
log()  { printf '[bench] %s\n' "$*"; }
die()  { printf '[bench] ERROR: %s\n' "$*" >&2; stop_all_nodes; exit 1; }
bar()  { printf '[bench] ─────────────────────────────────────────────\n'; }

##------------------------------------------------------------------------
prefill_feature_flags() {
    local data_dir="$1"
    mkdir -p "$data_dir"
    cat > "${data_dir}/feature_flags" <<'EOF'
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
}

start_node() {
    local node="$1" port="$2" data_dir="$3" conf_dir="$4" io_uring="$5"
    local mgmt_port=$(( port + 10000 ))
    log "Iniciando $node (AMQP :$port, management :$mgmt_port)..."

    mkdir -p "$data_dir" "$conf_dir"

    # Pre-populate feature flags to skip migrations that trigger a known
    # Horus compilation bug (tie_binding_to_dest_with_keep_while_cond).
    prefill_feature_flags "$data_dir"

    printf '%s\n' \
        "loopback_users = none" \
        "cluster_name = localhost" \
        "management.tcp.port = ${mgmt_port}" \
        "raft.data_dir = ${data_dir}/mnesia/${node}/quorum" \
        > "${conf_dir}/00-base.conf"

    if [[ "$io_uring" == "true" ]]; then
        printf 'message_store.io_uring = true\n' > "${conf_dir}/90-io-uring.conf"
        log "  io_uring habilitado."
    fi

    RABBITMQ_NODENAME="$node" \
    RABBITMQ_NODE_PORT="$port" \
    RABBITMQ_BASE="$data_dir" \
    RABBITMQ_PID_FILE="${data_dir}/${node}.pid" \
    RABBITMQ_LOG_BASE="${data_dir}/log" \
    RABBITMQ_MNESIA_BASE="${data_dir}/mnesia" \
    RABBITMQ_MNESIA_DIR="${data_dir}/mnesia/${node}" \
    RABBITMQ_QUORUM_DIR="${data_dir}/mnesia/${node}/quorum" \
    RABBITMQ_STREAM_DIR="${data_dir}/mnesia/${node}/stream" \
    RABBITMQ_FEATURE_FLAGS_FILE="${data_dir}/feature_flags" \
    RABBITMQ_PLUGINS_DIR="$BENCH_ROOT/plugins" \
    RABBITMQ_PLUGINS_EXPAND_DIR="${data_dir}/plugins" \
    RABBITMQ_ENABLED_PLUGINS_FILE="${data_dir}/enabled_plugins" \
    RABBITMQ_ENABLED_PLUGINS="rabbitmq_management" \
    RABBITMQ_SERVER_START_ARGS="" \
    RABBITMQ_CONFIG_FILES="$conf_dir" \
    ERL_LIBS="$BENCH_ROOT/plugins" \
        "$RABBITMQ_SERVER" -detached \
        >> "$RESULT_DIR/broker-${node}.log" 2>&1

    log "  Aguardando startup (máx 90s)..."
    local deadline=$(( $(date +%s) + 90 ))
    until "$RABBITMQCTL" -n "$node" await_startup > /dev/null 2>&1; do
        (( $(date +%s) > deadline )) && die "Broker $node não ficou pronto em 90s."
        sleep 2
    done
    log "  $node pronto."
}

stop_node() {
    local node="$1"
    log "Parando $node..."
    "$RABBITMQCTL" -n "$node" stop > /dev/null 2>&1 || true
    sleep 2
}

stop_all_nodes() {
    stop_node "$NODE_BASELINE" 2>/dev/null || true
    stop_node "$NODE_IOURING"  2>/dev/null || true
}

##------------------------------------------------------------------------
delete_queue_on() {
    local mgmt_port="$1"
    curl -s -u guest:guest \
        -X DELETE "http://localhost:${mgmt_port}/api/queues/%2F/${QUEUE_NAME}" \
        > /dev/null 2>&1 || true
}

run_once_on() {
    local uri="$1" mgmt_port="$2" run_num="$3"
    local tmp_out="/tmp/bench-run-${run_num}.txt"

    java -jar "$PERF_TEST_JAR" \
        --uri "$uri" \
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
        printf >&2 '[bench]   Run %s: parse falhou, descartando.\n' "$run_num"
        cat "$tmp_out" >> "$RESULT_DIR/parse-errors.log"
        return 1
    fi

    printf >&2 '[bench]   run %-4s  %s msg/s  p99=%s µs\n' \
        "$run_num" "$throughput" "$p99"
    printf '%s,%s\n' "$throughput" "$p99"

    delete_queue_on "$mgmt_port"
    sleep 1
}

run_warmup_on() {
    local uri="$1" mgmt_port="$2"
    local tmp_warmup="/tmp/bench-warmup-${mgmt_port}.txt"
    java -jar "$PERF_TEST_JAR" \
        --uri "$uri" \
        --queue "$QUEUE_NAME" \
        --queue-args "x-max-length=2000000" \
        --producers "$PRODUCERS" --consumers 0 \
        --flag persistent --size "$MSG_SIZE" \
        --confirm 200 --time "$DURATION" \
        > "$tmp_warmup" 2>&1 || true
    # Do not delete the queue here: accumulated messages remain on disk
    # so the next run_once_on() measures writes to an already-populated store.
    sleep 1
}

run_pair() {
    local pair_num="$1"
    bar
    log "Par $pair_num/$RUNS"
    bar

    local line attempt

    # Baseline — broker já está warm, store tem dados do par anterior
    attempt=0
    while (( attempt < 3 )); do
        attempt=$(( attempt + 1 ))
        if line=$(run_once_on \
                "amqp://guest:guest@localhost:${PORT_BASELINE}" \
                "$(( PORT_BASELINE + 10000 ))" \
                "b${pair_num}"); then break; fi
        line=""
    done
    [[ -n "$line" ]] || die "Baseline run $pair_num falhou após 3 tentativas."
    echo "$line" >> "$BASELINE_CSV"

    # io_uring — idem
    attempt=0
    while (( attempt < 3 )); do
        attempt=$(( attempt + 1 ))
        if line=$(run_once_on \
                "amqp://guest:guest@localhost:${PORT_IOURING}" \
                "$(( PORT_IOURING + 10000 ))" \
                "i${pair_num}"); then break; fi
        line=""
    done
    [[ -n "$line" ]] || die "io_uring run $pair_num falhou após 3 tentativas."
    echo "$line" >> "$IOURING_CSV"
}

##------------------------------------------------------------------------
main() {
    [[ -f "$PERF_TEST_JAR" ]]   || die "perf-test.jar não encontrado. Rode bench_broker.sh primeiro."
    [[ -x "$RABBITMQCTL" ]]     || die "rabbitmqctl não encontrado."
    [[ -x "$RABBITMQ_SERVER" ]] || die "sbin/rabbitmq-server não encontrado."

    local est_min=$(( (WARMUP_PAIRS + RUNS) * 2 * DURATION / 60 + 2 ))
    log "Benchmark dual-broker: ${RUNS} pares × 2 condições × ${DURATION}s | ${PRODUCERS} prod | ${MSG_SIZE}B"
    log "Warmup: ${WARMUP_PAIRS} pares descartados | Total estimado: ~${est_min} minutos"
    log "Design: dois brokers simultâneos, sem restart entre pares"

    rm -rf "$TEST_TMPDIR"
    mkdir -p "$TEST_TMPDIR"

    start_node "$NODE_BASELINE" "$PORT_BASELINE" "$DIR_BASELINE" "$CONF_BASELINE" "false"
    start_node "$NODE_IOURING"  "$PORT_IOURING"  "$DIR_IOURING"  "$CONF_IOURING"  "true"

    bar
    log "Warmup: ${WARMUP_PAIRS} pares (descartados) para aquecer os message stores..."
    bar
    for i in $(seq 1 "$WARMUP_PAIRS"); do
        log "  Warmup par $i/${WARMUP_PAIRS}..."
        run_warmup_on "amqp://guest:guest@localhost:${PORT_BASELINE}" "$(( PORT_BASELINE + 10000 ))"
        run_warmup_on "amqp://guest:guest@localhost:${PORT_IOURING}"  "$(( PORT_IOURING  + 10000 ))"
    done

    > "$BASELINE_CSV"
    > "$IOURING_CSV"

    for i in $(seq 1 "$RUNS"); do
        run_pair "$i"
    done

    stop_all_nodes

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
