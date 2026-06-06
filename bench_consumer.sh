#!/usr/bin/env bash
##
## bench_consumer.sh — Mede throughput de consumo (preadv vs pread sequencial).
##
## Cada run:
##   1. Pré-enche a fila com FILL_DURATION segundos de writes (sem consumers).
##      Isso garante que a maioria das mensagens está no disco, fora do cache.
##   2. Mede o throughput de consumo por CONSUME_DURATION segundos (sem producers).
##
## Uso:
##   bash bench_consumer.sh               # 20 runs, 15s fill, 10s consume, 4 consumers
##   bash bench_consumer.sh 15 10 8 2048  # 15 runs, 10s fill, 8 consumers, 2 KB
##
set -euo pipefail

BENCH_ROOT="$(cd "$(dirname "$0")" && pwd)"
RUNS="${1:-20}"
FILL_DURATION="${2:-25}"    # segundos de write para pre-encher a fila (caps em 2M por max-length)
CONSUME_DURATION="${3:-10}" # segundos de medição de consumo
CONSUMERS="${4:-4}"
MSG_SIZE="${5:-1024}"

PERF_TEST_JAR="$BENCH_ROOT/perf-test.jar"
RABBITMQCTL="$BENCH_ROOT/escript/rabbitmqctl"
export ERL_LIBS="$BENCH_ROOT/plugins"

HOSTNAME_SHORT=$(hostname -s)
NODE_NAME="rabbit@${HOSTNAME_SHORT}"
TEST_TMPDIR="/tmp/rabbitmq-test-instances"
NODE_DIR="${TEST_TMPDIR}/${NODE_NAME}"
CONF_DIR="${TEST_TMPDIR}/conf.d"
IO_URING_CONF="${CONF_DIR}/90-io-uring.conf"

RESULT_DIR="$BENCH_ROOT/bench-results"
BASELINE_CSV="$RESULT_DIR/consumer_baseline.csv"
IOURING_CSV="$RESULT_DIR/consumer_iouring.csv"
mkdir -p "$RESULT_DIR"

QUEUE_NAME="bench-consumer"

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

start_broker() {
    local label="$1"
    local enable_io_uring="${2:-false}"

    log "Iniciando broker ($label)..."
    make -C "$BENCH_ROOT" virgin-test-tmpdir > /dev/null 2>&1
    generate_conf

    if [[ "$enable_io_uring" == "true" ]]; then
        printf 'message_store.io_uring = true\n' > "$IO_URING_CONF"
        log "io_uring habilitado."
    fi

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

## Pré-enche a fila com mensagens persistentes sem consumers.
## Após o fill, a maioria das mensagens está no disco (fora do write_buffer/cache),
## garantindo que os consumers vão exercitar o caminho de leitura do disco.
fill_queue() {
    java -jar "$PERF_TEST_JAR" \
        --uri "amqp://guest:guest@localhost:5672" \
        --queue "$QUEUE_NAME" \
        --queue-args "x-max-length=2000000" \
        --producers 4 --consumers 0 \
        --flag persistent \
        --size "$MSG_SIZE" \
        --confirm 200 \
        --time "$FILL_DURATION" \
        > /tmp/bench-consumer-fill.txt 2>&1 || true
}

run_once() {
    local run_num="$1"
    local tmp_fill="/tmp/bench-consumer-fill-${run_num}.txt"
    local tmp_out="/tmp/bench-consumer-run-${run_num}.txt"

    ## Pré-enche a fila.
    java -jar "$PERF_TEST_JAR" \
        --uri "amqp://guest:guest@localhost:5672" \
        --queue "$QUEUE_NAME" \
        --queue-args "x-max-length=2000000" \
        --producers 4 --consumers 0 \
        --flag persistent \
        --size "$MSG_SIZE" \
        --confirm 200 \
        --time "$FILL_DURATION" \
        > "$tmp_fill" 2>&1 || true

    ## Mede throughput de consumo (sem producers — leituras puras do disco).
    ## --predeclared: não redeclara a fila (evita mismatch de args com a fila já existente).
    java -jar "$PERF_TEST_JAR" \
        --uri "amqp://guest:guest@localhost:5672" \
        --queue "$QUEUE_NAME" \
        --producers 0 \
        --consumers "$CONSUMERS" \
        --predeclared \
        --time "$CONSUME_DURATION" \
        > "$tmp_out" 2>&1

    local rate
    rate=$(grep "receiving rate avg" "$tmp_out" \
        | grep -oP '[\d,]+(?= msg/s)' | tr -d ',' | tail -1 || echo "")

    if [[ -z "$rate" ]]; then
        printf >&2 '[bench]   Run %d: parse falhou, descartando.\n' "$run_num"
        cat "$tmp_out" >> "$RESULT_DIR/consumer-parse-errors.log"
        return 1
    fi

    printf >&2 '[bench]   run %-3d  %s msg/s\n' "$run_num" "$rate"
    printf '%s\n' "$rate"

    delete_queue
    sleep 1
}

collect_runs() {
    local label="$1"
    local csv_file="$2"

    bar
    log "Coletando $RUNS runs — $label"
    bar

    ## Warmup (descartado).
    log "Warmup run (descartado)..."
    fill_queue
    java -jar "$PERF_TEST_JAR" \
        --uri "amqp://guest:guest@localhost:5672" \
        --queue "$QUEUE_NAME" \
        --producers 0 --consumers "$CONSUMERS" \
        --predeclared \
        --time "$CONSUME_DURATION" \
        > /tmp/bench-consumer-warmup.txt 2>&1 || true
    delete_queue
    sleep 2

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

    local per_run=$(( FILL_DURATION + CONSUME_DURATION ))
    log "Benchmark consumer: ${RUNS} runs × (${FILL_DURATION}s fill + ${CONSUME_DURATION}s consume)"
    log "${CONSUMERS} consumers | ${MSG_SIZE}B | Total estimado: ~$(( (RUNS + 1) * per_run * 2 / 60 )) minutos"

    bar
    log "=== FASE 1: Baseline (prim_file) ==="
    bar
    start_broker "consumer-baseline" "false"
    collect_runs "baseline" "$BASELINE_CSV"
    stop_broker

    bar
    log "=== FASE 2: io_uring ==="
    bar
    start_broker "consumer-iouring" "true"
    collect_runs "io_uring" "$IOURING_CSV"
    stop_broker

    bar
    log "Dados coletados. Rodando análise estatística..."
    BENCH_BASELINE_CSV="$BASELINE_CSV" \
    BENCH_IOURING_CSV="$IOURING_CSV" \
    BENCH_RUNS="$RUNS" \
    BENCH_DURATION="$CONSUME_DURATION" \
    BENCH_CONSUMERS="$CONSUMERS" \
    BENCH_SIZE="$MSG_SIZE" \
    BENCH_CONSUMER_MODE="1" \
        jupyter nbconvert --to notebook --execute \
            --output "$RESULT_DIR/bench_analyze_consumer.ipynb" \
            "$BENCH_ROOT/bench_analyze.ipynb" 2>&1 | grep -v "^$"
    log "Notebook salvo em: $RESULT_DIR/bench_analyze_consumer.ipynb"
}

main "$@"
