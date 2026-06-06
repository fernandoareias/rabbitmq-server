#!/usr/bin/env bash
##
## bench_broker.sh — benchmark io_uring vs baseline no broker RabbitMQ de dev.
##
## Pré-requisito: make dist  (feito uma vez)
##
## Uso:
##   bash bench_broker.sh                  # 30s, 4 producers, 1 KB
##   bash bench_broker.sh 60 8 4096        # 60s, 8 producers, 4 KB
##
set -euo pipefail

BENCH_ROOT="$(cd "$(dirname "$0")" && pwd)"
DURATION="${1:-30}"
PRODUCERS="${2:-4}"
MSG_SIZE="${3:-1024}"

PERF_TEST_JAR="$BENCH_ROOT/perf-test.jar"
PERF_TEST_URL="https://github.com/rabbitmq/rabbitmq-perf-test/releases/download/v2.22.0/perf-test-2.22.0.jar"

RABBITMQCTL="$BENCH_ROOT/escript/rabbitmqctl"
export ERL_LIBS="$BENCH_ROOT/plugins"

# Estas variáveis espelham o que o Makefile usa por padrão
HOSTNAME_SHORT=$(hostname -s)
NODE_NAME="rabbit@${HOSTNAME_SHORT}"
TEST_TMPDIR="/tmp/rabbitmq-test-instances"
NODE_DIR="${TEST_TMPDIR}/${NODE_NAME}"
CONF_DIR="${TEST_TMPDIR}/conf.d"
PID_FILE="${NODE_DIR}/${NODE_NAME}.pid"
IO_URING_CONF="${CONF_DIR}/90-io-uring.conf"

RESULT_DIR="$BENCH_ROOT/bench-results"
mkdir -p "$RESULT_DIR"

##------------------------------------------------------------------------
log() { echo "[bench] $*"; }
die() { echo "[bench] ERROR: $*" >&2; exit 1; }

##------------------------------------------------------------------------
check_prerequisites() {
    [[ -d "$BENCH_ROOT/plugins" ]] || \
        die "plugins/ não encontrado. Rode 'make dist' primeiro."
    [[ -x "$RABBITMQCTL" ]] || \
        die "rabbitmqctl não encontrado em $RABBITMQCTL"
    command -v java >/dev/null || die "java não encontrado."
}

download_perf_test() {
    if [[ ! -f "$PERF_TEST_JAR" ]]; then
        log "Baixando rabbitmq-perf-test..."
        wget -q --show-progress "$PERF_TEST_URL" -O "$PERF_TEST_JAR"
    fi
    log "perf-test: $PERF_TEST_JAR"
}

##------------------------------------------------------------------------
# Gera os arquivos de config no conf.d (replica write_config_files_broker
# do rabbitmq-run.mk para o caso sem porta customizada).
generate_conf() {
    mkdir -p "$CONF_DIR"
    printf '%s\n' \
        "loopback_users = none" \
        "cluster_name = localhost" \
        "raft.data_dir = ${NODE_DIR}/mnesia/${NODE_NAME}/quorum" \
        > "${CONF_DIR}/00-base.conf"
    # Sem porta customizada a management não escreve 10-management.conf,
    # mas vamos deixar o plugin habilitado via RABBITMQ_ENABLED_PLUGINS.
}

##------------------------------------------------------------------------
start_broker() {
    local label="$1"
    local enable_io_uring="${2:-false}"

    log "Preparando broker ($label)..."
    make -C "$BENCH_ROOT" virgin-test-tmpdir > /dev/null 2>&1

    generate_conf

    if [[ "$enable_io_uring" == "true" ]]; then
        printf 'message_store.io_uring = true\n' > "$IO_URING_CONF"
        log "io_uring habilitado."
    fi

    log "Iniciando broker em modo detached..."
    # run-background-broker usa -detached (fork Erlang) e retorna imediatamente.
    # Passamos RABBITMQ_CONFIG_FILES para apontar para o nosso conf.d.
    make -C "$BENCH_ROOT" run-background-broker \
        RABBITMQ_CONFIG_FILES="$CONF_DIR" \
        >> "$RESULT_DIR/broker-${label}.log" 2>&1

    log "Aguardando broker ficar pronto (máx. 90s)..."
    local deadline=$(( $(date +%s) + 90 ))
    until "$RABBITMQCTL" -n "$NODE_NAME" await_startup > /dev/null 2>&1; do
        if (( $(date +%s) > deadline )); then
            log "Últimas linhas do log:"
            tail -20 "$RESULT_DIR/broker-${label}.log" >&2
            die "Broker não ficou pronto em 90s."
        fi
        sleep 2
    done
    log "Broker $NODE_NAME pronto."
}

stop_broker() {
    log "Parando broker..."
    make -C "$BENCH_ROOT" stop-node > /dev/null 2>&1 || true
    local deadline=$(( $(date +%s) + 30 ))
    while [[ -f "$PID_FILE" ]]; do
        (( $(date +%s) > deadline )) && break
        sleep 1
    done
    rm -f "$IO_URING_CONF"
    sleep 2
}

##------------------------------------------------------------------------
run_perf_test() {
    local label="$1"
    local out="$RESULT_DIR/perf-${label}.txt"

    log "Rodando perf-test por ${DURATION}s (${PRODUCERS} producers, ${MSG_SIZE}B, persistent, confirm=200)..."
    java -jar "$PERF_TEST_JAR" \
        --uri "amqp://guest:guest@localhost:5672" \
        --producers  "$PRODUCERS" \
        --consumers  0 \
        --flag       persistent \
        --size       "$MSG_SIZE" \
        --confirm    200 \
        --confirm-timeout 60 \
        --time       "$DURATION" \
        2>&1 | tee "$out"

    log "Resultado salvo em $out"
}

##------------------------------------------------------------------------
extract_metrics() {
    local file="$1"
    local summary sent p99
    summary=$(grep -i "summary" "$file" | tail -1)
    sent=$(echo "$summary" | grep -oP 'sent: \K[\d,]+' | tr -d ',' || echo "N/A")
    p99=$(echo  "$summary" | grep -oP '99th \K[\d.]+' || echo "N/A")
    echo "${sent} msg/s | p99: ${p99} ms"
}

##------------------------------------------------------------------------
print_comparison() {
    local b="$1" i="$2"
    local b_rate i_rate b_p99 i_p99

    b_rate=$(echo "$b" | awk '{print $1}')
    i_rate=$(echo "$i" | awk '{print $1}')
    b_p99=$(echo  "$b" | awk '{print $NF}' | tr -d 'ms')
    i_p99=$(echo  "$i" | awk '{print $NF}' | tr -d 'ms')

    echo ""
    echo "═══════════════════════════════════════════════════════"
    printf "  Benchmark: %s prod | %sB | persistent | %ss\n" \
           "$PRODUCERS" "$MSG_SIZE" "$DURATION"
    echo "═══════════════════════════════════════════════════════"
    printf "  %-24s %12s   %12s\n" "Cenário" "Throughput" "Confirm p99"
    echo "───────────────────────────────────────────────────────"
    printf "  %-24s %12s   %12s\n" "Baseline (prim_file)" "${b_rate} msg/s" "${b_p99} ms"
    printf "  %-24s %12s   %12s\n" "io_uring"             "${i_rate} msg/s" "${i_p99} ms"
    echo "═══════════════════════════════════════════════════════"

    if [[ "$b_rate" != "N/A" && "$i_rate" != "N/A" && "$b_rate" -gt 0 ]] 2>/dev/null; then
        local speedup
        speedup=$(awk "BEGIN {printf \"%.2f\", $i_rate / $b_rate}")
        echo "  Speedup throughput: ${speedup}x"
    fi
    echo "  Logs em: $RESULT_DIR/"
    echo ""
}

##------------------------------------------------------------------------
main() {
    check_prerequisites
    download_perf_test

    log "=== Rodada 1: Baseline (io_uring desabilitado) ==="
    start_broker "baseline" "false"
    run_perf_test "baseline"
    BASELINE=$(extract_metrics "$RESULT_DIR/perf-baseline.txt")
    stop_broker

    log "=== Rodada 2: io_uring habilitado ==="
    start_broker "iouring" "true"
    run_perf_test "iouring"
    IOURING=$(extract_metrics "$RESULT_DIR/perf-iouring.txt")
    stop_broker

    print_comparison "$BASELINE" "$IOURING"
}

main "$@"
