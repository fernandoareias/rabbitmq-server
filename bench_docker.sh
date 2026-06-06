#!/usr/bin/env bash
##
## bench_docker.sh — benchmark io_uring vs baseline using Docker containers.
##
## Runs the RabbitMQ broker inside an OTP-27 container, bypassing the
## OTP-29/horus incompatibility that crashes the broker on a virgin start
## when using the host's Erlang runtime.
##
## The BEAM files in plugins/ must be compiled with OTP 27.  Because the host
## may run a different OTP version, the first step recompiles everything inside
## a temporary OTP-27 container via a volume mount (results written back to the
## working tree).  This only needs to happen once; subsequent runs skip it if
## the image already exists.
##
## Usage:
##   bash bench_docker.sh build              # compile (OTP 27) + build image
##   bash bench_docker.sh                    # 30 runs × 10s, 4 producers, 1 KB
##   bash bench_docker.sh 20 15 8 2048       # RUNS DURATION PRODUCERS MSG_SIZE
##
set -euo pipefail

BENCH_ROOT="$(cd "$(dirname "$0")" && pwd)"

# Handle the optional "build" subcommand before positional args are parsed.
if [[ "${1:-}" == "build" ]]; then
    _BUILD_ONLY=true
    shift
else
    _BUILD_ONLY=false
fi

RUNS="${1:-30}"
DURATION="${2:-10}"
PRODUCERS="${3:-4}"
MSG_SIZE="${4:-1024}"

IMAGE="rabbitmq-bench"
CONTAINER="rabbitmq-bench-run"
IOURING_CONF="/tmp/bench-io-uring.conf"

PERF_TEST_JAR="$BENCH_ROOT/perf-test.jar"
PERF_TEST_URL="https://github.com/rabbitmq/rabbitmq-perf-test/releases/download/v2.22.0/perf-test-2.22.0.jar"

RESULT_DIR="$BENCH_ROOT/bench-results"
BASELINE_CSV="$RESULT_DIR/baseline.csv"
IOURING_CSV="$RESULT_DIR/iouring.csv"
mkdir -p "$RESULT_DIR"

QUEUE_NAME="bench-stat"
AMQP_URI="amqp://guest:guest@localhost:5672"
MGMT_URL="http://localhost:15672"

##------------------------------------------------------------------------
log()  { printf '[bench] %s\n' "$*"; }
die()  { printf '[bench] ERROR: %s\n' "$*" >&2; stop_container; exit 1; }
bar()  { printf '[bench] ─────────────────────────────────────────────\n'; }

##------------------------------------------------------------------------
# Recompile all BEAM files using OTP 27 inside a temporary container.
# The working tree is mounted as a volume so compiled output is written back
# to the host and picked up by the subsequent docker build step.
compile_otp27() {
    bar
    log "Recompilando artefatos com OTP 27 via container temporário..."
    log "Isso pode levar vários minutos na primeira vez."
    bar
    # Mount at the exact same absolute path as on the host so that the
    # pre-existing .d dependency files (which contain host-absolute paths)
    # resolve correctly inside the container.
    docker run --rm \
        -v "${BENCH_ROOT}:${BENCH_ROOT}" \
        -w "${BENCH_ROOT}" \
        erlang:27-slim \
        bash -c "
            set -e
            apt-get update -qq >/dev/null 2>&1
            apt-get install -y -qq --no-install-recommends \
                make git liburing-dev gcc libc-dev unzip curl ca-certificates p7zip-full >/dev/null 2>&1

            echo '[compile] Instalando Elixir 1.18 para OTP 27...'
            curl -fsSL https://github.com/elixir-lang/elixir/releases/download/v1.18.4/elixir-otp-27.zip \
                -o /tmp/elixir.zip 2>/dev/null
            unzip -q /tmp/elixir.zip -d /usr/local
            export PATH=/usr/local/bin:\$PATH
            mix local.hex --force --if-missing >/dev/null 2>&1

            echo '[compile] Removendo ebin/ e escripts OTP incompatíveis...'
            find deps/ -type d -name 'ebin' -exec rm -rf {} + 2>/dev/null || true
            find plugins/ -type d -name 'ebin' -exec rm -rf {} + 2>/dev/null || true
            # Remove the rabbitmqctl escript so Mix recompiles it with OTP 27.
            rm -f escript/rabbitmqctl deps/rabbitmq_cli/escript/rabbitmqctl 2>/dev/null || true

            echo '[compile] Compilando com OTP 27...'
            HOME=/tmp PATH=/usr/local/bin:\$PATH make dist
            echo '[compile] Concluído.'
        "
    log "Compilação OTP 27 concluída."
}

build_image() {
    compile_otp27
    log "Construindo imagem Docker '$IMAGE'..."
    docker build -f "${BENCH_ROOT}/Dockerfile.bench" -t "$IMAGE" "$BENCH_ROOT"
    log "Imagem '$IMAGE' pronta."
}

check_prerequisites() {
    command -v docker >/dev/null || die "docker não encontrado."
    command -v java   >/dev/null || die "java não encontrado."
    if ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
        log "Imagem '$IMAGE' não encontrada. Executando build automático..."
        build_image
    fi
}

download_perf_test() {
    if [[ ! -f "$PERF_TEST_JAR" ]]; then
        log "Baixando rabbitmq-perf-test..."
        curl -fsSL "$PERF_TEST_URL" -o "$PERF_TEST_JAR"
    fi
}

##------------------------------------------------------------------------
start_container() {
    local label="$1"
    local enable_io_uring="${2:-false}"

    # Ensure no stale container is running.
    docker rm -f "$CONTAINER" > /dev/null 2>&1 || true

    log "Iniciando container ($label)..."

    local extra_args=()
    if [[ "$enable_io_uring" == "true" ]]; then
        printf 'message_store.io_uring = true\n' > "$IOURING_CONF"
        extra_args+=(-v "${IOURING_CONF}:/etc/rabbitmq/conf.d/90-io-uring.conf:ro")
        log "io_uring habilitado."
    fi

    # seccomp=unconfined is required on both containers so that io_uring
    # syscalls (blocked by the Docker default seccomp profile) are permitted.
    # Both phases use the same security context for a fair comparison.
    docker run -d \
        --name  "$CONTAINER" \
        --network host \
        --security-opt seccomp=unconfined \
        "${extra_args[@]}" \
        "$IMAGE" \
        >> "$RESULT_DIR/broker-${label}.log" 2>&1

    log "Aguardando startup (máx 90s)..."
    local deadline=$(( $(date +%s) + 90 ))
    until docker exec "$CONTAINER" \
            /opt/rabbitmq/escript/rabbitmqctl await_startup > /dev/null 2>&1; do
        if (( $(date +%s) > deadline )); then
            log "Últimas linhas do log do container:"
            docker logs --tail 30 "$CONTAINER" >&2
            die "Broker não ficou pronto em 90s."
        fi
        sleep 2
    done
    log "Broker pronto."
}

stop_container() {
    log "Parando container..."
    docker stop "$CONTAINER" > /dev/null 2>&1 || true
    docker rm   "$CONTAINER" > /dev/null 2>&1 || true
    rm -f "$IOURING_CONF"
}

##------------------------------------------------------------------------
delete_queue() {
    curl -s -u guest:guest \
        -X DELETE "${MGMT_URL}/api/queues/%2F/${QUEUE_NAME}" \
        > /dev/null 2>&1 || true
}

run_once() {
    local run_num="$1"
    local tmp_out="/tmp/bench-docker-run-${run_num}.txt"

    java -jar "$PERF_TEST_JAR" \
        --uri            "$AMQP_URI" \
        --queue          "$QUEUE_NAME" \
        --queue-args     "x-max-length=2000000" \
        --producers      "$PRODUCERS" \
        --consumers      0 \
        --flag           persistent \
        --size           "$MSG_SIZE" \
        --confirm        200 \
        --confirm-timeout 60 \
        --time           "$DURATION" \
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

    # Warmup (descartado).
    log "Warmup run (descartado)..."
    java -jar "$PERF_TEST_JAR" \
        --uri "$AMQP_URI" \
        --queue "$QUEUE_NAME" \
        --queue-args "x-max-length=2000000" \
        --producers "$PRODUCERS" --consumers 0 \
        --flag persistent --size "$MSG_SIZE" \
        --confirm 200 --time "$DURATION" \
        > /tmp/bench-docker-warmup.txt 2>&1 || true
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
    command -v docker >/dev/null || die "docker não encontrado."

    if [[ "$_BUILD_ONLY" == "true" ]]; then
        build_image
        exit 0
    fi

    check_prerequisites
    download_perf_test

    log "Benchmark Docker: ${RUNS} runs × ${DURATION}s | ${PRODUCERS} prod | ${MSG_SIZE}B"
    log "Imagem: $IMAGE | Total estimado: ~$(( (RUNS + 1) * DURATION * 2 / 60 )) minutos"

    bar
    log "=== FASE 1: Baseline (prim_file) ==="
    bar
    start_container "stat-baseline" "false"
    collect_runs "baseline" "$BASELINE_CSV"
    stop_container

    bar
    log "=== FASE 2: io_uring ==="
    bar
    start_container "stat-iouring" "true"
    collect_runs "io_uring" "$IOURING_CSV"
    stop_container

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
    log "PDFs gerados em:   $RESULT_DIR/"
}

main "$@"
