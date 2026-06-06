# Guia de Replicação — benchmark_timeseries.png

Passo a passo para reproduzir os gráficos de throughput e latência p99
do benchmark io_uring vs baseline a partir do zero.

---

## Pré-requisitos

| Ferramenta | Versão mínima | Verificação |
|---|---|---|
| Erlang/OTP | 27.x | `erl -version` |
| GNU Make 4 | 4.x | `gmake --version` |
| Java (JRE) | 11+ | `java -version` |
| Python | 3.10+ | `python3 --version` |

### Instalar dependências Python

```bash
pip install jupyter nbconvert numpy scipy matplotlib
```

---

## Passo 1 — Compilar o broker

```bash
gmake dist
```

Isso preenche os diretórios `plugins/` e `escript/` com os binários do broker.
Só precisa ser feito uma vez (ou após mudanças no código).

---

## Passo 2 — Obter o perf-test.jar

O script baixa o JAR automaticamente se ele não existir.
Para baixar manualmente:

```bash
wget https://github.com/rabbitmq/rabbitmq-perf-test/releases/download/v2.22.0/perf-test-2.22.0.jar \
     -O perf-test.jar
```

---

## Passo 3 — Coletar os dados

```bash
bash bench_statistical.sh
```

Isso executa **30 runs de 10 segundos cada**, com 4 produtores e mensagens de
1 KB, em dois cenários isolados:

- **Fase 1 — Baseline:** broker sem io_uring (caminho padrão `prim_file`)
- **Fase 2 — io_uring:** broker com `message_store.io_uring = true`

Tempo estimado: ~12 minutos no total.

### Parâmetros opcionais

```
bash bench_statistical.sh <RUNS> <DURAÇÃO_S> <PRODUTORES> <TAMANHO_B>
```

Exemplo equivalente ao benchmark do artigo:

```bash
bash bench_statistical.sh 30 10 4 1024
```

### O que acontece em cada fase

```
make virgin-test-tmpdir          # limpa /tmp/rabbitmq-test-instances
make run-background-broker       # sobe o broker em background
rabbitmqctl await_startup        # aguarda pronto (máx. 90 s)
  └─ 1× warmup run (descartado)
  └─ 30× runs de medição
       ├─ java perf-test --time 10 --producers 4 --consumers 0 \
       │       --flag persistent --size 1024 --confirm 200
       └─ DELETE fila entre runs (via Management HTTP API)
make stop-node                   # para o broker
```

### Configuração que diferencia os cenários

O único arquivo que muda é `conf.d/90-io-uring.conf`, presente apenas na Fase 2:

```ini
message_store.io_uring = true
```

Todos os outros parâmetros — porta, usuário, plugin list, tamanho de mensagem,
número de produtores — são idênticos nas duas fases.

### Saídas geradas em `bench-results/`

| Arquivo | Conteúdo |
|---|---|
| `baseline.csv` | throughput (msg/s) e latência p99 (µs) por run — baseline |
| `iouring.csv` | throughput (msg/s) e latência p99 (µs) por run — io_uring |
| `broker-stat-baseline.log` | log do broker durante a Fase 1 |
| `broker-stat-iouring.log` | log do broker durante a Fase 2 |

Formato do CSV — uma linha por run:

```
<throughput_msg_s>,<confirm_p99_us>
97432,13800
103215,12500
```

---

## Passo 4 — Gerar os gráficos

O script executa o notebook automaticamente ao final da coleta.
Para regenerar os gráficos manualmente a partir dos CSVs existentes:

```bash
BENCH_BASELINE_CSV=bench-results/baseline.csv \
BENCH_IOURING_CSV=bench-results/iouring.csv \
BENCH_RUNS=30 \
BENCH_DURATION=10 \
BENCH_PRODUCERS=4 \
BENCH_SIZE=1024 \
BENCH_CONSUMER_MODE=0 \
jupyter nbconvert --to notebook --execute \
    --output bench-results/bench_analyze.ipynb \
    bench_analyze.ipynb
```

Para explorar interativamente:

```bash
BENCH_BASELINE_CSV=bench-results/baseline.csv \
BENCH_IOURING_CSV=bench-results/iouring.csv \
jupyter notebook bench_analyze.ipynb
```

### Gráficos produzidos

| Arquivo | Descrição |
|---|---|
| `benchmark_timeseries.png` | Throughput e p99 de cada run em sequência temporal, com a média de cada fase como linha tracejada |
| `benchmark_boxplots.png` | Boxplots com jitter e anotação de significância estatística (Welch t-test) |
| `benchmark_kde.png` | Estimativa da função de densidade de probabilidade das duas distribuições |

---

## Reprodutibilidade

- O estado do broker é reiniciado entre as fases (`virgin-test-tmpdir`)
- A fila é deletada entre runs para evitar acúmulo de mensagens
- O warmup descarta o primeiro run para estabilizar JVM e io_uring rings
- O bootstrap usa semente fixa (`42`) — o notebook produz resultados idênticos
  ao ser reexecutado sobre os mesmos CSVs

Para comparar resultados entre máquinas diferentes, compartilhe os arquivos
`baseline.csv` e `iouring.csv` e execute o notebook localmente.
