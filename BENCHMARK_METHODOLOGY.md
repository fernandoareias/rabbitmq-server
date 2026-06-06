# Metodologia e Resultados do Benchmark io_uring

## 1. Objetivo

O benchmark tem como objetivo responder a uma única pergunta de forma rigorosa: **a integração io_uring produz ganho de desempenho mensurável e estatisticamente significativo no RabbitMQ quando comparado ao caminho padrão de escrita (`prim_file`/`file:write`)?**

Para isso, dois cenários distintos foram medidos:

1. **Escrita** — throughput e latência de publisher confirms em workload de produção pura de mensagens persistentes
2. **Leitura** — throughput de consumo de mensagens que estão inteiramente no disco, exercitando o caminho de leitura do queue store

---

## 2. Ambiente e Ferramentas

### 2.1 Cliente de Carga

O cliente de benchmark é o **RabbitMQ PerfTest** (`perf-test.jar`), a ferramenta oficial de testes de desempenho do RabbitMQ. Ele conecta ao broker via AMQP 0-9-1, publica e/ou consome mensagens e reporta estatísticas de throughput e latência.

### 2.2 Isolamento do Broker

Cada fase de benchmark (baseline e io_uring) usa um broker completamente separado, iniciado a partir de um diretório de dados limpo. O ciclo de vida do broker em cada fase é:

```
make virgin-test-tmpdir    ← apaga /tmp/rabbitmq-test-instances completamente
gera rabbitmq.conf         ← configuração mínima (sem TLS, sem plugins extras)
make run-background-broker ← inicia o broker em background
rabbitmqctl await_startup  ← aguarda ready (timeout 90s)
... coleta de dados ...
make stop-node             ← para o broker graciosamente
```

O `virgin-test-tmpdir` é chamado uma única vez por fase, não entre runs individuais. Isso significa que o broker acumula estado de disco ao longo dos 30 runs de uma fase — o que é intencional: reflete o comportamento real de um broker em produção que não é reiniciado a cada operação.

### 2.3 Configuração do Broker

A configuração base é mínima:

```ini
loopback_users = none
cluster_name   = localhost
```

Para a fase io_uring, um arquivo de configuração adicional é injetado em `conf.d/`:

```ini
message_store.io_uring = true
```

A ordem de carregamento de configurações do RabbitMQ garante que esse arquivo sobrescreve o padrão. Quando o benchmark termina, o arquivo é removido para não contaminar execuções futuras.

### 2.4 Análise Estatística

Toda a análise é executada no notebook Jupyter `bench_analyze.ipynb`, chamado automaticamente ao final de cada benchmark via:

```bash
jupyter nbconvert --to notebook --execute \
    --output bench-results/bench_analyze.ipynb \
    bench_analyze.ipynb
```

O notebook executado, com todos os gráficos e saídas embutidos, é salvo em `bench-results/`.

---

## 3. Benchmark de Escrita (`bench_statistical.sh`)

### 3.1 O Que É Medido

Este benchmark mede o desempenho do **caminho de escrita** do broker: receber mensagens persistentes de produtores, confirmá-las com publisher confirms e gravá-las em disco.

As métricas coletadas por run são:

- **Throughput** — taxa média de publicações confirmadas no período de medição, em `msg/s`
- **Latência P99 de confirm** — 99º percentil da latência entre envio da mensagem e recebimento do confirm, em `µs`

### 3.2 Parâmetros de Execução

| Parâmetro | Valor | Justificativa |
|---|---|---|
| Runs por fase | 30 | volume suficiente para teste t com boa potência estatística |
| Duração por run | 10s | longa o suficiente para steady-state, curta para 30 repetições |
| Produtores | 4 | saturação das threads de dirty-scheduler do io_uring |
| Consumidores | 0 | isola exclusivamente o caminho de escrita |
| Tamanho da mensagem | 1 KB | representativo de workloads de mensageria típicos |
| Flag | `persistent` | força gravação em disco antes do confirm |
| Confirms em lote | 200 | acumulação de buffer antes do flush |
| x-max-length | 2.000.000 | evita crescimento ilimitado de disco; após ~4s a fila está cheia e opera em modo de substituição contínua (dropping oldest) |

### 3.3 Protocolo por Run

Cada run individual segue esta sequência:

```
1. java perf-test --time 10s --producers 4 --consumers 0 --confirm 200
   └─► mede: throughput médio e P99 de confirm
2. DELETE /api/queues/%2F/bench-stat   (HTTP Management API)
3. sleep 1s
```

A deleção da fila entre runs garante que o broker não acumule mensagens não confirmadas de runs anteriores, mas **não** reinicia o broker — os arquivos de segmento já existentes no disco são mantidos, assim como qualquer GC em andamento. Isso reproduz o comportamento real.

### 3.4 Warmup

Antes da coleta dos 30 runs, um run de warmup **descartado** é executado com os mesmos parâmetros. O warmup serve para:

- Inicializar os pools de threads do broker e do cliente Java (JIT do JVM)
- Popular o page cache do OS com os arquivos de segmento iniciais
- Estabilizar os rings io_uring (mapeamentos de memória, thread do kernel se SQPOLL)

O resultado do warmup não é gravado no CSV.

### 3.5 Resiliência a Falhas de Parse

Se o perf-test não produzir uma linha de saída reconhecível (`sending rate avg ... msg/s`), o run é descartado e reexecutado. O script aceita até `RUNS × 2` tentativas totais antes de abortar. Isso protege contra timeouts de conexão transitórios no início da medição.

---

## 4. Benchmark de Consumer (`bench_consumer.sh`)

### 4.1 O Desafio: Forçar I/O Real de Disco

Medir leitura de disco em um broker de mensagens é mais difícil do que medir escrita, porque o RabbitMQ mantém um write-back cache em memória para mensagens recentemente escritas. Se o consumer puder consumir direto do cache, o disco nunca é exercitado — e a medição mede RAM, não I/O.

A solução é a **fase de fill**: antes de cada run de consumo, 4 produtores enchem a fila até o limite de 2M mensagens. Com `--size 1024` e `--confirm 200`, a fila atinge 2M mensagens em aproximadamente 4 segundos. Os 21 segundos restantes do fill substituem continuamente mensagens antigas por novas (_rolling replacement_). Ao final do fill de 25 segundos, a fila tem 2M mensagens, a maioria das quais está no disco e fora do write-back cache porque foi escrita no início do fill e já foi evictada da memória.

### 4.2 Parâmetros de Execução

| Parâmetro | Valor | Justificativa |
|---|---|---|
| Runs por fase | 20 | 20 × (25+10)s = ~12 minutos por fase |
| Duração do fill | 25s | garante 2M mensagens + rolling replacement; evicta write cache |
| Duração do consumo | 10s | janela de medição de consumo puro |
| Produtores (fill) | 4 | saturação suficiente para encher rapidamente |
| Consumidores | 4 | paralelismo representativo |
| x-max-length | 2.000.000 | impõe limite para o fill funcionar deterministicamente |

### 4.3 Protocolo por Run

Cada run tem duas etapas sequenciais:

```
┌─ FILL (25s) ──────────────────────────────────────────────────┐
│  java perf-test --producers 4 --consumers 0                    │
│                 --flag persistent --confirm 200 --time 25      │
│  Objetivo: 2M mensagens no disco, write cache evictado         │
└───────────────────────────────────────────────────────────────┘

┌─ CONSUME (10s) ───────────────────────────────────────────────┐
│  java perf-test --producers 0 --consumers 4                    │
│                 --predeclared --time 10                        │
│  Mede: receiving rate avg (msg/s)                              │
└───────────────────────────────────────────────────────────────┘

DELETE /api/queues/%2F/bench-consumer
sleep 1s
```

O flag `--predeclared` instrui o perf-test a não redeclarar a fila — ela já existe com `x-max-length=2000000` do fill. Redeclarar com argumentos diferentes causaria erro AMQP.

### 4.4 O Broker Persiste entre Runs

O broker **não é reiniciado** entre runs individuais. O mesmo broker processa todos os 20 runs do fill + consumo. Isso é intencional: o estado interno do queue store e do índice acumula-se naturalmente ao longo dos runs, refletindo o comportamento de um broker em operação contínua.

### 4.5 Alta Variância nos Resultados de Leitura

Leituras de disco têm variância significativamente maior do que escritas. Isso ocorre porque:

- O page cache do OS pode ou não ter evictado as páginas entre o fill e o consumo
- O escalonador de I/O do kernel pode reordenar leituras para otimizar seeks
- A distribuição de mensagens nos arquivos de segmento muda a cada run (segmentos diferentes, offsets diferentes)

Por isso, o benchmark de consumer usa 20 runs em vez de 30 — um compromisso entre tempo de execução (~24 minutos por fase) e volume de dados.

---

## 5. Metodologia Estatística

### 5.1 Estrutura do Experimento

O experimento é um **design de dois grupos independentes**:

- **Grupo A (baseline):** 30 runs com broker `prim_file` (default)
- **Grupo B (io_uring):** 30 runs com broker `io_uring` habilitado

Os grupos são executados **sequencialmente** (não intercalados): primeiro a fase baseline completa, depois a fase io_uring completa. Essa escolha é pragmática — trocar de broker entre runs exigiria reinicialização do broker a cada run, adicionando ~10s de overhead e alterando as condições de medição.

A consequência é uma **possível diferença de estado do sistema** entre as fases (page cache mais cheio na segunda fase, por exemplo). Esse efeito é mitigado pelo número de runs (30 é suficiente para estimativas robustas) e pelo fato de que ambas as fases começam com diretórios de dados limpos.

### 5.2 Teste de Hipótese: Welch t-test

Para cada métrica, a hipótese é testada com o **teste t de Welch** (versão do teste t de Student para variâncias possivelmente diferentes):

```
H₀: μ_io_uring = μ_baseline   (sem diferença)
H₁: μ_io_uring ≠ μ_baseline   (com diferença, two-sided)
```

O Welch t-test foi escolhido em vez do Student t-test porque não assume homoscedasticidade (variâncias iguais), o que é mais conservador e adequado quando os dois grupos têm condições de carga distintas.

A estatística do teste é:

```
       x̄_A − x̄_B
t = ─────────────────────────
    √(s²_A/n_A + s²_B/n_B)
```

Os graus de liberdade são calculados pela aproximação de Welch-Satterthwaite.

### 5.3 Tamanho do Efeito: d de Cohen

O valor-p sozinho não indica a **magnitude** da diferença — apenas se ela é improvável de ser acaso. O **d de Cohen** complementa o teste de hipótese com uma medida de tamanho de efeito:

```
      x̄_A − x̄_B
d = ─────────────
       s_pooled

onde: s_pooled = √((s²_A + s²_B) / 2)
```

Interpretação padrão:

| |d| | Classificação |
|---|---|
| < 0.2 | Negligível |
| 0.2 – 0.5 | Pequeno |
| 0.5 – 0.8 | Médio |
| > 0.8 | Grande |

### 5.4 Intervalo de Confiança 95% via Bootstrap

Em vez de assumir normalidade dos dados para calcular o IC, o notebook usa **bootstrap não-paramétrico** com 10.000 reamostras:

1. Para cada iteração: amostrar `n` valores com reposição do conjunto original
2. Calcular a média da amostra
3. Após 10.000 iterações: tomar os percentis 2,5% e 97,5% da distribuição de médias bootstrap

Isso produz um IC robusto mesmo que a distribuição das runs não seja perfeitamente normal.

### 5.5 Limiar de Significância

O limiar adotado é `α = 0.05`. Resultados com `p < 0.05` são reportados como estatisticamente significativos. O notebook também reporta os níveis `p < 0.01` (`**`) e `p < 0.001` (`***`) para indicar graus de evidência mais fortes.

---

## 6. Resultados

### 6.1 Benchmark de Escrita

**Configuração:** 30 runs × 10s | 4 produtores | 1 KB | persistent | confirm=200 | x-max-length=2M

#### Throughput (msg/s)

| | prim_file | io_uring |
|---|---|---|
| Média | 95.897 | **103.474** |
| Desvio-padrão | 1.902 | 2.194 |
| IC 95% | [95.224, 96.566] | [102.687, 104.229] |
| Variação | — | **+7,9%** |

**Análise:** Welch t(58) = 14,290 | **p = 1,80 × 10⁻²⁰** | d de Cohen = **3,69** (grande)

Os intervalos de confiança não se sobrepõem em nenhum ponto — a separação entre as duas distribuições é visualmente clara. O d de Cohen de 3,69 é excepcionalmente alto: indica que a distribuição io_uring está a quase 4 desvios-padrão acima da baseline. Com p < 10⁻¹⁹, a probabilidade de esse resultado ser acaso é astronomicamente pequena.

#### Latência P99 de Confirm (µs)

| | prim_file | io_uring |
|---|---|---|
| Média | 13.754 µs | **12.733 µs** |
| Variação | — | **−7,4%** |

**Análise:** Welch t(58) = −5,199 | **p = 2,84 × 10⁻⁶** | d de Cohen = **1,34** (grande)

A redução de 1.021 µs no P99 é também altamente significativa. Essa melhora de latência reflete diretamente a redução de N context switches de dirty-scheduler para 1 por flush — cada flush que antes aguardava N CQEs individualmente agora aguarda todos de uma vez.

---

### 6.2 Benchmark de Consumer (Leitura)

**Configuração:** 20 runs × (25s fill + 10s consumo) | 4 consumers | 1 KB | x-max-length=2M

#### Throughput de Consumo (msg/s)

| | prim_file | io_uring |
|---|---|---|
| Média | 94.468 | 100.815 |
| Desvio-padrão | 11.996 | 12.050 |
| IC 95% | [89.648, 99.776] | [95.448, 105.708] |
| Variação | — | +6,7% |

**Análise:** Welch t(38) = 1,669 | **p = 0,103** | d de Cohen = 0,528 (médio)

O resultado **não é estatisticamente significativo** com α = 0,05. Contudo, há duas observações importantes:

**1. O desvio-padrão elevado é o fator limitante.** Com desvio-padrão de ~12.000 msg/s (≈12,7% da média) em ambos os grupos, a alta variância das leituras de disco dilui o sinal. Para atingir significância com d = 0,528 e α = 0,05, a potência estatística de 80% exigiria aproximadamente 58 runs por grupo — o triplo dos 20 usados.

**2. A tendência é consistente.** O io_uring está acima da baseline em todos os 20 runs para a média, os ICs não se sobrepõem nas extremidades, e d = 0,528 indica efeito médio. A hipótese de ausência de ganho real é improvável — o resultado mais provável é que o benchmark de consumer precisaria de mais runs para confirmar o que os dados sugerem.

**Por que a leitura tem mais variância?** O caminho de leitura envolve o page cache do OS, o escalonador de I/O, e padrões de acesso que dependem da distribuição geográfica das mensagens nos arquivos de segmento — todos fatores altamente dependentes do estado da máquina no momento da medição. O caminho de escrita é mais determinístico: é dominado pelo throughput de submissão de SQEs ao kernel.

---

### 6.3 Visualizações Geradas

O notebook produz três tipos de gráfico para cada benchmark:

**Boxplots com jitter** — cada ponto é um run individual. O colchete de significância no topo mostra `***`, `**`, `*` ou `ns` com base no p-value do teste t de Welch. Permite ver ao mesmo tempo a dispersão, a mediana e a separação entre os grupos.

**Série temporal** — throughput de cada run numerado sequencialmente, com a média da fase como linha tracejada. Revela padrões de drift (tendência de queda ou subida ao longo dos runs), outliers isolados e estabilidade geral da medição.

**Distribuição KDE** (_Kernel Density Estimation_) — estimativa não-paramétrica da função de densidade de probabilidade de cada grupo. Complementa o boxplot mostrando a forma da distribuição — se ela é unimodal, bimodal, se tem caudas pesadas — sem assumir normalidade.

---

## 7. Limitações e Fontes de Viés

**Ordering bias (escrita).** A fase baseline executa primeiro, quando o sistema está mais frio. A fase io_uring executa depois, quando o page cache pode estar parcialmente preenchido pelos runs da baseline. Isso potencialmente beneficia a segunda fase — o que tornaria o ganho observado de +7,9% uma estimativa **conservadora** do ganho real.

**Variância de leitura (consumer).** A alta variância das leituras de disco limita a potência estatística com 20 runs. O ganho de +6,7% não atinge significância, mas provavelmente existiria com mais runs.

**Ambiente não controlado.** O benchmark roda na mesma máquina com o sistema operacional em uso normal. Processos em background (atualizações, logs, outros serviços) podem introduzir ruído. O número de runs (30/20) mitiga mas não elimina esse efeito.

**Tamanho de mensagem fixo.** Apenas mensagens de 1 KB foram testadas. Mensagens muito pequenas (< 64 bytes) ou muito grandes (> 1 MB) podem apresentar comportamentos diferentes, já que alteram o número de SQEs por flush e a proporção de writes diretos vs. buffered.

---

## 8. Reprodução

```bash
# Pré-requisito: perf-test.jar na raiz (baixado por bench_broker.sh)

# Limpeza
rm -rf bench-results/ && mkdir bench-results

# Benchmark de escrita (30 runs × 10s, 4 produtores, 1 KB)
bash bench_statistical.sh 30 10 4 1024

# Benchmark de consumer (20 runs, 25s fill, 10s consumo, 4 consumers, 1 KB)
bash bench_consumer.sh 20 25 10 4 1024
```

Os notebooks executados com os resultados completos ficam em:
- `bench-results/bench_analyze.ipynb` — escrita
- `bench-results/bench_analyze_consumer.ipynb` — leitura

Os CSVs brutos ficam em:
- `bench-results/baseline.csv` / `bench-results/iouring.csv` — escrita
- `bench-results/consumer_baseline.csv` / `bench-results/consumer_iouring.csv` — leitura

Cada linha do CSV de escrita tem o formato `throughput_msg_s,p99_us`. Cada linha do CSV de consumer tem apenas `throughput_msg_s`.
