# Integração io_uring no RabbitMQ Classic Queue

## Sumário

Este documento descreve a integração do subsistema `io_uring` do Linux no caminho de persistência das filas clássicas (_classic queues_) do RabbitMQ. O objetivo é reduzir o overhead de syscalls na escrita de mensagens persistentes, substituindo chamadas individuais de `file:write` por submissões em lote via `io_uring`, e medir o impacto real de desempenho em throughput e latência.

---

## 1. Contexto e Motivação

O RabbitMQ grava mensagens persistentes em disco por meio de três camadas principais:

- **`rabbit_msg_store`** — armazena os corpos das mensagens em arquivos de segmento sequenciais.
- **`rabbit_classic_queue_store_v2`** — armazena metadados por-fila em arquivos de segmento indexados por offset.
- **`rabbit_classic_queue_index_v2`** — mantém o índice de posições das mensagens dentro dos arquivos de segmento.

O caminho de escrita padrão funciona assim: mensagens são acumuladas em um buffer em memória (`prim_buffer`) e periodicamente descarregadas com `file:write`, que internamente emite uma chamada de sistema `write(2)` por flush. Com alta taxa de publicação de mensagens persistentes, esse padrão gera centenas de syscalls por segundo, cada uma com custo de troca de contexto entre modo usuário e modo kernel.

O `io_uring` permite submeter N operações de I/O ao kernel em **uma única syscall** (`io_uring_enter`), reduzindo drasticamente o overhead de troca de contexto sob carga elevada. Adicionalmente, o modo SQPOLL elimina até essa única syscall, permitindo que um thread do kernel faça polling contínuo da fila de submissão.

---

## 2. Biblioteca NIF

A base da integração é uma biblioteca Erlang NIF desenvolvida pelo autor, disponível em `/home/areias/dev/opensource/io_uring.erl`. Ela expõe o `io_uring` do Linux para o runtime Erlang por meio de funções C que interagem com `liburing`.

As funções principais utilizadas nesta integração são:

| Função NIF | Descrição |
|---|---|
| `io_uring:setup/2` | Cria um ring io_uring com N entradas |
| `io_uring:prep/3` | Prepara uma SQE (Submission Queue Entry) sem submeter |
| `io_uring:submit/1` | Submete todas as SQEs pendentes em uma única `io_uring_enter` |
| `io_uring:wait_cqe/1` | Aguarda e retorna um CQE (Completion Queue Entry) |
| `io_uring:wait_n_cqes/2` | Aguarda N CQEs em uma única chamada de dirty-scheduler |
| `io_uring:cqe_seen/2` | Marca um CQE como consumido, avançando o head do CQ |
| `io_uring:teardown/1` | Destrói o ring |

A função `wait_n_cqes/2` foi adicionada especificamente para esta integração. O motivador é o modelo de execução do Erlang: funções NIF marcadas como `ERL_NIF_DIRTY_JOB_IO_BOUND` são executadas em threads de dirty-scheduler separados. Chamar `wait_cqe` N vezes resultava em N trocas de contexto para o dirty-scheduler — uma por CQE coletado. Com `wait_n_cqes`, todos os N CQEs são coletados em **uma única chamada**, reduzindo N trocas a 1.

---

## 3. Alterações Realizadas no RabbitMQ

### 3.1 `rabbitmq-components.mk` — Declaração da Dependência

```makefile
dep_io_uring = git file:///home/areias/dev/opensource/io_uring.erl main
```

Uma linha foi adicionada ao arquivo `rabbitmq-components.mk` para declarar a biblioteca NIF `io_uring` como dependência gerenciada pelo sistema de build `erlang.mk`. Isso permite que `make deps` clone e compile a biblioteca automaticamente.

### 3.2 `deps/rabbit/Makefile` — Inclusão nos Deps do Core

```makefile
DEPS = ... io_uring
```

A dependência `io_uring` foi adicionada à lista `DEPS` do plugin `rabbit` (o core do servidor), para que seja compilada e carregada junto com o broker.

### 3.3 `deps/rabbit/priv/schema/rabbit.schema` — Configuração

Dois novos parâmetros foram adicionados ao arquivo de schema do `cuttlefish`, que traduz `rabbitmq.conf` para termos Erlang:

**`message_store.io_uring`** (padrão: `false`)
Habilita o caminho de escrita via `io_uring`. Quando `true`, writes de mensagens persistentes são submetidos via `io_uring` em vez de `file:write`. Requer Linux 5.1+ e `liburing`. É ignorado em outros sistemas operacionais.

**`message_store.io_uring_sqpoll`** (padrão: `false`)
Habilita o modo SQPOLL. Nesse modo, o kernel mantém um thread que faz polling contínuo da fila de submissão, eliminando a syscall `io_uring_enter` por lote. Requer Linux 5.12+ para uso sem privilégios (ou `CAP_SYS_NICE` em versões anteriores). Se indisponível, faz fallback automaticamente para um ring normal.

### 3.4 `deps/rabbit/src/rabbit_io_uring.erl` — Módulo Adaptador

Este módulo novo centraliza toda a interface entre o RabbitMQ e a NIF `io_uring`. Ele cumpre três papéis:

**Detecção de disponibilidade:** Na inicialização do broker, `rabbit_io_uring:start/0` testa se `io_uring` está disponível no kernel atual (criando e destruindo um ring de teste dentro de um `try-catch`). O resultado é armazenado em `persistent_term`, tornando `is_available/0` uma consulta O(1) sem overhead em código de hot path.

**Gerenciamento de rings:** Cada writer do message store e cada processo de fila clássica recebe seu próprio ring io_uring dedicado, criado via `create_ring/0` ou `create_queue_ring/0`. O isolamento por ring evita contenção de locks entre writers concorrentes e elimina qualquer possibilidade de CQEs de operações distintas serem coletados por quem não os submeteu.

**Primitivas de I/O:**

- `write/4` — escrita de um único binário em um offset absoluto. Usado para mensagens grandes que não passam pelo buffer.
- `writev/4` — escrita em lote de um iovec (lista de binários) a partir de um offset base. Cada binário vira um SQE separado, todos submetidos em uma única chamada `io_uring_enter`. Elimina o custo de `iolist_to_binary` antes da escrita.
- `pwritev/3` — scatter write: lista de pares `{Offset, Binary}`. Todos os SQEs submetidos em uma única chamada. Substitui `file:pwrite/2` nos flush paths do queue store e do índice.
- `preadv/3` — scatter-gather pread: lista de pares `{Offset, Size}`. Todos os reads submetidos em uma única chamada, resultados retornados na ordem original. Substitui N chamadas individuais de `file:pread`.

### 3.5 `deps/rabbit/src/rabbit_msg_store.erl` — Message Store

Esta é a camada que armazena os corpos das mensagens. A struct interna `#writer{}` foi estendida com três campos novos:

- `ring` — o ring io_uring dedicado a este writer (ou `undefined` quando io_uring não está disponível)
- `raw_fd` — o file descriptor bruto do sistema operacional (inteiro), necessário para submissão de SQEs diretamente à NIF
- `write_offset` — o offset em bytes do próximo write no arquivo de segmento, mantido em Erlang para não depender de seeks no descritor

O raciocínio para manter `write_offset` em Erlang é que o `io_uring` escreve em offsets absolutos (`pwrite` semantics), e cada SQE precisa especificar o offset. Rastreando o offset incrementalmente no estado do writer, evitamos qualquer chamada de seek.

As funções internas `writer_open`, `writer_recover`, `writer_flush`, `writer_direct_write` e `writer_close` foram modificadas para usar dois caminhos distintos via pattern matching: quando `ring = undefined`, o comportamento original com `file:write` é preservado; quando o ring está disponível, as operações são roteadas para `rabbit_io_uring`.

Na inicialização do message store, `rabbit_io_uring:start/0` é chamado uma única vez para detectar e cachear a disponibilidade do io_uring.

### 3.6 `deps/rabbit/src/rabbit_classic_queue_store_v2.erl` — Queue Store

O queue store gerencia arquivos de segmento por fila, onde cada mensagem ocupa uma posição indexada. A integração aqui foi feita em dois caminhos:

**Escrita (`pwritev`):** Quando o buffer de escrita é descarregado, em vez de chamar `file:pwrite/2` com uma lista de `{Offset, Data}`, o código agora chama `rabbit_io_uring:pwritev/3`, que submete todos os pares em um único `io_uring_enter`. Isso é particularmente relevante quando há acúmulo de muitas entradas pendentes — cada entrada vira um SQE, e todos são completados com uma única syscall.

**Leitura (`preadv`):** Para leitura de mensagens do disco, em vez de N chamadas individuais de `file:pread`, o código agora usa `rabbit_io_uring:preadv/3`, que submete N reads em uma única chamada e retorna todos os binários na ordem original.

O queue store mantém descritores de arquivo em cache (um `RawWriteFd` para escrita e um `RawReadFd` para leitura) que são abertos via `rabbit_io_uring:open_fd/2` e fechados via `rabbit_io_uring:close_fd/2` quando o segmento ativo muda.

### 3.7 `deps/rabbit/src/rabbit_classic_queue_index_v2.erl` — Queue Index

O índice mantém registros de posição para cada mensagem dentro dos segmentos. Segue o mesmo padrão do queue store: os flushes do índice para disco passaram a usar `pwritev`, consolidando todas as escritas de um flush em uma única syscall.

---

## 4. Otimização: Coleta Batch de CQEs (`wait_n_cqes`)

### 4.1 O Problema

O Erlang executa NIFs que fazem I/O bloqueante em threads de _dirty scheduler_. Cada chamada a uma NIF dirty causa uma troca de contexto do scheduler Erlang para a thread dirty. No código original, `collect_n_cqes` chamava `wait_cqe` em loop — uma chamada dirty por CQE coletado. Para um flush de 50 writes, isso gerava 50 trocas de contexto, consumindo dezenas de microssegundos de overhead apenas em scheduling.

### 4.2 A Solução

A NIF `wait_n_cqes(Ring, N)` foi adicionada à biblioteca io_uring. Ela coleta todos os N CQEs em **uma única chamada dirty-scheduled**:

1. `io_uring_wait_cqe_nr(ring, &dummy, N)` — aguarda até que todos os N CQEs estejam disponíveis (uma única syscall potencial)
2. `io_uring_peek_batch_cqe(ring, cqes, N)` — lê todos os N CQEs de uma vez sem syscall
3. Decodifica todos os resultados em termos Erlang
4. `io_uring_cq_advance(ring, N)` — avança o head do CQ em uma única operação atômica

O resultado: N trocas de contexto dirty → 1.

### 4.3 Bug Crítico Identificado e Corrigido

Durante os testes, um bug severo foi identificado na implementação inicial de `nif_wait_n_cqes`. A versão original usava um loop com chamadas repetidas a `io_uring_peek_batch_cqe`:

```c
/* Versão BUGADA — NÃO USAR */
while (collected < n) {
    unsigned got = io_uring_peek_batch_cqe(ring, cqes + collected, n - collected);
    if (got == 0) {
        io_uring_wait_cqe_nr(ring, &dummy, n - collected); /* espera mais */
    } else {
        collected += got;
    }
}
```

O problema: `io_uring_peek_batch_cqe` **sempre lê a partir do head atual do CQ ring**, sem avançá-lo. Quando a primeira chamada retornava menos do que N CQEs (por exemplo, apenas 3 de 5 esperados), o fallback chamava `io_uring_wait_cqe_nr` e depois `peek_batch_cqe` novamente — desta vez a partir do **mesmo head**, produzindo ponteiros duplicados para os CQEs já coletados. No passo de decodificação, a função `sqe_ctx_free` era chamada duas vezes sobre o mesmo ponteiro `ctx`, resultando em um **double-free** que derrubava o processo BEAM silenciosamente (sem nenhuma entrada no log Erlang).

O sintoma era: o broker sobrevivia ao benchmark de escrita (onde o iovec tem poucos elementos e a primeira chamada ao `peek` sempre retorna todos os CQEs de uma vez), mas morria no benchmark de consumer (onde o queue store e o índice geram iovecs maiores sob carga de max-length, fazendo com que o primeiro `peek` retornasse menos do que N e disparasse o caminho bugado).

A correção substituiu o loop por uma sequência simples e correta:

```c
/* Versão CORRETA */
io_uring_wait_cqe_nr(ring, &dummy, n); /* espera todos os N de uma vez */
unsigned collected = io_uring_peek_batch_cqe(ring, cqes, n); /* lê todos */
```

Uma única chamada ao `peek`, garantidamente após todos os N CQEs estarem disponíveis. Sem segunda chamada, sem ponteiros duplicados, sem double-free.

---

## 5. Metodologia do Benchmark

### 5.1 Configuração do Ambiente

O benchmark compara diretamente duas configurações do mesmo broker RabbitMQ:

- **Baseline** — broker padrão, `message_store.io_uring = false` (usa `prim_file`/`file:write`)
- **io_uring** — broker com `message_store.io_uring = true`

Cada fase usa um broker completamente separado, iniciado a partir de um diretório de dados limpo (`make virgin-test-tmpdir`). O broker é iniciado em modo background e o benchmark aguarda confirmação de startup via `rabbitmqctl await_startup` antes de iniciar as medições.

O cliente de benchmark é o [RabbitMQ PerfTest](https://perftest.rabbitmq.com/) (`perf-test.jar`), o cliente de carga oficial do RabbitMQ.

### 5.2 Benchmark de Escrita (`bench_statistical.sh`)

Mede o throughput de publicação de mensagens persistentes.

**Parâmetros de execução:**
- 30 runs por fase (baseline e io_uring)
- 10 segundos de medição por run
- 4 produtores paralelos
- Mensagens de 1 KB, flag `persistent`, `confirm=200`
- Fila clássica com `x-max-length=2000000`

**O que é medido:**
- Throughput médio de publicações confirmadas por segundo (msg/s)
- Latência P99 de publisher confirms (µs)

**Metodologia estatística:**
- Um run de warmup é descartado antes da coleta
- A análise usa o **teste t de Welch** (variâncias possivelmente desiguais) para comparar as duas distribuições
- São reportados: média, intervalo de confiança 95% via bootstrap (10.000 amostras), valor-p e d de Cohen (tamanho do efeito)
- Limiar de significância: p < 0.05

**Análise e visualização:**
A análise é executada em um **Jupyter Notebook** (`bench_analyze.ipynb`), chamado automaticamente pelo script ao final de cada fase de coleta via `jupyter nbconvert --to notebook --execute`. Os parâmetros (caminhos dos CSVs, número de runs, duração, tamanho de mensagem) são passados por variáveis de ambiente `BENCH_*`. O notebook executado, com todas as saídas e gráficos embutidos, é salvo em `bench-results/`. São gerados três tipos de visualização:

- **Boxplots** com jitter e colchete de significância estatística (***/**/*/ns)
- **Série temporal** das runs para avaliar estabilidade e tendências intra-fase
- **Distribuição KDE** (Kernel Density Estimation) para comparar a forma das distribuições

### 5.3 Benchmark de Leitura/Consumer (`bench_consumer.sh`)

Mede o throughput de consumo de mensagens que estão inteiramente no disco, forçando o caminho de leitura.

**Parâmetros de execução:**
- 20 runs por fase
- Cada run tem duas etapas:
  1. **Fill (25s):** 4 produtores enchem a fila até 2M mensagens — após ~4s a fila está cheia e começa a descartar mensagens antigas para cada nova mensagem inserida (rolling replacement)
  2. **Consume (10s):** os produtores param; 4 consumers consomem mensagens do disco sem novos writes
- Mensagens de 1 KB, `confirm=200`

A fase de fill é essencial para garantir que as mensagens estejam efetivamente no disco e fora do write buffer/page cache no momento da medição de consumo.

---

## 6. Resultados

Os benchmarks foram executados após todas as correções estarem em vigor, com a pasta de resultados limpa para garantir que apenas dados desta rodada estejam presentes.

### 6.1 Benchmark de Escrita

**Configuração:** 30 runs × 10s | 4 produtores | 1 KB | persistent | x-max-length=2M

| Métrica | prim_file (baseline) | io_uring | Variação |
|---|---|---|---|
| Throughput médio | 95.897 msg/s | **103.474 msg/s** | **+7,9%** |
| IC 95% | [95.224, 96.566] | [102.687, 104.229] | |
| P99 confirm latency | 13.754 µs | **12.733 µs** | **-7,4%** |
| IC 95% (latência) | [13.485, 14.055] | [12.508, 13.003] | |

**Análise estatística (throughput):**
- Welch t(58) = 14,290 | p = 1,80 × 10⁻²⁰ | d de Cohen = 3,69 (efeito **grande**)
- A diferença é **altamente significativa** e improvável de ser explicada por variação aleatória.

**Análise estatística (latência P99):**
- Welch t(58) = −5,199 | p = 2,84 × 10⁻⁶ | d de Cohen = 1,34 (efeito **grande**)
- A redução de latência também é **altamente significativa**.

O io_uring entrega +7,9% de throughput e −7,4% de latência P99 em relação ao prim_file, ambos com evidência estatística irrefutável.

### 6.2 Benchmark de Leitura (Consumer)

**Configuração:** 20 runs × (25s fill + 10s consume) | 4 consumers | 1 KB | x-max-length=2M

| Métrica | prim_file (baseline) | io_uring | Variação |
|---|---|---|---|
| Throughput médio | 94.468 msg/s | 100.814 msg/s | +6,7% |
| IC 95% | [89.648, 99.776] | [95.448, 105.708] | |

**Análise estatística:**
- Welch t(38) = 1,669 | p = 0,103 | d de Cohen = 0,528 (efeito médio)
- A diferença é **não significativa** (p ≥ 0,05) com 20 runs.

O caminho de leitura ainda usa `wait_cqe` por CQE individualmente (sem batch de coleta de CQEs), o que limita o ganho. O +6,7% observado é uma tendência consistente mas que não atinge significância estatística com 20 runs devido à alta variância inerente às leituras de disco. Para confirmar o ganho em leitura com significância, seria necessário aumentar o número de runs ou reduzir a variância da carga.

### 6.3 Estabilidade

Antes da correção do bug de double-free, o broker falhava consistentemente durante o benchmark de consumer (0 de 40 runs bem-sucedidas). Após a correção, **20 de 20 runs completaram sem nenhuma falha ou crash**.

---

## 7. Como Replicar os Benchmarks

### 7.1 Pré-requisitos

- Linux com kernel >= 5.1
- `liburing` instalado (`pacman -S liburing` / `apt install liburing-dev`)
- Erlang/OTP 27+, rebar3
- Java 17+ (para o PerfTest)
- Make 4+

### 7.2 Construir a Biblioteca NIF

```bash
cd /home/areias/dev/opensource/io_uring.erl
make
```

### 7.3 Construir o RabbitMQ

```bash
cd /home/areias/dev/opensource/rabbitmq-server
make -C deps/rabbit
```

### 7.4 Sincronizar para o Diretório de Plugins

O broker carrega BEAM e `.so` a partir de `plugins/`, não de `deps/`. Este passo é obrigatório após qualquer recompilação:

```bash
# Após recompilar deps/rabbit:
cp deps/rabbit/ebin/rabbit_io_uring.beam \
    plugins/rabbit-4.2.0+beta.4.687.gac4538c.dirty/ebin/

# Após recompilar a NIF (io_uring.erl):
cp /home/areias/dev/opensource/io_uring.erl/priv/io_uring_nif.so \
    plugins/io_uring-0.1.0/priv/
cp /home/areias/dev/opensource/io_uring.erl/_build/default/lib/io_uring/ebin/io_uring.beam \
    plugins/io_uring-0.1.0/ebin/
```

### 7.5 Executar os Benchmarks

```bash
# Baixar o PerfTest (se ainda não tiver)
bash bench_broker.sh

# Limpar resultados anteriores
rm -rf bench-results/ && mkdir bench-results

# Benchmark de escrita (30 runs × 10s, 4 produtores, 1 KB)
bash bench_statistical.sh 30 10 4 1024

# Benchmark de leitura/consumer (20 runs, 25s fill, 10s consume, 4 consumers, 1 KB)
bash bench_consumer.sh 20 25 10 4 1024
```

Ao final de cada benchmark, o notebook `bench_analyze.ipynb` é executado automaticamente e o resultado — com gráficos e análise estatística completos — é salvo como notebook executado em `bench-results/`. Os PNGs também são exportados individualmente para facilitar consulta rápida.

### 7.6 Habilitar io_uring Manualmente

Para iniciar o broker com io_uring habilitado fora do script de benchmark:

```ini
# rabbitmq.conf
message_store.io_uring = true
message_store.io_uring_sqpoll = false  # opcional; requer Linux 5.12+ ou CAP_SYS_NICE
```

Para verificar que o io_uring foi detectado com sucesso, procure no log do broker:

```
io_uring: enabled (sqpoll=false)
```

---

## 8. Arquivos Modificados

| Arquivo | Tipo de Alteração |
|---|---|
| `rabbitmq-components.mk` | Declaração da dependência `io_uring` |
| `deps/rabbit/Makefile` | Inclusão de `io_uring` nos `DEPS` |
| `deps/rabbit/priv/schema/rabbit.schema` | Parâmetros `message_store.io_uring` e `message_store.io_uring_sqpoll` |
| `deps/rabbit/src/rabbit_io_uring.erl` | Módulo adaptador novo (adapter layer) |
| `deps/rabbit/src/rabbit_msg_store.erl` | Caminho de escrita via `writev` |
| `deps/rabbit/src/rabbit_classic_queue_store_v2.erl` | Escrita via `pwritev`, leitura via `preadv` |
| `deps/rabbit/src/rabbit_classic_queue_index_v2.erl` | Flush do índice via `pwritev` |
| `/home/areias/dev/opensource/io_uring.erl/native/io_uring_nif.c` | Adição de `nif_wait_n_cqes` e correção do bug de double-free |
| `bench_analyze.ipynb` | Notebook Jupyter de análise estatística e visualização (substitui `bench_analyze.py`) |
| `bench_statistical.sh` | Chamada ao notebook via `jupyter nbconvert --execute` |
| `bench_consumer.sh` | Chamada ao notebook via `jupyter nbconvert --execute` |
