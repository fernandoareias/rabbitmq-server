# Arquitetura do RabbitMQ e Integração io_uring

## 1. Visão Geral

O RabbitMQ é um broker de mensagens multi-protocolo. Quando uma mensagem é publicada com flag `persistent`, ela precisa ser gravada em disco antes de o broker enviar a confirmação (_publisher confirm_) ao produtor. É nesse caminho — da publicação ao disco — que o io_uring atua.

Este documento descreve, de dentro para fora, como o RabbitMQ armazena mensagens persistentes e em quais pontos exatos a integração io_uring foi aplicada.

---

## 2. O Caminho de uma Mensagem Persistente

Quando um produtor publica uma mensagem `persistent` para uma fila clássica (_classic queue_), ela percorre o seguinte caminho até o disco:

```mermaid
flowchart TD
    P(["Produtor\n(AMQP client)"])
    CH["rabbit_channel\n(processo por conexão)"]
    AQ["rabbit_amqqueue_process\n(processo por fila)"]
    CQ["rabbit_classic_queue\n(lógica v2)"]
    IDX["rabbit_classic_queue_index_v2\nÍndice de posições"]
    STR["rabbit_classic_queue_store_v2\nStore por-fila"]
    MSG["rabbit_msg_store\nStore compartilhado"]
    DISK[("Disco\n.rdq / .idx")]

    P -- "AMQP publish\n(persistent)" --> CH
    CH -- "cast {write,...}" --> AQ
    AQ -- "write / write_flow" --> CQ
    CQ -- "flush_buffer → pwritev" --> IDX
    CQ -- "flush_buffer → pwritev" --> STR
    CQ -- "writer_flush → writev" --> MSG
    IDX -- "io_uring" --> DISK
    STR -- "io_uring" --> DISK
    MSG -- "io_uring" --> DISK

    style IDX fill:#e3f2fd,stroke:#1565c0
    style STR fill:#e3f2fd,stroke:#1565c0
    style MSG fill:#e3f2fd,stroke:#1565c0
    style DISK fill:#fce4ec,stroke:#880e4f
```

Há **três camadas de armazenamento** distintas, cada uma com seus próprios arquivos de segmento em disco. O io_uring foi integrado nas três (destacadas em azul).

---

## 3. As Três Camadas de Armazenamento

```mermaid
block-beta
  columns 3

  block:fila["Por Fila"]:2
    IDX["rabbit_classic_queue_index_v2\n─────────────────\nSeqId → Localização física\nArquivos: .idx por segmento\nEscrita: pwritev (io_uring)"]
    STR["rabbit_classic_queue_store_v2\n─────────────────\nCorpos estruturados por-fila\nArquivos: .qs por segmento\nEscrita: pwritev  Leitura: preadv"]
  end

  block:global["Compartilhado (vhost)"]:1
    MSG["rabbit_msg_store\n─────────────────\nCorpos brutos de mensagens\nArquivos: .rdq (segmentos seq.)\nEscrita: writev (io_uring)"]
  end

  style IDX fill:#e3f2fd,stroke:#1565c0
  style STR fill:#e3f2fd,stroke:#1565c0
  style MSG fill:#e8f5e9,stroke:#2e7d32
```

### 3.1 Message Store (`rabbit_msg_store`)

**O que é:** Um servidor `gen_server` compartilhado entre todas as filas clássicas da mesma vhost. Armazena os **corpos brutos** das mensagens em arquivos de segmento sequenciais (`.rdq`). Mensagens de múltiplas filas são intercaladas no mesmo arquivo.

**Como funciona:** As mensagens chegam por mensagens `cast {write, ...}` de vários processos de fila. O message store não grava imediatamente — ele acumula os dados em um buffer em memória (`prim_buffer`) e descarrega em dois casos:

- **Por tamanho:** quando o buffer ultrapassa um limiar definido pelo tamanho da mensagem
- **Por timer/sync:** quando `internal_sync` é chamado (a cada publisher confirm em lote, ou periodicamente)

```mermaid
sequenceDiagram
    participant Q as rabbit_amqqueue_process
    participant MS as rabbit_msg_store
    participant BUF as prim_buffer
    participant IO as io_uring (NIF)
    participant DISK as Disco (.rdq)

    Q->>MS: cast {write, MsgId, Body}
    MS->>BUF: prim_buffer:write(Body)
    Note over BUF: acumula até threshold ou sync

    Q->>MS: cast {confirm, SeqIds}
    MS->>MS: internal_sync()
    MS->>BUF: read_iovec(Buffer, Size)
    BUF-->>MS: [Bin1, Bin2, ..., BinN]
    MS->>IO: writev(Ring, RawFd, IoVec, Offset)
    IO->>DISK: N writes em 1 io_uring_enter
    DISK-->>IO: N CQEs
    IO-->>MS: {ok, TotalBytes}
    MS-->>Q: confirms enviados
```

**Estado interno relevante:**
```erlang
#writer{
    fd           :: file:fd(),            %% descritor Erlang (para fallback)
    buffer       :: prim_buffer(),        %% buffer de escrita em memória
    ring         :: ring() | undefined,   %% ring io_uring (quando disponível)
    raw_fd       :: integer() | undefined,%% fd bruto do OS para io_uring
    write_offset :: non_neg_integer()     %% offset atual no arquivo
}
```

**Onde o io_uring entra:**

| Operação | Sem io_uring | Com io_uring |
|---|---|---|
| Flush do buffer (N chunks) | `file:write(Fd, IoVec)` | `writev(Ring, RawFd, IoVec, Offset)` |
| Escrita direta (msg grande) | `file:write(Fd, Data)` | `write(Ring, RawFd, Data, Offset)` |

---

### 3.2 Queue Store (`rabbit_classic_queue_store_v2`)

**O que é:** Um armazenamento **por fila**, gerenciado pelo processo `rabbit_amqqueue_process`. Armazena entradas estruturadas de cada mensagem — offset dentro do segmento, tamanho, CRC32 opcional — em arquivos de segmento dedicados para cada fila.

**Como funciona:** Cada mensagem escrita gera uma entrada no `write_buffer` (um map `SeqId → {Offset, Size, Dados}`). Quando o buffer acumula dados suficientes (ou quando o índice solicita sync), `flush_buffer` consolida as entradas por segmento e as grava. A leitura é feita em lote por `read_many`.

```mermaid
flowchart LR
    subgraph Escrita
        WB["write_buffer\nmap SeqId → Entry"]
        FB["flush_buffer"]
        PW["pwritev\n{Off₁,Bin₁}…{OffN,BinN}"]
    end

    subgraph Leitura
        RM["read_many\n[{Off,Size}]"]
        PR["preadv\n1 io_uring_enter"]
    end

    SEG[("Arquivo de\nSegmento .qs")]

    WB -->|"tamanho ≥ threshold\nou sync request"| FB
    FB --> PW
    PW -->|"N SQEs\n1 submit"| SEG
    SEG -->|"N CQEs"| PW

    RM --> PR
    PR -->|"N reads\n1 submit"| SEG
    SEG -->|"N binários"| PR

    style PW fill:#e3f2fd,stroke:#1565c0
    style PR fill:#e3f2fd,stroke:#1565c0
```

**Onde o io_uring entra:**

| Operação | Sem io_uring | Com io_uring |
|---|---|---|
| Flush do buffer por segmento | `file:pwrite(Fd, [{Offset, Data}])` | `pwritev(Ring, RawFd, [{Offset, Data}])` |
| Leitura em lote (`read_many`) | N × `file:pread(Fd, Offset, Size)` | `preadv(Ring, RawFd, [{Offset, Size}])` |

---

### 3.3 Queue Index (`rabbit_classic_queue_index_v2`)

**O que é:** O índice da fila, também por fila. Mantém o mapeamento entre `SeqId` (número de sequência da mensagem na fila) e a localização física no queue store. É escrito em arquivos de segmento separados, um por intervalo de SeqIds.

**Como funciona:** Publicações, acknowledgements e redeliveries geram entradas no `write_buffer` do índice. Quando o buffer de um segmento enche (ou em eventos de sync), `flush_buffer` consolida as entradas ordenadas por offset e as escreve. O índice pode escrever em **múltiplos segmentos** em um único flush.

```mermaid
flowchart TD
    PUB["publish / ack / redeliver"] --> WB["write_buffer\nmap Segment → [LocBytes]"]
    WB -->|"flush_buffer"| LOOP["para cada segmento"]
    LOOP -->|"pwritev"| S0[("Seg 0\n.idx")]
    LOOP -->|"pwritev"| S1[("Seg 1\n.idx")]
    LOOP -->|"pwritev"| SN[("Seg N\n.idx")]

    style S0 fill:#fff9c4,stroke:#f57f17
    style S1 fill:#fff9c4,stroke:#f57f17
    style SN fill:#fff9c4,stroke:#f57f17
```

**Onde o io_uring entra:**

| Operação | Sem io_uring | Com io_uring |
|---|---|---|
| Flush por segmento | `file:pwrite(Fd, [{Offset, Data}])` | `pwritev(Ring, RawFd, [{Offset, Data}])` |

---

## 4. O Módulo Adaptador: `rabbit_io_uring`

Para isolar todos os módulos acima da NIF `io_uring`, foi criado o módulo `rabbit_io_uring` como camada de adaptação.

```mermaid
flowchart TB
    subgraph RabbitMQ["RabbitMQ (Erlang)"]
        MS["rabbit_msg_store"]
        QS["rabbit_classic_queue_store_v2"]
        QI["rabbit_classic_queue_index_v2"]
        ADAPT["rabbit_io_uring\n(módulo adaptador)"]
    end

    subgraph NIF["io_uring NIF (C / liburing)"]
        SETUP["setup / teardown"]
        PREP["prep"]
        SUBMIT["submit"]
        WAIT["wait_cqe\nwait_n_cqes"]
        SEEN["cqe_seen"]
    end

    subgraph KERNEL["Linux Kernel"]
        SQ["SQ Ring\n(Submission Queue)"]
        CQ["CQ Ring\n(Completion Queue)"]
    end

    DISK[("Disco")]

    MS --> ADAPT
    QS --> ADAPT
    QI --> ADAPT
    ADAPT --> PREP
    ADAPT --> SUBMIT
    ADAPT --> WAIT
    PREP --> SQ
    SUBMIT -->|"io_uring_enter\n(1 syscall)"| SQ
    SQ -->|"operações de I/O"| DISK
    DISK -->|"completions"| CQ
    CQ --> WAIT

    style ADAPT fill:#f3e5f5,stroke:#6a1b9a
    style SQ fill:#e8f5e9,stroke:#2e7d32
    style CQ fill:#e8f5e9,stroke:#2e7d32
```

### 4.1 Detecção de Disponibilidade

```mermaid
flowchart TD
    START(["rabbit_io_uring:start/0"])
    CFG{"msg_store_io_uring\n= true?"}
    OS{"Sistema\nLinux?"}
    PROBE["io_uring:setup 1 0\nio_uring:teardown"]
    OK["persistent_term ← true\nlog: io_uring enabled"]
    FAIL["persistent_term ← false\n(silencioso)"]

    START --> CFG
    CFG -- Não --> FAIL
    CFG -- Sim --> OS
    OS -- Não --> FAIL
    OS -- Sim --> PROBE
    PROBE -- sucesso --> OK
    PROBE -- exceção --> FAIL

    style OK fill:#e8f5e9,stroke:#2e7d32
    style FAIL fill:#fce4ec,stroke:#c62828
```

O resultado fica em `persistent_term` — leitura O(1) sem lock — para que `is_available/0` possa ser chamado livremente em hot paths.

### 4.2 Gerenciamento de Rings

Cada contexto de I/O recebe seu **próprio ring dedicado**. O isolamento garante que CQEs de operações distintas nunca se misturem.

```mermaid
flowchart LR
    subgraph Fila A
        QIA["Index\nring=Ring_A1"]
        QSA["Store\nring=Ring_A2"]
    end

    subgraph Fila B
        QIB["Index\nring=Ring_B1"]
        QSB["Store\nring=Ring_B2"]
    end

    subgraph "Msg Store (shared)"
        MSW["Writer\nring=Ring_MS"]
    end

    Ring_A1(("Ring A1"))
    Ring_A2(("Ring A2"))
    Ring_B1(("Ring B1"))
    Ring_B2(("Ring B2"))
    Ring_MS(("Ring MS"))
    DISK[("Disco")]

    QIA --- Ring_A1 --> DISK
    QSA --- Ring_A2 --> DISK
    QIB --- Ring_B1 --> DISK
    QSB --- Ring_B2 --> DISK
    MSW --- Ring_MS --> DISK
```

### 4.3 Primitivas de I/O

```mermaid
flowchart LR
    subgraph "write — 1 binário"
        W1["prep(write, Off)"] --> WS1["submit\n1 io_uring_enter"] --> WC1["1 CQE"]
    end

    subgraph "writev — iovec sequencial"
        WV1["prep×N\n(cada chunk)"] --> WVS["submit\n1 io_uring_enter"] --> WVC["wait_n_cqes N\n1 dirty call"]
    end

    subgraph "pwritev — offsets arbitrários"
        PW1["prep×N\n{Off,Bin} pairs"] --> PWS["submit\n1 io_uring_enter"] --> PWC["wait_n_cqes N\n1 dirty call"]
    end

    subgraph "preadv — scatter-gather read"
        PR1["prep×N\n{Off,Size} pairs"] --> PRS["submit\n1 io_uring_enter"] --> PRC["wait_cqe×N\nN dirty calls"]
    end
```

---

## 5. O Modelo de Execução: Dirty Schedulers e Batch CQE Collection

### 5.1 O Problema: N Context Switches por Flush

```mermaid
sequenceDiagram
    participant E as Processo Erlang
    participant DS as Dirty Scheduler Thread
    participant K as Kernel (io_uring)

    Note over E,K: Abordagem ANTIGA — wait_cqe em loop

    E->>DS: context switch (flush N=5 writes)
    DS->>K: io_uring_enter (submit 5 SQEs)
    DS->>E: context switch de volta

    loop 5 vezes (uma por CQE)
        E->>DS: context switch
        DS->>K: wait_cqe
        K-->>DS: CQE i
        DS->>E: context switch de volta
    end
    Note over E,K: Total: 1 + 5 = 6 context switches por flush
```

### 5.2 A Solução: `wait_n_cqes` — 1 Context Switch por Flush

```mermaid
sequenceDiagram
    participant E as Processo Erlang
    participant DS as Dirty Scheduler Thread
    participant K as Kernel (io_uring)

    Note over E,K: Abordagem NOVA — wait_n_cqes

    E->>DS: context switch (flush N=5 writes)
    DS->>K: io_uring_enter (submit 5 SQEs)
    DS->>E: context switch de volta

    E->>DS: context switch (1 único dirty call)
    DS->>K: io_uring_wait_cqe_nr(5) — espera todos
    K-->>DS: 5 CQEs disponíveis
    DS->>K: io_uring_peek_batch_cqe(5) — lê todos
    DS->>K: io_uring_cq_advance(5) — avança de uma vez
    DS->>E: context switch de volta
    Note over E,K: Total: 1 + 1 = 2 context switches por flush
```

### 5.3 O Bug de Double-Free e sua Correção

A implementação inicial de `nif_wait_n_cqes` chamava `io_uring_peek_batch_cqe` em loop. A função `peek_batch` lê sempre a partir do **head atual do CQ ring, sem avançá-lo**. Quando o primeiro `peek` retornava menos do que N CQEs, o fallback chamava `io_uring_wait_cqe_nr` e depois chamava `peek_batch` novamente — desta vez a partir do **mesmo head** — gerando ponteiros duplicados.

```mermaid
flowchart TD
    subgraph "Versão BUGADA"
        B1["peek_batch(N)\nretorna got=3 < N=5"]
        B2["wait_cqe_nr(2)\nnovos CQEs disponíveis"]
        B3["peek_batch(N-3=2)\nLÊ DO MESMO HEAD!"]
        B4["cqes array:\n0=CQE_A  1=CQE_B  2=CQE_C\n3=CQE_A ⚠️  4=CQE_B ⚠️"]
        B5["sqe_ctx_free(CQE_A.ctx)\nsqe_ctx_free(CQE_A.ctx) ← DOUBLE FREE\n💥 SEGFAULT"]
        B1 --> B2 --> B3 --> B4 --> B5
    end

    subgraph "Versão CORRETA"
        C1["wait_cqe_nr(N=5)\naguarda TODOS disponíveis"]
        C2["peek_batch(N=5)\nUMA única leitura do head"]
        C3["cqes array:\n0=CQE_A  1=CQE_B  2=CQE_C\n3=CQE_D  4=CQE_E  ✓"]
        C4["cq_advance(5)\navança head uma vez\n✅ sem duplicatas"]
        C1 --> C2 --> C3 --> C4
    end

    style B4 fill:#fce4ec,stroke:#c62828
    style B5 fill:#fce4ec,stroke:#c62828
    style C3 fill:#e8f5e9,stroke:#2e7d32
    style C4 fill:#e8f5e9,stroke:#2e7d32
```

---

## 6. Diagrama Completo da Integração

```mermaid
flowchart TB
    subgraph CLIENT["Cliente AMQP"]
        PROD(["Produtor"])
    end

    subgraph BROKER["RabbitMQ Broker (Erlang/OTP)"]
        CH["rabbit_channel"]
        AQ["rabbit_amqqueue_process"]

        subgraph STORAGE["Camadas de Armazenamento"]
            IDX["Index v2\nring=Ring₁"]
            STO["Store v2\nring=Ring₂"]
            MST["Msg Store\nring=Ring₃"]
        end

        ADAPT["rabbit_io_uring\n(adaptador)"]
        NIF["io_uring NIF\n(C / liburing)"]
    end

    subgraph KERNEL["Linux Kernel"]
        SQ["SQ Ring"]
        CQ["CQ Ring"]
        VFS["VFS / Block Layer"]
    end

    DISK[("Disco\n.rdq  .qs  .idx")]

    PROD -- "AMQP publish persistent" --> CH
    CH -- "cast write" --> AQ
    AQ --> IDX & STO & MST

    IDX -- pwritev --> ADAPT
    STO -- "pwritev / preadv" --> ADAPT
    MST -- writev --> ADAPT

    ADAPT --> NIF
    NIF -- prep×N --> SQ
    NIF -- "submit\n(1 syscall)" --> SQ
    SQ --> VFS --> DISK
    DISK --> VFS --> CQ
    NIF -- wait_n_cqes --> CQ

    AQ -- "publisher confirm" --> CH
    CH -- "AMQP ack" --> PROD

    style IDX fill:#e3f2fd,stroke:#1565c0
    style STO fill:#e3f2fd,stroke:#1565c0
    style MST fill:#e3f2fd,stroke:#1565c0
    style ADAPT fill:#f3e5f5,stroke:#6a1b9a
    style NIF fill:#f3e5f5,stroke:#6a1b9a
    style SQ fill:#e8f5e9,stroke:#2e7d32
    style CQ fill:#e8f5e9,stroke:#2e7d32
    style DISK fill:#fce4ec,stroke:#880e4f
```

---

## 7. Configuração

A integração é **desabilitada por padrão** e ativada via `rabbitmq.conf`:

```ini
# Habilita io_uring no message store (Linux 5.1+)
message_store.io_uring = true

# Opcional: modo SQPOLL (Linux 5.12+ ou CAP_SYS_NICE)
# O kernel faz polling contínuo do SQ ring, eliminando io_uring_enter por lote
message_store.io_uring_sqpoll = false
```

Quando `message_store.io_uring = false` (padrão), nenhum código novo é executado nos hot paths — o pattern matching nos campos `ring = undefined` da struct de cada camada direciona tudo para o caminho original com `file:write`.
