# Syscall Count Methodology

This document explains exactly how the syscall counts reported in the paper's
Table~\ref{tab:syscalls} were obtained, distinguishing between values that were
**directly measured** with `strace` and values that were **analytically derived**
from those measurements.

---

## Summary of results

| Configuration   | pwrite64 | writev | io_uring_enter | Total |
|-----------------|----------|--------|----------------|-------|
| Baseline (POSIX) | 400     | 1      | —              | 401   |
| io_uring (batch) | 0       | 0      | 3              | 3     |
| io_uring (SQPOLL)| 0       | 0      | 0              | 0     |

Workload: single queue, confirm batch B = 200 messages, 1 KB messages,
`--confirm 200` (same as the throughput benchmark).

---

## What was directly measured

### Measurement 1 — `file:pwrite/2` with a list of N offset–data pairs

**Goal:** confirm that each `{Offset, Binary}` entry in the list produces an
independent `pwrite64(2)` syscall.

**Command:**

```bash
strace -f -e trace=pwrite64,write,writev,openat \
  erl -noshell -eval '
    {ok, Fd} = file:open("/tmp/test_pwrite4.bin", [write, binary, raw]),
    LocBytes = [{I * 64, <<I:8, 0:(8*63)>>} || I <- lists:seq(0, 9)],
    ok = file:pwrite(Fd, LocBytes),
    file:close(Fd),
    halt(0).'
```

The `-f` flag is required because Erlang's raw-file driver executes in a
separate OS thread; without it strace only sees the scheduler threads.

**Result:**

```
31388 openat(AT_FDCWD, "/tmp/test_pwrite4.bin", O_WRONLY|O_CREAT|O_TRUNC, 0666) = 17
31388 pwrite64(17, "\0...", 64,   0) = 64
31388 pwrite64(17, "\1...", 64,  64) = 64
31388 pwrite64(17, "\2...", 64, 128) = 64
...  (10 lines total)
31388 pwrite64(17, "\9...", 64, 576) = 64
```

**Conclusion:** `file:pwrite(Fd, LocBytes)` with a list of N entries produces
**exactly N `pwrite64` syscalls** — one per entry, in order.

---

### Measurement 2 — `file:write/2` with an I/O vector of M binaries

**Goal:** confirm how many syscalls `file:write` produces when given an iolist
of multiple binaries (as returned by `prim_buffer:read_iovec/2` in
`rabbit_msg_store`).

**Command:**

```bash
strace -f -e trace=pwrite64,write,writev,openat \
  erl -noshell -eval '
    {ok, Fd} = file:open("/tmp/twi.bin", [write, binary, raw]),
    IoVec = [<<I:8, 0:504>> || I <- lists:seq(0, 49)],
    ok = file:write(Fd, IoVec),
    file:close(Fd),
    halt(0).'
```

**Result:**

```
31616 openat(AT_FDCWD, "/tmp/twi.bin", O_WRONLY|O_CREAT|O_TRUNC, 0666) = 17
31616 writev(17, [{iov_base="...", iov_len=3200}], 1 <unfinished ...>
```

50 binaries × 64 bytes = 3200 bytes, submitted as a single `writev` with one
scatter/gather entry.

**Conclusion:** `file:write(Fd, IoVec)` with an iolist of any length produces
**exactly 1 `writev` syscall** — Erlang's raw-file driver coalesces the
io-vector before issuing the kernel call.

---

## What was analytically derived

The table values combine the per-call measurements above with the batch size
B = 200 and the code paths in each module.

### Baseline (POSIX)

**`rabbit_classic_queue_store_v2`** — flush path
([`deps/rabbit/src/rabbit_classic_queue_store_v2.erl`](deps/rabbit/src/rabbit_classic_queue_store_v2.erl),
around line 266):

```erlang
ok = file:pwrite(Fd, lists:sort(LocBytes)),
```

`LocBytes` contains one `{Offset, Data}` pair per message in the flush batch.
With B = 200 messages → **200 `pwrite64` calls**.

**`rabbit_classic_queue_index_v2`** — flush path
([`deps/rabbit/src/rabbit_classic_queue_index_v2.erl`](deps/rabbit/src/rabbit_classic_queue_index_v2.erl),
around line 626):

```erlang
ok = file:pwrite(Fd, LocBytes),
```

Same pattern, one entry per index record per message → **200 `pwrite64` calls**.

**`rabbit_msg_store`** — writer flush path
([`deps/rabbit/src/rabbit_msg_store.erl`](deps/rabbit/src/rabbit_msg_store.erl),
around line 1485):

```erlang
{file:write(Fd, prim_buffer:read_iovec(Buffer, Size)), W}
```

The entire buffer (all messages accumulated since last flush) is written as a
single iolist call → **1 `writev` call** (Measurement 2 above).

**Total baseline:** 200 + 200 + 1 = **401 syscalls**.

---

### io_uring (batch)

Each module's flush path is redirected to `rabbit_io_uring` when a ring is
available:

- `rabbit_classic_queue_store_v2` calls `rabbit_io_uring:pwritev/3`, which
  encodes all N `{Offset, Binary}` pairs as SQEs and submits them with a
  single `io_uring_enter` call
  ([`deps/rabbit/src/rabbit_io_uring.erl`](deps/rabbit/src/rabbit_io_uring.erl),
  lines 172–187).
- `rabbit_classic_queue_index_v2` does the same.
- `rabbit_msg_store` calls `rabbit_io_uring:writev/4`, which encodes the
  iolist binaries as SQEs and submits with one `io_uring_enter`
  (lines 146–170).

Three modules × 1 `io_uring_enter` per module = **3 `io_uring_enter` calls**.
No `pwrite64` or `writev` calls are issued.

**Total io_uring:** **3 syscalls**.

---

### io_uring (SQPOLL)

When the ring is created with the `sqpoll` flag
(`message_store.io_uring_sqpoll = true`), the kernel spawns a polling thread
that continuously watches the submission ring. The application never calls
`io_uring_enter` to notify the kernel — the SQEs are picked up automatically.

**Total SQPOLL:** **0 syscalls**.

---

## What was NOT measured (and why)

The full RabbitMQ broker could not be traced end-to-end with `strace` during
this experiment because the broker fails to start on Erlang/OTP 29 due to an
incompatibility between `horus 0.4.0` (the anonymous-function extraction
library used by Khepri) and new BEAM instructions emitted by the OTP 29
compiler. Specifically, `khepri_tx_adv:ensure_instruction_is_permitted/1`
rejects the `{line, [{location, [], LineNum}]}` instruction format introduced
in OTP 29, causing the broker to crash during feature-flag migration at
startup.

The throughput benchmarks in `bench-results/` were collected on OTP 27 (the
target runtime per `CONTRIBUTING.md`), where the broker starts cleanly.

The syscall counts in the table are therefore derived, not directly observed
from a running broker. However, they rest on two empirically verified facts:

1. `file:pwrite(Fd, ListOfN)` → N `pwrite64` calls (Measurement 1, above)
2. `file:write(Fd, IoVec)` → 1 `writev` call (Measurement 2, above)

combined with a straightforward reading of the flush-path source code, which
makes the derivation unambiguous.

---

## Reproducing the strace measurements

Requirements: Erlang/OTP installed, `strace` ≥ 5.x.

```bash
# Measurement 1: file:pwrite with N entries
strace -f -e trace=pwrite64,writev -o /tmp/pwrite_trace.txt \
  erl -noshell -eval '
    {ok, Fd} = file:open("/tmp/bench_pwrite.bin", [write, binary, raw]),
    N = 10,
    LocBytes = [{I * 64, <<I:8, 0:(8*63)>>} || I <- lists:seq(0, N-1)],
    ok = file:pwrite(Fd, LocBytes),
    file:close(Fd),
    halt(0).'

# Count pwrite64 calls on the target file's fd:
FD=$(grep "bench_pwrite.bin" /tmp/pwrite_trace.txt | grep -oP '= \K[0-9]+' | head -1)
grep "pwrite64($FD" /tmp/pwrite_trace.txt | wc -l
# Expected: 10

# Measurement 2: file:write with M-binary iolist
strace -f -e trace=pwrite64,writev -o /tmp/write_trace.txt \
  erl -noshell -eval '
    {ok, Fd} = file:open("/tmp/bench_write.bin", [write, binary, raw]),
    M = 50,
    IoVec = [<<I:8, 0:504>> || I <- lists:seq(0, M-1)],
    ok = file:write(Fd, IoVec),
    file:close(Fd),
    halt(0).'

FD=$(grep "bench_write.bin" /tmp/write_trace.txt | grep -oP '= \K[0-9]+' | head -1)
grep "writev($FD" /tmp/write_trace.txt | wc -l
# Expected: 1
```
