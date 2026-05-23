# TensorIPC Architecture Manifesto

## Domain-Driven Design

The system is decomposed into two bounded contexts communicating exclusively
through a shared kernel object — no HTTP, no RPC, no message queue between
workers.

### Worker A — Writer (Write Domain)

```
POST /tensor → FastAPI → SharedMemory.buf[:] = payload
```

- Exposes an HTTP endpoint for external tensor ingestion.
- Owns the lifecycle of the POSIX shared-memory segment (creates, writes,
  unlinks on shutdown).
- Encodes a monotonically increasing counter in the first 4 bytes so the
  Reader can detect *every* update, even if the payload bytes are identical.

### Worker B — Reader (Read Domain)

```
while True: bytes(shm.buf) → compare → sleep(10ms)
```

- Attaches to the already-existing segment (`create=False`).
- Never unlinks — the Writer owns destruction.
- Uses **state-tracking** (previous snapshot vs current) to suppress
  duplicate-read noise.
- Graceful shutdown via `SIGTERM`/`SIGINT` handler — closes handle, logs
  poll summary.

### Gateway (Rate-Limiting Reverse Proxy)

- Strips the Writer from direct network exposure.
- Implements a **token-bucket rate limiter** (100 tokens, refill 100/s).
- Logs structured JSON on every request (`method`, `path`, `status`,
  `latency`, `rate_limited`).
- Returns **502 Bad Gateway** (not 503) with `degraded` header when Writer
  is unreachable — signals *degraded but alive* to upstream health checkers.

## Resilience Engineering

| Hazard | Mitigation |
|--------|-----------|
| Writer crash | Gateway returns 502 → client retries with backoff |
| Segment not yet created | Reader retries attach (20 attempts × 2 s) |
| Identical consecutive writes | Write counter in first 4 bytes ensures every POST produces a distinct state |
| Reader crash | `restart: always` in docker-compose |
| SIGTERM during poll | `_running` flag + signal handler → clean close() |
| Docker daemon restart | IPC namespace persists for the lifetime of the container |

## The Zero-Copy Mechanism

```
Writer (container A)                    Reader (container B)
        │                                       │
        │ shm_open("tensor_data")               │
        │ ftruncate(64)                         │
        │ mmap()                                │
        │  ↓                                    │
        │ ┌──────────────┐                      │
        │ │   /dev/shm   │ ← same physical pages │
        │ │  tensor_data │                      │
        │ └──────────────┘                      │
        │                    mmap() ─────────────┤
        │                    memcpy() ──────────┤
```

1. **Writer** calls `shm_open()` + `ftruncate()` + `mmap()` — creates a file
   descriptor in `/dev/shm` backed by anonymous pages.
2. **Docker IPC namespace** (`ipc:shareable` / `ipc:container:writer`) maps
   the Writer's `/dev/shm` into the Reader's mount namespace.
3. **Reader** calls `shm_open()` with the same name — the kernel returns a
   pointer to the *exact same physical pages*. No data is copied between
   containers.
4. Both sides `munmap()` / `close()` independently. The kernel frees pages
   only after the last handle is closed **and** `shm_unlink()` is called.

## Tradeoffs & Limitations

### 1. Single-Node Boundary

Shared memory cannot span physical machines. Scaling beyond one host
requires:
- **RDMA** (InfiniBand / RoCE) — remote direct memory access, kernel bypass.
- **eBPF** + `bpf_sk_lookup` — in-kernel packet steering without user-space
  context switches.

For single-node multi-container workloads (model ensembling, data
augmentation pipelines), TCP adds unnecessary overhead. For distributed
training, reach for NCCL/RCCL instead.

### 2. Gateway GC Pauses

The Go gateway runs a stop-the-world garbage collector. Under sustained 521
RPS load, GC may introduce latency jitter of up to 2-3 ms. If predictable
sub-millisecond tail latencies are required, rewrite the gateway in Rust
(no GC, deterministic `Box`/`Arc` lifetimes).

### 3. Idle Polling Loop

Worker B polls `/dev/shm` every 10 ms — approximately 0.2% of a single core
at idle. This is acceptable because:
- The alternative (eventfd / pipe / message queue) reintroduces a kernel
  context switch, negating the zero-copy win.
- The polling interval is tunable. Setting `POLL_INTERVAL_SEC = 0.1` drops
  CPU to ~0.02% at the cost of 100 ms detection latency.

If power efficiency is critical (e.g. battery-powered edge devices), replace
polling with a `inotify` watch on the `/dev/shm` directory — though this
adds a ~50 µs kernel notification path and loses the pure-mmap advantage.
