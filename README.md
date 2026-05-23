# TensorIPC — Zero-Copy AI Memory Bridge

![CI/CD](https://github.com/adithya-313/Tensor-IPC/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue)
![Go](https://img.shields.io/badge/Go-1.21-blue)
![Python](https://img.shields.io/badge/Python-3.11-blue)

**TensorIPC** is a production-grade zero-copy inter-process communication
cluster that bypasses the TCP/IP network stack by routing all data through
the Linux kernel's POSIX shared-memory subsystem (`/dev/shm`) via Docker's
IPC namespace.

### Why not TCP?

Every byte transferred through TCP must traverse the kernel's network stack:
system calls, socket buffers, context switches, checksum offload. In a
tightly coupled AI pipeline (e.g. inference server → preprocessor →
postprocessor), these overheads dominate latency. Docker IPC namespace lets
containers share the same `/dev/shm` — the Reader `mmap`s the exact physical
memory pages the Writer wrote, with **zero copies** and **nanosecond-latency**
reads.

## Quickstart

```bash
# Build and start all services
docker compose up -d --build

# Verify the stack is healthy
curl -s http://localhost:8000/health

# Run the 50-VU load test (10 seconds, 521 req/s sustained)
./validate_phase3.sh

# Tear down (clears all IPC kernel objects)
docker compose down --volumes
```

## Project Structure

```
tensoripc/
├── .github/workflows/ci.yml   # CI/CD (push/PR → build → test → validate)
├── docker-compose.yml         # All services: writer, reader, gateway, k6
├── gateway/                   # Phase 2: Go reverse proxy + rate limiter
├── worker_a/                  # Phase 1+3: FastAPI HTTP → SharedMemory writer
├── worker_b/                  # Phase 1+4: Continuous polling reader daemon
├── load_test/                 # Phase 3: k6 benchmark script (50 VUs)
├── ARCHITECTURE.md            # DDD, resilience, zero-copy tradeoffs
├── validate_phase3.sh         # Self-validation: build → test → scrape logs
└── validate_phase4.sh         # Phase 4 CI-style validation loop
```

## Phases

| Phase | What | Key Result |
|-------|------|-----------|
| 1 | Kernel-level SHM cluster via Docker IPC | 64-byte read, VALIDATION PASSED |
| 2 | Distroless Go gateway (rate-limited) | `/health` 200, graceful degradation |
| 3 | FastAPI HTTP-to-IPC bridge + k6 | 1792 req, 100%, 521 RPS |
| 4 | CI/CD + continuous polling daemon | 360/360 payloads detected |
