# When threads win (and when they lose) — with real HTTP + Postgres + RAG ingestion for examples

Threads shine when the program spends time **waiting** on external systems:
- HTTP APIs (network I/O, TLS, server processing)
- PostgreSQL (network + server execution + disk/cache)
- Embedding server calls (Ollama HTTP)
- Filesystems (disk I/O)

Threads usually **do not** help when the bottleneck is:
- CPU-bound Python bytecode (GIL)
- A single downstream bottleneck that can’t serve requests concurrently
- Lock contention on shared resources
- Connection storms (creating too many TCP/DB connections)

This repo demonstrates all of these using **real HTTP + real Postgres**.

---

## Lab 01: Real HTTP fan-out (FastAPI, cached vs overloaded)

This lab benchmarks a real HTTP service running locally via FastAPI.  
The endpoint performs a binary search over a large sorted array and supports two modes:

- **mem**: data is loaded once at startup and served from memory (realistic, production-style)
- **disk**: data is reloaded and parsed on every request (intentionally pathological)

### Results (cached / `mode=mem`)
On a warmed server and client, the cached path shows a clear “threads help, then hurt” curve:

- **Serial**: ~590 RPS
- **Threads(4)**: ~1325 RPS (**~2.2× speedup**, best point)
- **Threads(8+)**: throughput drops and p95/p99 latency grows rapidly

This is the expected behavior of a healthy I/O-bound HTTP workload:
threads overlap request/response waits and improve throughput **until**
CPU contention, kernel scheduling, socket contention, and server-side limits dominate.

### Why throughput drops after the knee
Adding threads beyond the service’s capacity does not create more work being done.
Instead it increases:
- request queueing inside the server
- lock and scheduling contention
- tail latency (p95/p99)

The result is lower effective throughput even though more client threads are active.

### Key takeaway
> Threads improve HTTP throughput **only up to the downstream system’s capacity**.
Beyond that point, more concurrency increases latency and reduces throughput.

This is why real systems cap concurrency and tune for the knee, not for maximum threads.

### The pathological case (`mode=disk`)
When the server reloads and reparses data on every request, even low concurrency
causes queueing and tail latency explosions. In this mode, increasing client threads
makes performance dramatically worse.

This demonstrates an important rule:
> Client-side threading cannot compensate for inefficient server-side work.

---

## Lab 02: Real PostgreSQL SELECT+JOIN fan-out (no mocks)

### the results (2000 queries)
- Serial: **~82 QPS**, p95 ~15ms
- Threads(4): **~126 QPS** (best), p95 ~9.5ms
- Threads(8): slightly worse
- Threads(16/32): worse (tail latency rises, QPS drops)

### Interpretation
Threads helped because DB calls are I/O waits, but you hit real limits:
- connection overhead (the lab does one connection per query)
- DB CPU / lock / buffer cache / planner overhead
- kernel/network overhead
- `max_connections` and overall Postgres contention

So you saw a clean “knee” at ~4 workers.

> For DB workloads, thread wins exist, but there’s nearly always a sweet spot. Past that, tail latency climbs and throughput drops.

---

## Lab 04: RAG ingestion pipeline (PDF → chunk → Ollama embed → pgvector)

### the results (same PDF, 40 chunks)
- First run: **21.42s**
- Second run: **0.70s**
- Failures logged in `ppb_rag_failures` (none surfaced in stdout)

### Interpretation
That massive improvement is expected:
- Ollama model warm-up / first-time load dominates the first run
- subsequent runs benefit from model already resident + cached state
- also likely DB cache effects

This is a real production phenomenon:
- first request latency matters (cold start)
- steady-state throughput matters (warm)

Threads are useful here because embedding calls are **HTTP I/O** to a local service; concurrency improves throughput *until* Ollama becomes the bottleneck.

---

## Quick decision rules (practical)
Use threads when:
- the work is mostly waiting on HTTP/DB/disk
- downstream can handle concurrent requests (or you apply backpressure)
- you need compatibility with blocking libraries (psycopg sync, many SDKs)

Avoid threads (or cap them) when:
- you see p95/p99 exploding as workers increase
- you’re creating too many connections (HTTP/DB)
- you have shared locks / global mutable state
- the server side is single-worker/single-process

Threads are a tool. The system decides whether they help.
