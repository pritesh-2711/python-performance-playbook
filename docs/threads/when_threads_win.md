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

## Lab 01: Real HTTP fan-out (FastAPI binary search service)

### the results (200 req, repeat=3)
- Serial: **~7.96 RPS**, p50 ~125ms
- Threads(2..32): **RPS collapsed** and tail latency exploded (p99 up to seconds)

### Interpretation (tell-it-like-it-is)
This is a **downstream bottleneck + queueing** story.

The FastAPI endpoint does a heavy server-side workload per request:
- reads a large file (`sorted_ints.txt`) every request (disk I/O)
- parses it into Python ints (CPU + allocations)
- then binary-searches

When the client increases concurrency, the server can’t actually process requests in parallel fast enough.
Requests queue up, latency grows, and the benchmark computes total time as the sum of individual request latencies — so throughput collapses.

So this lab is still valuable, but it demonstrates the real rule:

> Client-side threads do not create throughput if the server is the bottleneck. They can make it worse by increasing queueing and tail latency.

This is exactly what production load testing looks like: you don’t judge threads by “more workers is faster”; you find the system’s knee.

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
