# Tuning thread pools (realistic rules)

The goal is not “maximum threads”.
The goal is “maximum throughput at acceptable p95/p99”.

You already saw why:
- HTTP lab collapsed with concurrency (queueing + bottleneck)
- Postgres lab had a sweet spot around 4 workers

---

## 1) The tuning workflow (repeatable)
For a fixed workload:
1) pick worker values (1,2,4,8,16,32…)
2) measure:
   - throughput (RPS/QPS)
   - p50/p95/p99 latency
3) pick the “knee”:
   - throughput stops improving
   - p95/p99 starts rising sharply

That knee is the production starting point.

---

## 2) What to do when throughput drops as threads increase (HTTP case)
This means downstream is saturated and queueing dominates.

Actions:
- reduce client workers to below the knee
- add server-side concurrency (uvicorn workers / async handlers / caching)
- implement server backpressure (limit concurrency + reject excess rather than queueing forever)
- make the endpoint less expensive (don’t reload huge files per request)

Key idea:
> Concurrency without capacity creates queueing, not performance.

---

## 3) Postgres: where the knee comes from
You hit a knee because you were doing “one connection per query” and the DB has finite capacity.

Actions that change the knee:
- connection pooling (pgbouncer, psycopg_pool)
- reuse a connection per worker
- batch work
- reduce query cost (indexes, better plans)
- reduce transaction overhead

Practical cap:
- start with workers in the single digits for local Postgres
- go higher only if you have pooling + the DB has headroom

---

## 4) Pipeline tuning (RAG ingestion)
the pipeline has three main limits:
1) PDF extraction (CPU + IO)
2) embedding service (Ollama) throughput
3) DB insert throughput

Knobs:
- `embed_workers`: increases embedding concurrency until Ollama saturates
- `queue_max`: controls memory + allows smoothing bursts
- `db_batch`: increases insert efficiency until batches become too large (memory/latency tradeoff)

Suggested procedure:
- first run once to warm Ollama (cold start is misleading)
- measure steady-state with the same PDF multiple times
- increase `embed_workers` until time no longer improves
- increase `db_batch` until time no longer improves
- keep `queue_max` moderate (100–500) to prevent RAM spikes

---

## 5) Safety limits you should enforce
Even if performance looks great, stability matters.

Hard caps:
- max threads for HTTP fan-out (avoid request storms)
- max concurrent DB queries (avoid `max_connections` pressure)
- bounded queues between stages

If you can’t bound it, it’s not production-safe.

---

## 6) What to log while tuning
Minimum:
- throughput
- p50/p95/p99
- error rate (HTTP status / DB errors)
- queue sizes (for pipelines)

The tuning goal is stable curves, not single-run hero numbers.
