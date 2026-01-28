# D2 — asyncpg pool sizing: fan-out vs tail latency

In async DB-heavy services, throughput is often dominated by:

- query cost
- DB capacity
- pool size (max connections)
- client-side concurrency (tasks waiting for connections)

This lab shows how **concurrency >> pool_max** turns into **queueing**, which shows up as tail latency.

## Run :

Lab: `labs/async/02_asyncpg_pool_fanout.py`

What it runs:
- task concurrency: [1, 4, 8, 16, 32, 64]
- pool max size: [4, 8, 16, 32]
- queries=3000 repeat=3 (9000 total per row)

Sample of my output :

- pool max = 4
  - c=1: mean ~0.07ms, p99 ~0.17
  - c=16: mean ~0.68ms, p99 ~6.44
  - c=64: mean ~2.72ms, p99 ~15.35

- pool max = 16
  - c=16: best-ish throughput + stable tails
  - c=64: p99 jumps (queueing + downstream contention)

## Interpretation

### 1) If concurrency > pool_max
Tasks queue waiting for a free connection.
- throughput may stay flat
- p95/p99 rises fast
- this is client-side queueing, not “slow DB”

### 2) If pool_max is too large
You can overload the DB:
- more concurrent queries = more DB contention
- tail latency can degrade
- sometimes throughput *drops* when DB saturates

## Production guidance

### Latency-critical APIs
- Keep concurrency close to pool size.
- Prefer small pool sizes that keep p99 stable.
- Measure on real DB hardware; this varies wildly.

### Throughput batch workloads
- Increase pool size only if DB can actually handle it.
- Batch writes, minimize round trips, avoid chatty patterns.

## Common anti-patterns

- “We increased concurrency and it got slower”: you created queueing.
- “Let’s increase pool_max to 100”: you often just create DB contention and worse p99.
- “Connections are cheap”: they aren’t; each consumes DB memory/CPU and scheduling.

## Practical checklist

- pool_max usually starts near:
  - (number of DB CPU cores) or (max acceptable concurrent queries)
- cap task concurrency (Semaphore) even if you have a pool
- measure:
  - QPS
  - p95/p99 latency
  - DB CPU, locks, buffers, active connections
