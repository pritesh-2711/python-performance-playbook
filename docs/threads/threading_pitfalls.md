# Threading pitfalls (real HTTP + Postgres + RAG)

Threads are easy to start and hard to make stable at scale. The failures are predictable.

---

## 1) “More threads made it slower” — why it happened in the HTTP lab
You saw:
- Serial ~8 RPS
- Threads(2..32) got dramatically worse

This is classic queueing collapse:
- downstream (FastAPI server) couldn’t process requests concurrently fast enough
- client concurrency increased the arrival rate
- requests queued → latency exploded → throughput collapsed

Key point:
> Threads improve waiting time only if the downstream can service concurrency (or you enforce a limit).

---

## 2) Connection storms
### HTTP
If you open too many connections at once, you pay:
- TLS handshakes
- socket overhead
- server accept backlog pressure

Mitigation:
- reuse connections (keep-alive)
- cap concurrency
- configure client limits
- implement retries with jitter (carefully)

### Postgres
If every query makes a new DB connection (as in the simple lab), that becomes the bottleneck:
- authentication
- backend process overhead
- memory use per connection

Mitigation:
- connection pooling (pgbouncer / psycopg_pool / SQLAlchemy pool)
- reuse connections per worker
- cap workers below DB knee
- set Postgres `max_connections` sensibly

---

## 3) Tail latency (p95/p99) is the truth
Average latency is easy to improve by queueing.
What breaks systems is p99.

In the Postgres lab, QPS improved up to 4 threads but then p95/p99 rose and QPS dropped.

Rule:
> Tune threads by p95/p99 + throughput, not by mean latency.

---

## 4) Locks & shared state
Python-side locks:
- a “threadsafe” global dict or logger can become a bottleneck
- contention kills scaling quietly

DB-side locks (Lab 03):
- hot row updates force serialization (row lock)
- sharding reduces contention, but the current lab is still dominated by connection/commit overhead, so the difference looked smaller than expected

Mitigation:
- avoid hot rows for counters (shard, batch, or redesign)
- keep transactions short
- reuse connections and reduce per-op overhead so lock effects become visible

---

## 5) Unbounded queues = memory blow-ups
Pipelines need backpressure.
If producers > consumers and queues are unbounded:
- memory grows
- latency grows
- system eventually dies

Fix:
- bounded `queue.Queue(maxsize=...)`
- producer blocks (or drops) when queue is full
- treat queue size as a safety valve

the RAG pipeline uses bounded queues — that’s a production pattern.

---

## 6) Cancellation and shutdown
If you Ctrl+C and threads keep running:
- you leak DB connections
- you leave partial batches
- you corrupt invariants (maybe)

Fix pattern:
- `threading.Event` stop flag
- sentinel messages in queues
- timeouts on blocking operations
- careful “finish current batch then stop”

the Lab 05 is built to prove this pattern.

---

## 7) Measuring wrong things
Common benchmarking mistakes:
- not warming caches/models (the RAG run showed huge cold vs warm difference)
- mixing setup time with steady-state time
- ignoring system load and background tasks
- overfitting to one run

Fix:
- run multiple iterations
- separate cold-start from steady-state
- report p50/p95/p99 and throughput

Threads are not “fast”; they are “fast when the system lets them be fast”.
