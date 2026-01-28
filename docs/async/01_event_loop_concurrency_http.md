# D1 — Event loop concurrency with real HTTP

This lab shows the real tradeoff in async HTTP fan-out:

- Increasing concurrency can raise throughput (RPS)
- But once the upstream/server saturates, tail latency (p95/p99) can explode

## Run:

Lab: `labs/async/01_event_loop_concurrency_http.py`  
Target: local FastAPI `http://127.0.0.1:8008/search`

Results:

Requests: 500 | repeat: 3
Concurrency sweep: [1, 2, 4, 8, 16, 32]

D1-http async c=1 Wall 45.363s RPS 33.1 mean 30.16ms p95 33.67 p99 34.70
D1-http async c=2 Wall 22.929s RPS 65.4 mean 30.41ms p95 33.91 p99 34.68
D1-http async c=4 Wall 13.263s RPS 113.1 mean 35.02ms p95 45.53 p99 51.14
D1-http async c=8 Wall 9.848s RPS 152.3 mean 51.65ms p95 77.54 p99 88.09
D1-http async c=16 Wall 7.324s RPS 204.8 mean 75.70ms p95 123.21 p99 144.35
D1-http async c=32 Wall 6.024s RPS 249.0 mean 122.83ms p95 191.75 p99 220.03


## Interpretation

- From c=1 → c=2, throughput roughly doubles with almost no latency change.
- As concurrency increases further, throughput keeps climbing, but p95/p99 degrade sharply.

This is classic queueing:
- While there is spare capacity, concurrency overlaps I/O wait.
- Once you hit the server’s capacity, extra concurrency mostly adds queue depth.
- Queue depth ⇒ longer waits ⇒ p95/p99 blow up.

## How to choose concurrency (production rule)

Pick concurrency based on the system goal:

### Latency-critical services (p99 matters)
- Stop increasing concurrency when p95/p99 breaches your SLO.
- The best concurrency is usually *before* max throughput.

### Throughput-critical batch jobs
- You can push concurrency higher as long as:
  - errors/timeouts don’t rise
  - downstream doesn’t collapse
  - you have backpressure limits (see D4 later)

## Practical notes

- Always cap concurrency using a `Semaphore` (or pool limits).
- If you need both high throughput and good p99, you usually need:
  - faster downstream (DB/index/caching)
  - load shedding/backpressure
  - horizontal scaling
