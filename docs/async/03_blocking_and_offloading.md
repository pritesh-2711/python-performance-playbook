# D3 — Blocking calls in async, and how to offload correctly

This chapter covers two distinct production realities:

1) **Blocking I/O inside async** kills the event loop. Fix: `asyncio.to_thread()` (or switch to async client).
2) **CPU-bound Python inside async** blocks the loop too. Fix: process pool (not threads).

## D3A — Blocking sync HTTP inside async vs to_thread

### Run :
Lab: `labs/async/03a_blocking_http_in_async_vs_to_thread.py`

D3A-bad sync HTTP in async c=32
Wall 12.850s, 500 req, 38.9 RPS, mean 25.5ms

D3A-good to_thread sync HTTP c=32 tcap=32
Wall 1.134s, 500 req, 440.7 RPS, mean 62.1ms


### Interpretation

- BAD: `requests.get()` runs inside coroutine → blocks the single event-loop thread.
  Result: the async scheduler cannot make progress; concurrency is mostly fake.
- GOOD: `to_thread()` moves the blocking call out of the loop thread.
  Result: your loop stays responsive and can overlap I/O.

Why mean latency is higher in the “good” run:

- we pushed ~11× more throughput → server becomes the bottleneck → queueing shifts downstream.
That’s expected. The key win is: we removed the *client-side event-loop bottleneck*.

### Rule :

Use `to_thread()` for **blocking I/O libraries** when you cannot use async-native versions:

- sync HTTP clients
- sync SDKs
- sync filesystem/network calls
- some DB drivers (but prefer async drivers)

Always cap it:

- `Semaphore` or a bounded worker pool
- otherwise you melt the machine and destroy p99

## D3B — CPU in async: blocking vs ProcessPool

### Run :
Lab: `labs/async/03b_cpu_in_async_blocking_vs_processpool.py`

D3B-bad CPU in loop c=32
Wall 4.622s, 400 jobs, 86.5/s

D3B-good processpool c=32 procs=8
Wall 0.708s, 400 jobs, 565.1/s


### Interpretation

- BAD: CPU work runs on the event-loop thread → requests serialize → throughput collapses as you add “concurrency”.
- GOOD: process pool gives real parallelism across cores → throughput jumps.

Why mean latency is higher in the “good” run:

- each job waits for an executor slot + IPC overhead exists
- but overall throughput is massively better

### Rule :

For CPU-bound Python:

- **do NOT use threads expecting speedup**
- use **processes** (ProcessPool / multiprocessing / separate workers)

## Practical decision rules (async services)

If code inside request handler is:

- **blocking I/O** → use async-native client OR `to_thread` (bounded)
- **CPU-bound Python** → process pool or external worker system (Celery/RQ/etc.)
- **mixed** (I/O + CPU) → async for I/O, process pool for CPU stage, and add backpressure between stages

## Anti-patterns

- “My FastAPI is async, so it’s fast”: not if you block the loop.
- Unbounded `to_thread` fanout: destroys tail latency and can crash the box.
- CPU heavy parsing/tokenization inside async handler: move it out.
