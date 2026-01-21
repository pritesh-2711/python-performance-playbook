# Core Models for Python Concurrency (Threads, Processes, Async)

This chapter is the baseline for everything else in this repo.
If you skip it, you’ll misuse concurrency later.

---

## 1) Concurrency vs Parallelism vs Async

### Concurrency
**Making progress on multiple tasks by interleaving.**
- Example: you "juggle" many network calls by switching between them while waiting.

### Parallelism
**Running multiple tasks at the same time.**
- Example: CPU-heavy tasks running simultaneously on multiple cores.

### Async (async/await)
A **single-threaded concurrency model** (by default).
- It interleaves tasks on **one thread** using an **event loop**.
- It only works well when tasks frequently **await** (i.e., release control).

---

## 2) The GIL (Global Interpreter Lock): what it blocks vs what it doesn’t

### What it blocks
In **CPython**, only one thread can execute Python bytecode at a time (the GIL).
So **threads do not speed up CPU-bound pure Python code**.

### What it doesn’t block (in practice)
Threads still help when your code spends time:
- waiting on **I/O** (network, disk, DB)
- inside C extensions that **release the GIL** (some NumPy ops, some compression libs, etc.)

**Rule of thumb**
- CPU-bound pure Python → **multiprocessing**
- I/O-bound → **threads or async**
- Mixed pipelines → **hybrid** (async + threads + processes)

---

## 3) Scheduling models: OS threads vs process scheduler vs event loop

### OS threads
- Scheduled by the OS (preemptive scheduling)
- Can overlap on I/O waits
- In CPython: GIL prevents true CPU parallelism for Python bytecode

### Processes
- Scheduled by OS as separate processes
- Each has its own Python interpreter + its own GIL
- Real CPU parallelism
- Overhead: process startup, IPC, serialization, memory duplication

### Event loop (asyncio)
- Cooperative scheduling
- Tasks run until they hit an `await`, then yield control back to the loop
- Extremely efficient for high I/O concurrency
- Falls apart if you block the loop (sync sleep, CPU work, blocking libs)

---

## 4) Costs you must internalize

### Context switching cost
Switching between threads/processes has overhead.
- Threads: cheaper than processes, but still overhead.
- Processes: heavier (scheduler + memory + IPC).

### Syscalls
Real I/O usually means syscalls.
- You can hide syscall latency by overlapping I/O (threads/async).
- You cannot "async away" CPU time.

### I/O wait
Most production services are I/O bound:
- network calls
- DB queries
- disk reads/writes
Concurrency wins big here.

### Backpressure
If producers create work faster than consumers can handle,
your system collapses via:
- memory blow-up (unbounded queues)
- latency explosion (queues grow)
- timeouts cascading

Backpressure is *the* concurrency concept that separates toys from production.

---

## 5) Batch processing vs streaming

### Batch
- You process a large finite set (e.g., 10M rows).
- Optimization targets: throughput, resource utilization, chunking, memory footprint.

### Streaming
- Infinite/ongoing work (events, messages, web traffic).
- Optimization targets: stability, backpressure, bounded queues, predictable p95/p99 latency.

Most real systems are streaming systems pretending to be batch.

---

## 6) Demos (run & understand these before anything else)


### Demo 1: Threads help I/O wait
`labs/core-models/01_io_wait_threads_vs_serial.py`

### Demo 2: Threads don’t speed up CPU-bound Python (GIL)
`labs/core-models/02_cpu_bound_gil_threads_vs_processes.py`

### Demo 3: Blocking kills async
`labs/core-models/03_async_event_loop_blocking_vs_await.py`

### Demo 4: Backpressure (bounded queue vs unbounded queue)
`labs/core-models/04_backpressure_queue_bounded_vs_unbounded.py`

### Demo 5: Diminishing returns (thread overhead)
`labs/core-models/05_thread_overhead_diminishing_returns.py`

---

## 7) Diagram: what runs where

```mermaid
flowchart LR
  A[Your Python Code] --> B{Work Type?}
  B -->|I/O-bound| C[Threads or Async]
  B -->|CPU-bound Python| D[Multiprocessing]
  B -->|Mixed pipeline| E[Hybrid: async + threads + processes]
  C --> F[Backpressure limits: semaphores/queues]
  D --> G[Beware IPC + serialization + memory duplication]
  E --> H[Bounded queues between stages]

---

## Results

**Machine COnfiguration used in these experiments**: 
OS : Linux, CPython 3.12
Cores : 32 cores

| Demo | Key result | Why it matters |
|---|---|---|
| 01 I/O wait (50×0.2s) | Serial 10.008s vs Threads(20) 0.607s | Threads overlap I/O wait |
| 02 CPU-bound (8 tasks) | Threads 11.545s vs Processes 1.212s | GIL blocks CPU scaling with threads |
| 03 Async | `await` version 0.201s vs blocking 10.011s | Blocking the loop destroys async concurrency |
| 04 Backpressure | Unbounded backlog 3600 vs bounded backlog 200 | Bounded queues prevent memory/latency blowups |
| 05 Thread overhead | Speedup continues up to 64 threads here | Gains plateau/degrade depends on workload + OS; don’t assume linear scaling |


> Results vary by machine load, CPU governor, Python version, and workload characteristics. The shape/trend is what matters.

---

