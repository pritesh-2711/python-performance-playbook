# Process Pool Tuning

There is no universal “best” configuration.

## Workers

- Upper bound: number of CPU cores
- Practical bound: cores minus OS overhead

### Evidence

From:

- `labs/multiprocessing/01_cpu_bound_processes_vs_threads.py`

Processes scale until:

- CPU saturates
- Memory bandwidth becomes a bottleneck

## Chunking

Chunking = grouping multiple items into one task.

### Two regimes (proven)

#### 1. Tiny tasks (overhead dominated)

- Chunking helps
- Fewer submissions
- Less IPC overhead

#### 2. Heavy / uneven tasks

- Chunking hurts
- Fewer tasks → stragglers dominate
- Parallelism drops

### Evidence

From:

- `labs/multiprocessing/05_process_pool_chunking_nlp.py`

You observed:

- chunk=1 best for heavy NLP
- large chunks degraded throughput

## Rule

> Chunking is a tuning knob, not a rule.

Measure. Don’t guess.
