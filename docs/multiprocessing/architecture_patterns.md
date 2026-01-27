# Production Multiprocessing Patterns

Most real systems look like this:

CPU-bound work → I/O-bound sink

## Anti-pattern

- Every worker writes to DB
- Per-row commits
- Unbounded memory growth

## Correct pattern

- CPU workers compute only
- Results sent via bounded queue
- Single writer batches I/O

## Lab evidence

From:

- `labs/multiprocessing/06_pipeline_cpu_procs_to_postgres_writer.py`

### Naive (8 writers, commit per row)

- ~1,985 items/s

### Writer pattern (batched)

- ~5,913 items/s
- ~3× faster
- Stable under load

## Why this works

- Centralized I/O
- Fewer DB connections
- WAL + index cost amortized
- Backpressure prevents OOM

## Rule

> Parallelize computation, serialize side effects.
