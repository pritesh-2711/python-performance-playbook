# Multiprocessing Decision Tree (Production)

This decision tree helps you choose the right concurrency model and multiprocessing pattern based on:

- workload type (CPU vs I/O)
- payload size (IPC tax)
- latency vs throughput
- memory constraints
- sink type (DB / filesystem / network)

---

## Step 0 — Are you sure you need multiprocessing?

### Is the workload CPU-bound?

Examples:
- tokenization / feature hashing / parsing
- OCR / image filters / PDF rendering
- compression / checksums
- embedding generation (CPU model)
- heavy Python loops

If YES → go to Step 1  
If NO (I/O-bound) → prefer **async** or **threads** (multiprocessing usually adds overhead)

**Quick sanity check**
- If your wall-time barely changes when you increase workers, you're not CPU-bound anymore (or you hit another bottleneck).

---

## Step 1 — Does the work release the GIL?

If work is:
- pure Python loops → **GIL blocks threads** → multiprocessing helps
- NumPy / OpenCV / PyTorch (CPU ops) → may release GIL → threads can sometimes scale too

If threads don't scale → multiprocessing  
If threads scale because C-extensions release GIL → threads may be simpler

---

## Step 2 — What is the payload shape?

### Are you sending big objects between processes?

Examples:
- large strings (documents)
- numpy arrays (images, embeddings)
- big Python objects

If YES → IPC/pickle tax will dominate. Prefer:
- **paths/IDs** across process boundary
- **shared_memory** for arrays
- avoid sending full payloads

If NO (small messages) → ProcessPoolExecutor style map is fine

---

## Step 3 — What is the sink?

### Does each task write to a shared sink?

Examples:
- Postgres inserts/upserts
- S3 uploads
- Kafka producer
- filesystem writes in same directory

If YES → do NOT let all workers write directly.
Use: **CPU workers → single writer (batched)** + bounded queue (backpressure).

If NO (pure compute returning small results) → use process pool map/reduce.

---

## Step 4 — Latency vs throughput?

### Latency-critical (p99 matters)
Use:
- more, smaller tasks (avoid huge chunks)
- bounded queue sizes
- fewer copies, minimal IPC payload

### Throughput-critical (jobs/sec)
Use:
- batching where safe (chunking / writer batch)
- shared memory for large arrays
- amortize I/O overhead

---

## The actual decision tree

Start here:

1) **CPU-bound?**
- No → threads/async
- Yes → (2)

2) **Threads scale?**
- Yes (C extensions release GIL) → threads may be enough
- No → multiprocessing (3)

3) **Payload > few MB?**
- Yes → send paths/IDs OR shared memory
- No → process pool map works (4)

4) **Shared sink?**
- Yes → CPU workers → single writer (batched) + bounded queue
- No → pool map/reduce

5) **Task size variance high?**
- Yes → avoid big chunks; keep tasks small; dynamic scheduling
- No → chunking can improve throughput (measure)

---

## Pattern mapping to labs

- Pool map CPU tasks: `01_cpu_bound_processes_vs_threads.py`
- IPC tax paths vs payloads: `02_pickle_ipc_tax_paths_vs_payloads.py`
- Start methods: `03_start_method_fork_vs_spawn_vs_forkserver.py`
- Shared memory arrays: `04_shared_memory_doc_images.py`
- Chunking: `05_process_pool_chunking_nlp.py`
- CPU→DB writer pipeline: `06_pipeline_cpu_procs_to_postgres_writer.py`
