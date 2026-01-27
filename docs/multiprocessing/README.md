# Multiprocessing in Real Python Systems

This section explains **how and why Python systems use multiprocessing in production**.

This is not a “how to use ProcessPoolExecutor” tutorial.
It is a **systems-level guide** backed by runnable labs and measured evidence.

## When you should use multiprocessing

Use multiprocessing when:

- Your workload is **CPU-bound**
- Threads do not scale (GIL)
- Async cannot help (pure computation)
- You need **true parallelism**

Common examples:

- NLP preprocessing
- Feature extraction
- Image processing
- Compression / parsing
- Embedding generation
- Offline batch pipelines

## Learning path (recommended)

1. **Why processes beat threads**
   - `labs/multiprocessing/01_cpu_bound_processes_vs_threads.py`
   - Shows GIL limits and true parallelism

2. **IPC and serialization tax**
   - `labs/multiprocessing/02_pickle_ipc_tax_paths_vs_payloads.py`
   - Shows why *what you send* matters more than worker count

3. **Start methods (Linux)**
   - `labs/multiprocessing/03_start_method_fork_vs_spawn_vs_forkserver.py`
   - Fork pitfalls with threads and native libs

4. **Shared memory**
   - `labs/multiprocessing/04_shared_memory_doc_images.py`
   - Avoid copying large arrays across processes

5. **Chunking & batching**
   - `labs/multiprocessing/05_process_pool_chunking_nlp.py`
   - Why chunking helps sometimes — and hurts other times

6. **Production pipelines**
   - `labs/multiprocessing/06_pipeline_cpu_procs_to_postgres_writer.py`
   - CPU workers + DB writer + backpressure

Read the docs in the same order.
