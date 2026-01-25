"""
C3: multiprocessing start methods on Linux: fork vs spawn vs forkserver.

What this lab proves (professionally relevant):
1) Startup overhead differences (fork is fast, spawn is slow).
2) Fork is NOT safe if the parent has running threads (common in services).
   We'll simulate a lock held by a background thread, then fork and show the child can hang.
3) Spawn/forkserver avoid inheriting unsafe threaded state, at the cost of startup overhead.

Run:
  python labs/multiprocessing/03_start_method_fork_vs_spawn_vs_forkserver.py --method fork
  python labs/multiprocessing/03_start_method_fork_vs_spawn_vs_forkserver.py --method spawn
  python labs/multiprocessing/03_start_method_fork_vs_spawn_vs_forkserver.py --method forkserver

Tip:
- I have tried this on Linux only, but spawn/forkserver are still relevant because containers/services often have threads.
"""

from __future__ import annotations

import argparse
import os
import threading
import time
from multiprocessing import get_context


def cpu_task(n: int) -> int:
    # small deterministic CPU loop
    acc = 0
    for i in range(n):
        acc = (acc * 1664525 + 1013904223 + i) & 0xFFFFFFFF
    return acc


def worker_cpu(n: int) -> int:
    return cpu_task(n)


# ---- fork + threads pitfall demo ----
_LOCK = threading.Lock()


def _bg_thread_hold_lock(hold_s: float) -> None:
    _LOCK.acquire()
    try:
        time.sleep(hold_s)
    finally:
        _LOCK.release()


def child_try_lock(timeout_s: float) -> str:
    """
    In a forked child, if the parent had a thread holding _LOCK at fork time,
    the child will inherit the locked state *without the owning thread*.
    Trying to acquire can block forever. We use timeout to keep it safe.
    """
    got = _LOCK.acquire(timeout=timeout_s)
    if got:
        _LOCK.release()
        return "acquired"
    return "timeout"


def run_startup_bench(ctx_name: str, procs: int, n: int) -> None:
    ctx = get_context(ctx_name)

    t0 = time.perf_counter()
    ps = []
    for _ in range(procs):
        p = ctx.Process(target=worker_cpu, args=(n,))
        p.start()
        ps.append(p)
    for p in ps:
        p.join()
    wall = time.perf_counter() - t0

    print(f"[startup] method={ctx_name:10s} procs={procs:2d} wall={wall:.3f}s")


def run_fork_threads_pitfall(ctx_name: str) -> None:
    ctx = get_context(ctx_name)

    # Start background thread and hold lock during fork window
    th = threading.Thread(target=_bg_thread_hold_lock, args=(2.0,), daemon=True)
    th.start()
    time.sleep(0.1)  # ensure lock is acquired

    t0 = time.perf_counter()
    # child tries to acquire with timeout, so it won't hang your run
    p = ctx.Process(target=_child_report, args=(ctx_name,))
    p.start()
    p.join(timeout=5.0)
    wall = time.perf_counter() - t0

    if p.is_alive():
        print(f"[threads] method={ctx_name:10s} child still alive after timeout => hard hang risk (killing)")
        p.kill()
        p.join()
    else:
        print(f"[threads] method={ctx_name:10s} completed in {wall:.3f}s")


def _child_report(ctx_name: str) -> None:
    res = child_try_lock(timeout_s=1.0)
    print(f"[child] method={ctx_name} lock_acquire={res}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--method", choices=["fork", "spawn", "forkserver"], default="fork")
    ap.add_argument("--procs", type=int, default=16)
    ap.add_argument("--n", type=int, default=2_000_000)
    ap.add_argument("--skip-threads-demo", action="store_true")
    args = ap.parse_args()

    print(f"Python PID={os.getpid()} | method={args.method} | cpu_count={os.cpu_count()}")
    run_startup_bench(args.method, procs=args.procs, n=args.n)

    if not args.skip_threads_demo:
        print("\nFork+threads pitfall demo (safe, uses timeouts):")
        run_fork_threads_pitfall(args.method)

    print("\nExpected:")
    print("- fork: fastest startup, but unsafe if parent has threads / locks / native runtimes.")
    print("- spawn: clean state, slower startup, requires picklable targets.")
    print("- forkserver: compromise; avoids forking a threaded parent in many cases.")


if __name__ == "__main__":
    main()
