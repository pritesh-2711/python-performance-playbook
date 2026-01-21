"""
Thread overhead demo: increasing thread count helps I/O-bound work *until it doesn't*.

What it shows
- For I/O wait workloads, threads improve throughput by overlapping waits.
- After a point, more threads add overhead (scheduling/context switching) and gains flatten or degrade.

Run:
  python labs/core-models/05_thread_overhead_diminishing_returns.py

Optional:
  python labs/core-models/05_thread_overhead_diminishing_returns.py --tasks 200 --wait 0.05

Notes:
- This uses time.sleep to simulate I/O wait. Later you'll repeat with real HTTP/DB.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor


def io_task(_: int, wait_s: float) -> int:
    time.sleep(wait_s)
    return 1


def run(n_tasks: int, wait_s: float, workers: int) -> float:
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(lambda i: io_task(i, wait_s), range(n_tasks)))
    return time.perf_counter() - t0


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--tasks", type=int, default=200, help="Number of I/O-like tasks.")
    p.add_argument("--wait", type=float, default=0.05, help="Sleep per task (seconds).")
    p.add_argument(
        "--workers",
        type=str,
        default="1,2,4,8,16,32,64",
        help="Comma-separated worker counts to test.",
    )
    args = p.parse_args()

    worker_list = [int(x.strip()) for x in args.workers.split(",") if x.strip()]
    n = args.tasks
    w = args.wait

    print(f"\nTasks: {n}, wait per task: {w:.3f}s")
    print("Workers | Time (s) | Speedup vs 1 worker")
    print("-" * 40)

    baseline = None
    for workers in worker_list:
        dt = run(n, w, workers)
        if baseline is None:
            baseline = dt
        speedup = baseline / dt if dt > 0 else float("inf")
        print(f"{workers:>7} | {dt:>8.3f} | {speedup:>17.2f}x")

    print("\nExpected pattern:")
    print("- Speedup rises quickly at first (overlapping I/O waits).")
    print("- Then plateaus (I/O limit) and may degrade (thread overhead).\n")


if __name__ == "__main__":
    main()
