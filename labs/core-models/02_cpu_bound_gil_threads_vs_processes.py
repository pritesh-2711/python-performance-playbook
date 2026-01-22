"""
CPU-bound demo: threads won't speed up pure Python CPU work because of the GIL.
Processes can speed it up by using multiple cores.

Run:
  python labs/core_models/02_cpu_bound_gil_threads_vs_processes.py

Notes:
- This is intentionally pure Python work.
- Results vary by CPU, OS, power mode.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


def cpu_task(n: int) -> int:
    # pure python loop (GIL-bound)
    s = 0
    for i in range(n):
        s += (i * i) % 97
    return s


def bench(label: str, fn) -> float:
    t0 = time.perf_counter()
    fn()
    dt = time.perf_counter() - t0
    print(f"{label:<18} {dt:.3f}s")
    return dt


def main() -> None:
    workers = max(2, (os.cpu_count() or 2) // 2)
    tasks = 8
    n = 30_000_000  # tune up/down depending on your machine

    print(f"CPU cores: {os.cpu_count()} | workers: {workers} | tasks: {tasks}")
    print("Work is pure Python => threads should NOT scale well.\n")

    def serial():
        for _ in range(tasks):
            cpu_task(n)

    def threads():
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(cpu_task, [n] * tasks))

    def procs():
        with ProcessPoolExecutor(max_workers=workers) as ex:
            list(ex.map(cpu_task, [n] * tasks))

    bench("Serial", serial)
    bench("Threads", threads)
    bench("Processes", procs)

    print("\nExpected:")
    print("- Threads ~= Serial (sometimes worse due to overhead)")
    print("- Processes faster (until overhead dominates)")


if __name__ == "__main__":
    main()
