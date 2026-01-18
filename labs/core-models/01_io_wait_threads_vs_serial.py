"""
I/O-bound demo (simulated wait): threads overlap I/O waiting and reduce wall time.

Run:
  python labs/core_models/01_io_wait_threads_vs_serial.py

What it shows:
- Serial I/O waits add up.
- Threads overlap waits -> shorter total time.

Note:
This uses time.sleep as a stand-in for network/DB wait.
In later sections, I will use real HTTP and real PostgreSQL.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor


def io_task(i: int, wait_s: float = 0.2) -> int:
    time.sleep(wait_s)
    return i


def run_serial(n: int) -> float:
    t0 = time.perf_counter()
    for i in range(n):
        io_task(i)
    return time.perf_counter() - t0


def run_threads(n: int, workers: int) -> float:
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(io_task, range(n)))
    return time.perf_counter() - t0


def main() -> None:
    n = 50

    serial_s = run_serial(n)
    threaded_s_5 = run_threads(n, workers=5)
    threaded_s_20 = run_threads(n, workers=20)

    print(f"Tasks: {n}, each waits ~0.2s")
    print(f"Serial     : {serial_s:.3f}s")
    print(f"Threads(5) : {threaded_s_5:.3f}s")
    print(f"Threads(20): {threaded_s_20:.3f}s")
    print("\nExpected: threads much faster than serial for I/O wait.")


if __name__ == "__main__":
    main()
