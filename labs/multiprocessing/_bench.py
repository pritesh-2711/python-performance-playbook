"""
Benchmarking utilities for multiprocessing labs.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

import psutil


@dataclass
class Result:
    name: str
    wall_s: float
    items: int
    proc_rss_mb: float
    sys_cpu_avg: float
    sys_cpu_peak: float

    @property
    def ips(self) -> float:
        return self.items / self.wall_s if self.wall_s > 0 else 0.0


def _sample_system_cpu(stop: threading.Event, out: list[float], interval_s: float) -> None:
    psutil.cpu_percent(interval=None)  # prime
    while not stop.is_set():
        out.append(psutil.cpu_percent(interval=interval_s))


def measure(fn, *, name: str, items: int, cpu_sample_interval_s: float = 0.2) -> Result:
    proc = psutil.Process(os.getpid())
    rss0 = proc.memory_info().rss

    stop = threading.Event()
    samples: list[float] = []
    th = threading.Thread(
        target=_sample_system_cpu,
        args=(stop, samples, cpu_sample_interval_s),
        daemon=True,
    )

    t0 = time.perf_counter()
    th.start()

    fn()

    stop.set()
    th.join(timeout=2.0)
    wall = time.perf_counter() - t0

    rss1 = proc.memory_info().rss
    avg = (sum(samples) / len(samples)) if samples else 0.0
    peak = max(samples) if samples else 0.0

    return Result(
        name=name,
        wall_s=wall,
        items=items,
        proc_rss_mb=(rss1 - rss0) / (1024 * 1024),
        sys_cpu_avg=avg,
        sys_cpu_peak=peak,
    )


def print_results(results: list[Result]) -> None:
    print("\nName                         | Wall(s) | Items | Items/s | ΔRSS(MB) | CPU(avg/peak)")
    print("-" * 92)
    for r in results:
        print(
            f"{r.name:<28} | {r.wall_s:>7.3f} | {r.items:>5d} | {r.ips:>7.2f} | {r.proc_rss_mb:>8.1f} | "
            f"{r.sys_cpu_avg:>5.1f}%/{r.sys_cpu_peak:>5.1f}%"
        )
