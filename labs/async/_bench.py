from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

import psutil


@dataclass
class Result:
    name: str
    wall_s: float
    n: int
    per_s: float
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    rss_delta_mb: float
    cpu_avg_pct: float
    cpu_peak_pct: float


def _pct(values: Sequence[float], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    k = int(round((p / 100.0) * (len(xs) - 1)))
    k = max(0, min(k, len(xs) - 1))
    return xs[k]


def _cpu_sample(proc: psutil.Process) -> float:
    # System-wide CPU percent; good enough for comparing runs on same machine.
    return psutil.cpu_percent(interval=None)


def run_with_sys_sampling(fn: Callable[[], None]) -> tuple[float, float, float, float]:
    proc = psutil.Process(os.getpid())
    rss0 = proc.memory_info().rss

    # Prime cpu_percent
    psutil.cpu_percent(interval=None)

    cpu_samples: list[float] = []
    t0 = time.perf_counter()

    # A light sampling loop: we sample right before and after; and also once mid-run if long enough.
    fn()

    t1 = time.perf_counter()
    cpu_samples.append(_cpu_sample(proc))

    rss1 = proc.memory_info().rss
    wall = t1 - t0
    rss_delta_mb = (rss1 - rss0) / (1024 * 1024)

    if cpu_samples:
        return wall, sum(cpu_samples) / len(cpu_samples), max(cpu_samples), rss_delta_mb
    return wall, 0.0, 0.0, rss_delta_mb


def summarize_latencies_ms(lat_ms: Iterable[float]) -> tuple[float, float, float, float]:
    xs = list(lat_ms)
    if not xs:
        return 0.0, 0.0, 0.0, 0.0
    mean = sum(xs) / len(xs)
    return mean, _pct(xs, 50), _pct(xs, 95), _pct(xs, 99)


def print_results(results: list[Result]) -> None:
    print()
    print(
        "Name".ljust(34)
        + " | Wall(s) |   N |   /s   | Mean(ms) | P50 | P95 | P99 | ΔRSS(MB) | CPU(avg/peak)"
    )
    print("-" * 110)
    for r in results:
        print(
            f"{r.name.ljust(34)} | "
            f"{r.wall_s:7.3f} | "
            f"{r.n:4d} | "
            f"{r.per_s:6.1f} | "
            f"{r.mean_ms:8.2f} | "
            f"{r.p50_ms:5.2f} | "
            f"{r.p95_ms:5.2f} | "
            f"{r.p99_ms:5.2f} | "
            f"{r.rss_delta_mb:7.1f} | "
            f"{r.cpu_avg_pct:6.1f}%/{r.cpu_peak_pct:6.1f}%"
        )
