from __future__ import annotations

import argparse
import asyncio
import os
import signal
import time
from dataclasses import dataclass
from typing import List, Tuple

import httpx

from _bench import print_results, summarize_latencies_ms, run_with_sys_sampling, Result


@dataclass
class RunStats:
    lats_ms: List[float]
    errors: int


def _url() -> str:
    base = os.getenv("PPB_HTTP_BASE", "http://127.0.0.1:8008").rstrip("/")
    path = os.getenv("PPB_HTTP_PATH", "/search")
    if not path.startswith("/"):
        path = "/" + path
    return base + path


async def _one_http(client: httpx.AsyncClient, url: str, q: str, timeout_s: float) -> Tuple[float, bool]:
    """
    Returns (latency_ms, ok)
    """
    t0 = time.perf_counter()
    try:
        r = await client.get(url, params={"q": q}, timeout=timeout_s)
        ok = (200 <= r.status_code < 300)
    except Exception:
        ok = False
    lat_ms = (time.perf_counter() - t0) * 1000.0
    return lat_ms, ok


async def run_unbounded(n: int, url: str, q: str, timeout_s: float) -> RunStats:
    """
    Creates all tasks at once. This is the classic "oops" pattern in real services.
    """
    lats: List[float] = []
    errors = 0

    limits = httpx.Limits(max_connections=None, max_keepalive_connections=100)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [asyncio.create_task(_one_http(client, url, q, timeout_s)) for _ in range(n)]
        for t in asyncio.as_completed(tasks):
            lat, ok = await t
            lats.append(lat)
            if not ok:
                errors += 1

    return RunStats(lats, errors)


async def run_bounded(n: int, concurrency: int, url: str, q: str, timeout_s: float) -> RunStats:
    """
    Bounded in-flight requests using a Semaphore (production pattern).
    """
    sem = asyncio.Semaphore(concurrency)
    lats: List[float] = []
    errors = 0

    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(limits=limits) as client:

        async def one() -> None:
            nonlocal errors
            async with sem:
                lat, ok = await _one_http(client, url, q, timeout_s)
                lats.append(lat)
                if not ok:
                    errors += 1

        await asyncio.gather(*[asyncio.create_task(one()) for _ in range(n)])

    return RunStats(lats, errors)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1500)
    ap.add_argument("--q", default="transformers")
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("--concurrency", default="8,16,32,64")
    args = ap.parse_args()

    url = _url()
    conc = [int(x) for x in args.concurrency.split(",")]

    stop = asyncio.Event()

    def _sigint(*_: object) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _sigint)

    results: list[Result] = []

    # UNBOUNDED
    stats: RunStats = RunStats([], 0)

    def _run_unbounded_blocking() -> None:
        nonlocal stats
        stats = asyncio.run(run_unbounded(args.n, url, args.q, args.timeout))

    wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(_run_unbounded_blocking)
    m, p50, p95, p99 = summarize_latencies_ms(stats.lats_ms)
    results.append(
        Result(
            f"D4-http UNBOUNDED n={args.n} timeout={args.timeout}s errors={stats.errors}",
            wall,
            args.n,
            args.n / wall,
            m,
            p50,
            p95,
            p99,
            rss_delta,
            cpu_avg,
            cpu_peak,
        )
    )

    # BOUNDED sweeps
    for c in conc:
        stats = RunStats([], 0)

        def _run_bounded_blocking() -> None:
            nonlocal stats
            stats = asyncio.run(run_bounded(args.n, c, url, args.q, args.timeout))

        wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(_run_bounded_blocking)
        m, p50, p95, p99 = summarize_latencies_ms(stats.lats_ms)
        results.append(
            Result(
                f"D4-http bounded c={c} n={args.n} timeout={args.timeout}s errors={stats.errors}",
                wall,
                args.n,
                args.n / wall,
                m,
                p50,
                p95,
                p99,
                rss_delta,
                cpu_avg,
                cpu_peak,
            )
        )

    print_results(results)

    print("\nHow to read this:")
    print("- UNBOUNDED: easy to create huge in-flight pressure; tails/errors typically worsen.")
    print("- BOUNDED: stable; choose concurrency where p95/p99 stay acceptable.")


if __name__ == "__main__":
    main()
