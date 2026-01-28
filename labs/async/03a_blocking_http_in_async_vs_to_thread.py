"""
python labs/async/03a_blocking_http_in_async_vs_to_thread.py --n 500 --concurrency 32 --thread-cap 32

Demonstrates the impact of blocking HTTP calls inside async coroutines vs offloading to threads.
Expected:
- BAD: sync HTTP blocks loop => worse throughput + ugly tails as concurrency rises.
- GOOD: to_thread keeps loop responsive; better scaling until server saturates.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from statistics import mean

import httpx
import requests
from dotenv import load_dotenv

from _bench import Result, print_results, summarize_latencies_ms, run_with_sys_sampling


def blocking_requests_call(url: str, params: dict, timeout: float) -> float:
    t0 = time.perf_counter()
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return (time.perf_counter() - t0) * 1000.0


async def bad_blocking(url: str, n: int, concurrency: int, timeout: float) -> list[float]:
    sem = asyncio.Semaphore(concurrency)
    lat: list[float] = []

    async def one(i: int) -> None:
        async with sem:
            # BAD: sync requests call blocks the event loop thread
            ms = blocking_requests_call(url, {"q": f"q{i}"}, timeout)
            lat.append(ms)

    await asyncio.gather(*[asyncio.create_task(one(i)) for i in range(n)])
    return lat


async def good_to_thread(url: str, n: int, concurrency: int, timeout: float, thread_cap: int) -> list[float]:
    sem = asyncio.Semaphore(concurrency)
    tsem = asyncio.Semaphore(thread_cap)
    lat: list[float] = []

    async def one(i: int) -> None:
        async with sem:
            async with tsem:
                ms = await asyncio.to_thread(blocking_requests_call, url, {"q": f"q{i}"}, timeout)
            lat.append(ms)

    await asyncio.gather(*[asyncio.create_task(one(i)) for i in range(n)])
    return lat


def main() -> None:
    load_dotenv()
    base = os.getenv("PPB_HTTP_BASE", "http://127.0.0.1:8008")
    path = os.getenv("PPB_HTTP_PATH", "/search")
    url = f"{base}{path}"

    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=500)
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--thread-cap", type=int, default=32)
    ap.add_argument("--timeout", type=float, default=5.0)
    args = ap.parse_args()

    # preflight
    try:
        requests.get(url, params={"q": "ping"}, timeout=2.0).raise_for_status()
    except Exception as e:
        raise SystemExit(f"HTTP preflight failed at {url}: {e}")

    results: list[Result] = []

    bad_lat: list[float] = []
    def _bad() -> None:
        nonlocal bad_lat
        bad_lat = asyncio.run(bad_blocking(url, args.n, args.concurrency, args.timeout))
    wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(_bad)
    m, p50, p95, p99 = summarize_latencies_ms(bad_lat)
    results.append(Result(f"D3A-bad sync HTTP in async c={args.concurrency}", wall, args.n, args.n / wall, m, p50, p95, p99, rss_delta, cpu_avg, cpu_peak))

    good_lat: list[float] = []
    def _good() -> None:
        nonlocal good_lat
        good_lat = asyncio.run(good_to_thread(url, args.n, args.concurrency, args.timeout, args.thread_cap))
    wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(_good)
    m, p50, p95, p99 = summarize_latencies_ms(good_lat)
    results.append(Result(f"D3A-good to_thread sync HTTP c={args.concurrency} tcap={args.thread_cap}", wall, args.n, args.n / wall, m, p50, p95, p99, rss_delta, cpu_avg, cpu_peak))

    print_results(results)
    print("\nExpected:")
    print("- BAD: sync HTTP blocks loop => worse throughput + ugly tails as concurrency rises.")
    print("- GOOD: to_thread keeps loop responsive; better scaling until server saturates.")


if __name__ == "__main__":
    main()
