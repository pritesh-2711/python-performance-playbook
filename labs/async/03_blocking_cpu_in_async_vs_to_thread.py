"""
python labs/async/03_blocking_cpu_in_async_vs_to_thread.py --n 400 --concurrency 32 --repeat 80 --token-cap 8000 --thread-cap 8
Demonstrates the impact of blocking CPU work inside async coroutines vs offloading to threads.
Expected:
- BAD: event loop blocked => all tasks slow; p95/p99 ugly as concurrency rises.
- GOOD: loop stays responsive; throughput improves up to thread_cap; tail latency stabilizes.
- If both are too fast, increase --repeat or --token-cap.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import time
from pathlib import Path

from _bench import Result, print_results, summarize_latencies_ms, run_with_sys_sampling

from dotenv import load_dotenv
load_dotenv()

# Reuse your REAL dataset materialized in Section C
# python-playbook/multiprocessing/labs/multiprocessing/_data/arxiv_text/docs
DEFAULT_ARXIV_DIR = Path(__file__).resolve().parents[2] / "labs" / "multiprocessing" / "_data" / "arxiv_text" / "docs"
TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def cpu_nlp_hash(path: str, repeat: int, token_cap: int) -> int:
    txt = Path(path).read_text(encoding="utf-8", errors="ignore")
    tokens = TOKEN_RE.findall(txt.lower())[:token_cap]

    acc = 0
    for _ in range(repeat):
        for t in tokens:
            h = 2166136261
            for ch in t:
                h ^= ord(ch)
                h = (h * 16777619) & 0xFFFFFFFF
            acc ^= h
    return acc


async def simulate_requests_blocking(paths: list[str], n: int, concurrency: int, repeat: int, token_cap: int) -> list[float]:
    """
    BAD: CPU runs inside coroutine => blocks event loop.
    """
    sem = asyncio.Semaphore(concurrency)
    lat_ms: list[float] = []

    async def one(i: int) -> None:
        async with sem:
            t0 = time.perf_counter()
            _ = cpu_nlp_hash(paths[i % len(paths)], repeat=repeat, token_cap=token_cap)
            t1 = time.perf_counter()
            lat_ms.append((t1 - t0) * 1000.0)

    tasks = [asyncio.create_task(one(i)) for i in range(n)]
    await asyncio.gather(*tasks)
    return lat_ms


async def simulate_requests_to_thread(paths: list[str], n: int, concurrency: int, repeat: int, token_cap: int, thread_cap: int) -> list[float]:
    """
    GOOD: CPU offloaded to threads; event loop stays responsive.
    thread_cap prevents creating too many CPU tasks at once (backpressure).
    """
    sem = asyncio.Semaphore(concurrency)
    tsem = asyncio.Semaphore(thread_cap)
    lat_ms: list[float] = []

    async def one(i: int) -> None:
        async with sem:
            t0 = time.perf_counter()
            async with tsem:
                _ = await asyncio.to_thread(cpu_nlp_hash, paths[i % len(paths)], repeat, token_cap)
            t1 = time.perf_counter()
            lat_ms.append((t1 - t0) * 1000.0)

    tasks = [asyncio.create_task(one(i)) for i in range(n)]
    await asyncio.gather(*tasks)
    return lat_ms


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=400, help="number of simulated requests/jobs")
    ap.add_argument("--concurrency", type=int, default=32, help="async in-flight tasks")
    ap.add_argument("--repeat", type=int, default=80, help="CPU heaviness (increase if too fast)")
    ap.add_argument("--token-cap", type=int, default=8000, help="max tokens per doc to hash")
    ap.add_argument("--thread-cap", type=int, default=8, help="max concurrent to_thread CPU jobs")
    ap.add_argument("--arxiv-dir", default=str(DEFAULT_ARXIV_DIR))
    args = ap.parse_args()

    arxiv_dir = Path(args.arxiv_dir)
    files = sorted(arxiv_dir.glob("*.txt"))
    if not files:
        raise SystemExit(f"No ArXiv docs found at {arxiv_dir}. Run multiprocessing 00_fetch_and_materialize.py first.")

    paths = [str(p) for p in files[: min(len(files), 2000)]]

    print(f"Docs: {len(paths)} from {arxiv_dir}")
    print(f"n={args.n} concurrency={args.concurrency} repeat={args.repeat} token_cap={args.token_cap} thread_cap={args.thread_cap}")

    results: list[Result] = []

    # BAD
    bad_lat: list[float] = []
    def bad_run() -> None:
        nonlocal bad_lat
        bad_lat = asyncio.run(simulate_requests_blocking(paths, args.n, args.concurrency, args.repeat, args.token_cap))

    wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(bad_run)
    mean, p50, p95, p99 = summarize_latencies_ms(bad_lat)
    results.append(
        Result(
            name=f"D3-bad blocking c={args.concurrency}",
            wall_s=wall,
            n=args.n,
            per_s=(args.n / wall if wall else 0.0),
            mean_ms=mean,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
            rss_delta_mb=rss_delta,
            cpu_avg_pct=cpu_avg,
            cpu_peak_pct=cpu_peak,
        )
    )

    # GOOD
    good_lat: list[float] = []
    def good_run() -> None:
        nonlocal good_lat
        good_lat = asyncio.run(
            simulate_requests_to_thread(paths, args.n, args.concurrency, args.repeat, args.token_cap, args.thread_cap)
        )

    wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(good_run)
    mean, p50, p95, p99 = summarize_latencies_ms(good_lat)
    results.append(
        Result(
            name=f"D3-good to_thread c={args.concurrency} tcap={args.thread_cap}",
            wall_s=wall,
            n=args.n,
            per_s=(args.n / wall if wall else 0.0),
            mean_ms=mean,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
            rss_delta_mb=rss_delta,
            cpu_avg_pct=cpu_avg,
            cpu_peak_pct=cpu_peak,
        )
    )

    print_results(results)

    print("\nExpected:")
    print("- BAD: event loop blocked => all tasks slow; p95/p99 ugly as concurrency rises.")
    print("- GOOD: loop stays responsive; throughput improves up to thread_cap; tail latency stabilizes.")
    print("- If both are too fast, increase --repeat or --token-cap.")


if __name__ == "__main__":
    main()
