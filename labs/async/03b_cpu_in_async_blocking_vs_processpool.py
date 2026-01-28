"""
python labs/async/03b_cpu_in_async_blocking_vs_processpool.py --n 400 --concurrency 32 --repeat 200 --token-cap 20000 --procs 8

Demonstrates the impact of blocking CPU work inside async coroutines vs offloading to a process pool.
Expected:
- BAD: CPU runs on event loop thread => tasks serialize; worse throughput under concurrency.
- GOOD: process pool gives real parallelism; better throughput until CPU saturated.
- If it’s too fast/flat, increase --repeat/--token-cap.
"""
from __future__ import annotations

import argparse
import asyncio
import re
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from _bench import Result, print_results, summarize_latencies_ms, run_with_sys_sampling

DEFAULT_ARXIV_DIR = (
    Path(__file__).resolve().parents[2]
    / "labs"
    / "multiprocessing"
    / "_data"
    / "arxiv_text"
    / "docs"
)

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


async def bad_cpu_in_loop(paths: list[str], n: int, concurrency: int, repeat: int, token_cap: int) -> list[float]:
    sem = asyncio.Semaphore(concurrency)
    lat: list[float] = []

    async def one(i: int) -> None:
        async with sem:
            t0 = time.perf_counter()
            _ = cpu_nlp_hash(paths[i % len(paths)], repeat, token_cap)
            lat.append((time.perf_counter() - t0) * 1000.0)

    await asyncio.gather(*[asyncio.create_task(one(i)) for i in range(n)])
    return lat


async def good_cpu_processpool(
    paths: list[str],
    n: int,
    concurrency: int,
    repeat: int,
    token_cap: int,
    procs: int,
) -> list[float]:
    sem = asyncio.Semaphore(concurrency)
    lat: list[float] = []

    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=procs) as pool:

        async def one(i: int) -> None:
            async with sem:
                t0 = time.perf_counter()
                _ = await loop.run_in_executor(pool, cpu_nlp_hash, paths[i % len(paths)], repeat, token_cap)
                lat.append((time.perf_counter() - t0) * 1000.0)

        await asyncio.gather(*[asyncio.create_task(one(i)) for i in range(n)])

    return lat


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=400)
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--repeat", type=int, default=200)
    ap.add_argument("--token-cap", type=int, default=20000)
    ap.add_argument("--procs", type=int, default=8)
    ap.add_argument("--arxiv-dir", default=str(DEFAULT_ARXIV_DIR))
    args = ap.parse_args()

    arxiv_dir = Path(args.arxiv_dir)
    files = sorted(arxiv_dir.glob("*.txt"))
    if not files:
        raise SystemExit(f"No ArXiv docs found at {arxiv_dir}. Run multiprocessing 00_fetch_and_materialize.py first.")

    paths = [str(p) for p in files[: min(len(files), 2000)]]
    print(f"Docs: {len(paths)} from {arxiv_dir}")
    print(f"n={args.n} concurrency={args.concurrency} repeat={args.repeat} token_cap={args.token_cap} procs={args.procs}")

    results: list[Result] = []

    bad_lat: list[float] = []
    def _bad() -> None:
        nonlocal bad_lat
        bad_lat = asyncio.run(bad_cpu_in_loop(paths, args.n, args.concurrency, args.repeat, args.token_cap))

    wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(_bad)
    m, p50, p95, p99 = summarize_latencies_ms(bad_lat)
    results.append(Result(f"D3B-bad CPU in loop c={args.concurrency}", wall, args.n, args.n / wall, m, p50, p95, p99, rss_delta, cpu_avg, cpu_peak))

    good_lat: list[float] = []
    def _good() -> None:
        nonlocal good_lat
        good_lat = asyncio.run(good_cpu_processpool(paths, args.n, args.concurrency, args.repeat, args.token_cap, args.procs))

    wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(_good)
    m, p50, p95, p99 = summarize_latencies_ms(good_lat)
    results.append(Result(f"D3B-good processpool c={args.concurrency} procs={args.procs}", wall, args.n, args.n / wall, m, p50, p95, p99, rss_delta, cpu_avg, cpu_peak))

    print_results(results)

    print("\nExpected:")
    print("- BAD: CPU runs on event loop thread => tasks serialize; worse throughput under concurrency.")
    print("- GOOD: process pool gives real parallelism; better throughput until CPU saturated.")
    print("- If it’s too fast/flat, increase --repeat/--token-cap.")


if __name__ == "__main__":
    main()
