from __future__ import annotations

import argparse
import asyncio
import os
import time
# from typing import Iterable

import httpx
from dotenv import load_dotenv
load_dotenv()

from _bench import Result, print_results, summarize_latencies_ms, run_with_sys_sampling  # noqa: E402


async def _one(client: httpx.AsyncClient, url: str, params: dict, sem: asyncio.Semaphore, lat_ms: list[float]) -> None:
    async with sem:
        t0 = time.perf_counter()
        r = await client.get(url, params=params)
        r.raise_for_status()
        t1 = time.perf_counter()
        lat_ms.append((t1 - t0) * 1000.0)


async def run_http(n: int, concurrency: int, url: str, params: dict, timeout_s: float) -> list[float]:
    sem = asyncio.Semaphore(concurrency)
    lat_ms: list[float] = []

    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(timeout=timeout_s, limits=limits) as client:
        tasks = [asyncio.create_task(_one(client, url, params, sem, lat_ms)) for _ in range(n)]
        await asyncio.gather(*tasks)

    return lat_ms


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=500)
    ap.add_argument("--concurrency", default="1,2,4,8,16,32")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--q", default="python concurrency")
    args = ap.parse_args()

    here_env = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(here_env)

    base = os.getenv("PPB_HTTP_BASE", "http://127.0.0.1:8008")
    path = os.getenv("PPB_HTTP_PATH", "/search")
    url = base.rstrip("/") + path

    concs = [int(x) for x in args.concurrency.split(",")]

    print(f"Target URL: {url}")
    print(f"Requests: {args.requests} | repeat: {args.repeat}")
    print(f"Concurrency sweep: {concs}")

    results: list[Result] = []

    for c in concs:
        all_lat: list[float] = []

        def _run_once_blocking() -> None:
            # run async workload from sync harness
            lat = asyncio.run(run_http(args.requests, c, url, {"q": args.q}, args.timeout))
            all_lat.extend(lat)

        wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(lambda: [_run_once_blocking() for _ in range(args.repeat)])

        mean, p50, p95, p99 = summarize_latencies_ms(all_lat)
        total = args.requests * args.repeat
        rps = total / wall if wall > 0 else 0.0

        results.append(
            Result(
                name=f"D1-http async c={c}",
                wall_s=wall,
                n=total,
                per_s=rps,
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

    print("\nHow to read this:")
    print("- Higher concurrency usually increases throughput (RPS) until the server/DB saturates.")
    print("- Watch p95/p99: too much concurrency can wreck tail latency even if RPS rises.")


if __name__ == "__main__":
    main()
