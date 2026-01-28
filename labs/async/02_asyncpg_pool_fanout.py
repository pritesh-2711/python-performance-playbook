from __future__ import annotations

import argparse
import asyncio
import os
import random
import time

import asyncpg

from _bench import Result, print_results, summarize_latencies_ms, run_with_sys_sampling
from dotenv import load_dotenv
load_dotenv()


QUERY = """
SELECT u.user_id, u.email, e.event_type, e.payload
FROM ppb_async_users u
JOIN ppb_async_events e ON e.user_id = u.user_id
WHERE u.user_id = $1
LIMIT 5;
"""


async def _one(pool: asyncpg.Pool, user_id: int, sem: asyncio.Semaphore, lat_ms: list[float]) -> None:
    async with sem:
        t0 = time.perf_counter()
        async with pool.acquire() as conn:
            _ = await conn.fetch(QUERY, user_id)
        t1 = time.perf_counter()
        lat_ms.append((t1 - t0) * 1000.0)


async def run_queries(
    *,
    dsn: str,
    n: int,
    concurrency: int,
    pool_min: int,
    pool_max: int,
    user_max: int,
) -> list[float]:
    sem = asyncio.Semaphore(concurrency)
    lat_ms: list[float] = []

    pool = await asyncpg.create_pool(dsn=dsn, min_size=pool_min, max_size=pool_max)
    try:
        tasks = []
        for _ in range(n):
            uid = random.randint(1, user_max)
            tasks.append(asyncio.create_task(_one(pool, uid, sem, lat_ms)))
        await asyncio.gather(*tasks)
    finally:
        await pool.close()

    return lat_ms


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", type=int, default=3000)
    ap.add_argument("--concurrency", default="1,4,8,16,32,64")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--pool", default="4,8,16,32", help="pool max sizes to try (min is same as max/2, at least 1)")
    ap.add_argument("--user-max", type=int, default=50_000, help="max user_id in ppb_async_users")
    args = ap.parse_args()

    here_env = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(here_env)
    dsn = os.getenv("PPB_PG_DSN_ASYNC")
    if not dsn:
        raise SystemExit("Missing PPB_PG_DSN_ASYNC in labs/async/.env")

    concs = [int(x) for x in args.concurrency.split(",")]
    pools = [int(x) for x in args.pool.split(",")]

    print(f"D2 asyncpg pool fanout | queries={args.queries} repeat={args.repeat}")
    print(f"Concurrency sweep: {concs}")
    print(f"Pool max sweep: {pools}")

    results: list[Result] = []

    for pmax in pools:
        pmin = max(1, pmax // 2)

        for c in concs:
            all_lat: list[float] = []

            def _run_once_blocking() -> None:
                lat = asyncio.run(
                    run_queries(
                        dsn=dsn,
                        n=args.queries,
                        concurrency=c,
                        pool_min=pmin,
                        pool_max=pmax,
                        user_max=args.user_max,
                    )
                )
                all_lat.extend(lat)

            wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(lambda: [_run_once_blocking() for _ in range(args.repeat)])

            total = args.queries * args.repeat
            qps = total / wall if wall > 0 else 0.0
            mean, p50, p95, p99 = summarize_latencies_ms(all_lat)

            results.append(
                Result(
                    name=f"D2-pool max={pmax} c={c}",
                    wall_s=wall,
                    n=total,
                    per_s=qps,
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

    print("\nInterpretation:")
    print("- If concurrency >> pool_max: tasks queue waiting for a connection => p95/p99 rises.")
    print("- If pool_max too large: DB contention rises; tail latency can degrade.")
    print("- The best pool size depends on DB capacity + query cost; measure on your machine.")


if __name__ == "__main__":
    main()
