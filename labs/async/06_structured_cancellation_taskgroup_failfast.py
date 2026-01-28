from __future__ import annotations

import argparse
import asyncio
import os
import time
from dataclasses import dataclass
from typing import List, Tuple

import asyncpg
import httpx

from _bench import Result, print_results, summarize_latencies_ms, run_with_sys_sampling
from dotenv import load_dotenv
load_dotenv()

DDL = """
CREATE TABLE IF NOT EXISTS ppb_async_failfast (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  job_id INT NOT NULL,
  http_ok BOOLEAN NOT NULL,
  latency_ms REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ppb_async_failfast_created_at_idx ON ppb_async_failfast(created_at);
"""


def _http_url() -> str:
    base = os.getenv("PPB_HTTP_BASE", "http://127.0.0.1:8008").rstrip("/")
    path = os.getenv("PPB_HTTP_PATH", "/search")
    if not path.startswith("/"):
        path = "/" + path
    return base + path


def _pg_dsn() -> str:
    dsn = os.getenv("PPB_PG_DSN_ASYNC") or os.getenv("PPB_PG_DSN")
    if not dsn:
        raise SystemExit("Set PPB_PG_DSN_ASYNC (or PPB_PG_DSN) in your .env")
    return dsn


async def _ensure_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(DDL)


async def _one_http(client: httpx.AsyncClient, url: str, timeout_s: float) -> Tuple[float, bool]:
    t0 = time.perf_counter()
    try:
        r = await client.get(url, params={"q": "transformers"}, timeout=timeout_s)
        ok = (200 <= r.status_code < 300)
    except Exception:
        ok = False
    lat_ms = (time.perf_counter() - t0) * 1000.0
    return lat_ms, ok


@dataclass
class RunStats:
    lats_ms: List[float]
    errors: int
    completed: int
    cancelled: int


async def run_failfast(
    *,
    n: int,
    concurrency: int,
    timeout_s: float,
    deadline_s: float,
    error_budget: int,
    pool_max: int,
) -> RunStats:
    url = _http_url()
    dsn = _pg_dsn()

    sem = asyncio.Semaphore(concurrency)
    lats: List[float] = []
    errors = 0
    completed = 0
    cancelled = 0

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=pool_max)
    await _ensure_db(pool)

    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(limits=limits) as client:
        start = time.perf_counter()

        async def job(i: int) -> None:
            nonlocal errors, completed, cancelled
            try:
                async with sem:
                    # global deadline enforcement
                    if (time.perf_counter() - start) > deadline_s:
                        raise TimeoutError("global deadline exceeded")

                    lat_ms, ok = await _one_http(client, url, timeout_s)
                    lats.append(lat_ms)

                    if not ok:
                        errors += 1

                    # DB write (kept small)
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO ppb_async_failfast(job_id, http_ok, latency_ms) VALUES($1, $2, $3)",
                            i,
                            ok,
                            float(lat_ms),
                        )

                    completed += 1

                    # fail-fast when error budget exceeded
                    if errors > error_budget:
                        raise RuntimeError(f"error budget exceeded: {errors} > {error_budget}")

            except asyncio.CancelledError:
                cancelled += 1
                raise

        try:
            async with asyncio.TaskGroup() as tg:
                for i in range(n):
                    tg.create_task(job(i))
        except Exception:
            # TaskGroup will cancel remaining tasks automatically.
            pass

    await pool.close()
    return RunStats(lats_ms=lats, errors=errors, completed=completed, cancelled=cancelled)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--timeout", type=float, default=2.0, help="per-request timeout seconds")
    ap.add_argument("--deadline", type=float, default=5.0, help="global deadline seconds (fail-fast)")
    ap.add_argument("--error-budget", type=int, default=20, help="max errors before fail-fast")
    ap.add_argument("--pool-max", type=int, default=16)
    args = ap.parse_args()

    stats: RunStats = RunStats([], 0, 0, 0)
    results: list[Result] = []

    def _run() -> None:
        nonlocal stats
        stats = asyncio.run(
            run_failfast(
                n=args.n,
                concurrency=args.concurrency,
                timeout_s=args.timeout,
                deadline_s=args.deadline,
                error_budget=args.error_budget,
                pool_max=args.pool_max,
            )
        )

    wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(_run)
    m, p50, p95, p99 = summarize_latencies_ms(stats.lats_ms)
    results.append(
        Result(
            f"D4-failfast n={args.n} c={args.concurrency} deadline={args.deadline}s "
            f"errors={stats.errors} completed={stats.completed} cancelled={stats.cancelled}",
            wall,
            max(stats.completed, 1),
            (stats.completed / wall) if wall > 0 else 0.0,
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
    print("\nExpected:")
    print("- When deadline/error budget hits, TaskGroup cancels remaining tasks.")
    print("- This is the baseline for correct cancellation propagation in async services.")


if __name__ == "__main__":
    main()
