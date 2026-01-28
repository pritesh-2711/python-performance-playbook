from __future__ import annotations

import argparse
import asyncio
import os
import time
from dataclasses import dataclass
from typing import List, Tuple

import asyncpg
import httpx

from _bench import Result, run_with_sys_sampling, print_results, summarize_latencies_ms
from dotenv import load_dotenv
load_dotenv()

DDL = """
CREATE TABLE IF NOT EXISTS ppb_async_events (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  query TEXT NOT NULL,
  http_ok BOOLEAN NOT NULL,
  latency_ms REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS ppb_async_events_created_at_idx
ON ppb_async_events(created_at);
"""

INSERT_SQL = """
INSERT INTO ppb_async_events (query, http_ok, latency_ms)
VALUES ($1, $2, $3);
"""


@dataclass(frozen=True)
class Event:
    query: str
    http_ok: bool
    latency_ms: float


async def ensure_db(dsn: str, reset: bool) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        if reset:
            await conn.execute("DROP TABLE IF EXISTS ppb_async_events;")
        await conn.execute(DDL)
    finally:
        await conn.close()


async def fetch_one(client: httpx.AsyncClient, url: str, q: str) -> Event:
    t0 = time.perf_counter()
    try:
        r = await client.get(url, params={"q": q})
        ok = r.status_code == 200
    except Exception:
        ok = False
    latency = (time.perf_counter() - t0) * 1000.0
    return Event(query=q, http_ok=ok, latency_ms=latency)


# ---------------- MODES ----------------
async def run_naive(
    *,
    n: int,
    http_conc: int,
    pool: asyncpg.Pool,
    url: str,
) -> List[float]:
    """
    One task does: HTTP then DB insert.
    No explicit backpressure between HTTP fanout and DB.
    Returns per-item end-to-end latencies (ms) for stats.
    """
    sem = asyncio.Semaphore(http_conc)
    latencies: List[float] = []

    async with httpx.AsyncClient(timeout=5.0) as client:

        async def one(i: int) -> None:
            async with sem:
                t0 = time.perf_counter()
                ev = await fetch_one(client, url, f"q-{i}")
                async with pool.acquire() as conn:
                    await conn.execute(INSERT_SQL, ev.query, ev.http_ok, ev.latency_ms)
                latencies.append((time.perf_counter() - t0) * 1000.0)

        await asyncio.gather(*[asyncio.create_task(one(i)) for i in range(n)])

    return latencies


async def run_writer(
    *,
    n: int,
    http_conc: int,
    pool: asyncpg.Pool,
    url: str,
    qmax: int,
    batch: int,
) -> List[float]:
    """
    Producer (HTTP) -> bounded queue -> single writer that batches inserts.
    Queue maxsize enforces backpressure.
    Returns per-item end-to-end latencies (ms).
    """
    q: asyncio.Queue[Tuple[Event, float]] = asyncio.Queue(maxsize=qmax)
    sem = asyncio.Semaphore(http_conc)
    stop = object()
    latencies: List[float] = []

    async with httpx.AsyncClient(timeout=5.0) as client:

        async def producer(i: int) -> None:
            async with sem:
                t0 = time.perf_counter()
                ev = await fetch_one(client, url, f"q-{i}")
                await q.put((ev, t0))

        async def writer() -> None:
            buf: List[Tuple[Event, float]] = []
            async with pool.acquire() as conn:
                while True:
                    item = await q.get()
                    if item is stop:
                        break
                    buf.append(item)

                    if len(buf) >= batch:
                        await conn.executemany(
                            INSERT_SQL,
                            [(e.query, e.http_ok, e.latency_ms) for (e, _) in buf],
                        )
                        for _, t0 in buf:
                            latencies.append((time.perf_counter() - t0) * 1000.0)
                        buf.clear()

                if buf:
                    await conn.executemany(
                        INSERT_SQL,
                        [(e.query, e.http_ok, e.latency_ms) for (e, _) in buf],
                    )
                    for _, t0 in buf:
                        latencies.append((time.perf_counter() - t0) * 1000.0)

        writer_task = asyncio.create_task(writer())
        await asyncio.gather(*[asyncio.create_task(producer(i)) for i in range(n)])
        await q.put(stop)  # type: ignore[arg-type]
        await writer_task

    return latencies


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["naive", "writer"], required=True)
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--http-conc", type=int, default=32)
    ap.add_argument("--pool-max", type=int, default=16)
    ap.add_argument("--qmax", type=int, default=1000)
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    dsn = os.getenv("PPB_PG_DSN_ASYNC")
    if not dsn:
        raise SystemExit("Set PPB_PG_DSN_ASYNC in labs/async/.env (or env).")

    base = os.getenv("PPB_HTTP_BASE", "http://127.0.0.1:8008")
    path = os.getenv("PPB_HTTP_PATH", "/search")
    url = base + path

    async def _run_and_get_latencies() -> List[float]:
        await ensure_db(dsn, args.reset)
        pool = await asyncpg.create_pool(dsn, max_size=args.pool_max)
        try:
            if args.mode == "naive":
                return await run_naive(n=args.n, http_conc=args.http_conc, pool=pool, url=url)
            return await run_writer(
                n=args.n,
                http_conc=args.http_conc,
                pool=pool,
                url=url,
                qmax=args.qmax,
                batch=args.batch,
            )
        finally:
            await pool.close()

    latencies: List[float] = []

    def _run():
        nonlocal latencies
        latencies = asyncio.run(_run_and_get_latencies())

    name = (
        f"D5-naive http={args.http_conc} pool={args.pool_max}"
        if args.mode == "naive"
        else f"D5-writer http={args.http_conc} pool={args.pool_max} batch={args.batch} qmax={args.qmax}"
    )

    wall, cpu_avg, cpu_peak, rss_delta = run_with_sys_sampling(_run)

    mean_ms, p50, p95, p99 = summarize_latencies_ms(latencies)

    res = Result(
        name=name,
        wall_s=wall,
        n=args.n,
        per_s=(args.n / wall) if wall > 0 else 0.0,
        mean_ms=mean_ms,
        p50_ms=p50,
        p95_ms=p95,
        p99_ms=p99,
        rss_delta_mb=rss_delta,
        cpu_avg_pct=cpu_avg,
        cpu_peak_pct=cpu_peak,
    )

    print_results([res])


    print("\nHow to read this:")
    print("- naive: every task does HTTP + DB write => DB becomes bottleneck; no smoothing/backpressure.")
    print("- writer: bounded queue + batching => stable and production-safe; tune batch/qmax.")
    print("- Watch p95/p99: if they explode, you’re over-driving DB or queue is too large.")


if __name__ == "__main__":
    main()
