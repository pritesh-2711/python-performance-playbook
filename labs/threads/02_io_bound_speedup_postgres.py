"""
Lab 02 (Threads): Real PostgreSQL SELECT+JOIN fan-out vs serial

- Reads DB config from labs/threads/.env
- Creates required tables and seeds data automatically
- Uses a real SELECT+JOIN query (no pg_sleep)

Run:
  python labs/threads/02_io_bound_speedup_postgres.py

What it measures:
- QPS and p50/p95/p99 for running many queries
- Scaling as you increase ThreadPoolExecutor workers

Important:
- One connection per query (simple + correct)
- Pooling will be introduced later as an optimization pattern (still threads)
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg

from setup import setup_db


def pct(values: list[float], p: float) -> float:
    s = sorted(values)
    k = int((p / 100.0) * (len(s) - 1))
    return s[k]


def summarize(lat: list[float]) -> dict:
    total = sum(lat)
    n = len(lat)
    return {
        "n": n,
        "total_s": total,
        "mean_ms": statistics.mean(lat) * 1000.0,
        "p50_ms": pct(lat, 50) * 1000.0,
        "p95_ms": pct(lat, 95) * 1000.0,
        "p99_ms": pct(lat, 99) * 1000.0,
        "qps": (n / total) if total > 0 else 0.0,
    }


def print_report(title: str, r: dict) -> None:
    print(f"\n{title}")
    print(f"Queries  : {r['n']}")
    print(f"Total    : {r['total_s']:.3f}s")
    print(f"QPS      : {r['qps']:.2f}")
    print(f"Mean     : {r['mean_ms']:.2f} ms")
    print(f"P50/P95/P99: {r['p50_ms']:.2f} / {r['p95_ms']:.2f} / {r['p99_ms']:.2f} ms")


SQL = """
SELECT u.user_id, u.email, e.event_type, e.created_at
FROM ppb_users u
JOIN ppb_events e ON e.user_id = u.user_id
WHERE u.user_id = %s
ORDER BY e.created_at DESC
LIMIT 1;
"""


def one_query(dsn: str, user_id: int) -> float:
    t0 = time.perf_counter()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL, (user_id,))
            cur.fetchone()
    return time.perf_counter() - t0


def run_serial(dsn: str, user_ids: list[int]) -> dict:
    lat = [one_query(dsn, uid) for uid in user_ids]
    return summarize(lat)


def run_threads(dsn: str, user_ids: list[int], workers: int) -> dict:
    lat: list[float] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(one_query, dsn, uid) for uid in user_ids]
        for f in as_completed(futs):
            lat.append(f.result())
    return summarize(lat)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=2000, help="Number of queries")
    p.add_argument("--workers", default="1,2,4,8,16,32", help="Thread workers to test")
    p.add_argument("--seed", type=int, default=42, help="Random seed for user_id selection")
    args = p.parse_args()

    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    dsn = setup_db(dotenv_path)

    rng = random.Random(args.seed)
    # user_ids in range seeded by setup (1..50_000)
    user_ids = [rng.randint(1, 50_000) for _ in range(args.n)]

    print(f"DB ready. Queries: {args.n}")
    serial = run_serial(dsn, user_ids)
    print_report("Serial", serial)
    baseline = serial["qps"]

    for w in [int(x.strip()) for x in args.workers.split(",") if x.strip()]:
        threaded = run_threads(dsn, user_ids, w)
        print_report(f"Threads({w})  speedup={threaded['qps']/baseline:.2f}x", threaded)


if __name__ == "__main__":
    main()
