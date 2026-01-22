"""
Lab 03 (Threads): Postgres lock contention (hot row) kills throughput and tail latency.

- Reads DB config from labs/threads/.env
- Creates tables + seeds counters automatically
- Measures:
  - ops/s
  - p50/p95/p99

Run:
  python labs/threads/03_lock_contention_postgres.py

Cases:
1) HOT: all threads update row id=1
2) SHARDED: threads update distinct rows (2..2+shards-1)
"""

from __future__ import annotations

import argparse
import os
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
        "ops_s": (n / total) if total > 0 else 0.0,
    }


def print_report(title: str, r: dict) -> None:
    print(f"\n{title}")
    print(f"Ops      : {r['n']}")
    print(f"Total    : {r['total_s']:.3f}s")
    print(f"Ops/s    : {r['ops_s']:.2f}")
    print(f"Mean     : {r['mean_ms']:.2f} ms")
    print(f"P50/P95/P99: {r['p50_ms']:.2f} / {r['p95_ms']:.2f} / {r['p99_ms']:.2f} ms")


UPDATE_SQL = "UPDATE ppb_counters SET value = value + 1 WHERE id = %s;"


def one_update(dsn: str, row_id: int) -> float:
    t0 = time.perf_counter()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(UPDATE_SQL, (row_id,))
            conn.commit()
    return time.perf_counter() - t0


def run_case(dsn: str, workers: int, ops_per_worker: int, mode: str, shards: int) -> dict:
    lat: list[float] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = []
        for w in range(workers):
            for _ in range(ops_per_worker):
                if mode == "hot":
                    row_id = 1
                else:
                    row_id = 2 + (w % shards)
                futs.append(ex.submit(one_update, dsn, row_id))
        for f in as_completed(futs):
            lat.append(f.result())
    return summarize(lat)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--ops-per-worker", type=int, default=80)
    p.add_argument("--shards", type=int, default=64)
    args = p.parse_args()

    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    dsn = setup_db(dotenv_path)

    hot = run_case(dsn, args.workers, args.ops_per_worker, "hot", args.shards)
    print_report(f"HOT ROW (workers={args.workers})", hot)

    sharded = run_case(dsn, args.workers, args.ops_per_worker, "sharded", args.shards)
    print_report(f"SHARDED (workers={args.workers}, shards={args.shards})", sharded)

    print("\nExpected:")
    print("- HOT: row lock contention -> worse tail latency, limited ops/s.")
    print("- SHARDED: contention reduced -> better scaling.")


if __name__ == "__main__":
    main()
