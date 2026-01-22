"""
Lab 01 (Threads): Real HTTP fan-out vs serial

Compares:
- Serial HTTP GETs to local FastAPI service
- ThreadPoolExecutor fan-out

Measures:
- total time
- throughput (req/s)
- latency percentiles (p50/p95/p99)

Prereq:
1) Start server:
   uvicorn labs.threads.http_service_fastapi:app --host 127.0.0.1 --port 8008

2) Then run:
   python labs/threads/01_io_bound_speedup_http.py

Notes:
- This is real HTTP. It will highlight both client and server bottlenecks.
- If you increase workers too much, the server/OS may become the limiter.
"""

from __future__ import annotations

import argparse
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx


def pct(values: list[float], p: float) -> float:
    s = sorted(values)
    k = int((p / 100.0) * (len(s) - 1))
    return s[k]


def one_request(client: httpx.Client, url: str, target: int, repeat: int, mode: str) -> float:
    t0 = time.perf_counter()
    r = client.get(url, params={"target": target, "repeat": repeat, "mode": mode}, timeout=60.0)
    r.raise_for_status()
    _ = r.json()
    return time.perf_counter() - t0


def summarize(lat: list[float], wall_s: float) -> dict:
    n = len(lat)
    return {
        "n": n,
        "wall_s": wall_s,
        "rps": (n / wall_s) if wall_s > 0 else 0.0,
        "mean_ms": (statistics.mean(lat) * 1000.0) if n else float("nan"),
        "p50_ms": pct(lat, 50) * 1000.0,
        "p95_ms": pct(lat, 95) * 1000.0,
        "p99_ms": pct(lat, 99) * 1000.0,
    }


def print_report(title: str, r: dict) -> None:
    print(f"\n{title}")
    print(f"Requests : {r['n']}")
    print(f"Wall     : {r['wall_s']:.3f}s")
    print(f"RPS      : {r['rps']:.2f}")
    print(f"Mean     : {r['mean_ms']:.2f} ms")
    print(f"P50/P95/P99: {r['p50_ms']:.2f} / {r['p95_ms']:.2f} / {r['p99_ms']:.2f} ms")


def run_serial(url: str, n: int, repeat: int, mode: str) -> dict:
    lat: list[float] = []
    t0 = time.perf_counter()
    with httpx.Client() as client:
        for i in range(n):
            target = (i * 2)
            lat.append(one_request(client, url, target, repeat, mode))
    wall = time.perf_counter() - t0
    return summarize(lat, wall)


def run_threads(url: str, n: int, repeat: int, workers: int, mode: str) -> dict:
    lat: list[float] = []
    t0 = time.perf_counter()
    with httpx.Client() as client:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = []
            for i in range(n):
                target = (i * 2)
                futs.append(ex.submit(one_request, client, url, target, repeat, mode))
            for f in as_completed(futs):
                lat.append(f.result())
    wall = time.perf_counter() - t0
    return summarize(lat, wall)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://127.0.0.1:8008/search", help="FastAPI search endpoint")
    p.add_argument("--n", type=int, default=200, help="Number of requests")
    p.add_argument("--repeat", type=int, default=3, help="Binary search repeat per request")
    p.add_argument("--mode", default="mem", choices=["mem", "disk"], help="Server mode: mem cached vs disk reload")
    p.add_argument("--workers", default="1,2,4,8,16,32", help="Thread workers to test")
    args = p.parse_args()

    print(f"Target URL: {args.url}")
    print(f"Requests: {args.n} | repeat: {args.repeat} | mode: {args.mode}")

    serial = run_serial(args.url, args.n, args.repeat, args.mode)
    print_report("Serial", serial)
    baseline_rps = serial["rps"]

    for w in [int(x.strip()) for x in args.workers.split(",") if x.strip()]:
        threaded = run_threads(args.url, args.n, args.repeat, w, args.mode)
        print_report(f"Threads({w})  speedup={threaded['rps']/baseline_rps:.2f}x", threaded)


if __name__ == "__main__":
    main()
