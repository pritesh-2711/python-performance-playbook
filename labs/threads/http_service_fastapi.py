"""
Local FastAPI service (real HTTP) for threads fan-out benchmarking.

Modes:
- mode=mem  (default): load + parse the data file ONCE at startup; requests use cached list
- mode=disk: load + parse on every request (intentionally heavy; demonstrates downstream bottlenecks)

Run:
  python labs/threads/http_service_fastapi.py --init
  uvicorn labs.threads.http_service_fastapi:app --host 127.0.0.1 --port 8008
"""

from __future__ import annotations

import argparse
import os
from bisect import bisect_left
from typing import Optional

from fastapi import FastAPI, Query

app = FastAPI(title="PPB Threads HTTP Service", version="1.0")

DATA_DIR = os.path.join(os.path.dirname(__file__), "_data")
DATA_FILE = os.path.join(DATA_DIR, "sorted_ints.txt")

# Cached in-memory data (loaded at startup)
CACHED: Optional[list[int]] = None


def ensure_data_file(n: int = 2_000_000) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(DATA_FILE):
        return
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write(f"{i * 2}\n")


def load_sorted_ints() -> list[int]:
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return [int(line) for line in f]


def binary_search(arr: list[int], target: int) -> bool:
    idx = bisect_left(arr, target)
    return idx < len(arr) and arr[idx] == target


@app.on_event("startup")
def startup() -> None:
    global CACHED
    if not os.path.exists(DATA_FILE):
        ensure_data_file()
    # Real file I/O once. This makes HTTP benchmarking meaningful.
    CACHED = load_sorted_ints()


@app.get("/search")
def search(
    target: int = Query(...),
    repeat: int = Query(3, ge=1, le=50),
    mode: str = Query("mem", pattern="^(mem|disk)$"),
) -> dict:
    if mode == "disk":
        arr = load_sorted_ints()
    else:
        # cached path
        assert CACHED is not None
        arr = CACHED

    found = False
    for _ in range(repeat):
        found = binary_search(arr, target)

    return {"found": found, "target": target, "repeat": repeat, "n": len(arr), "mode": mode}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--init", action="store_true")
    p.add_argument("--n", type=int, default=2_000_000)
    args = p.parse_args()
    if args.init:
        ensure_data_file(n=args.n)
        print(f"Initialized: {DATA_FILE} with {args.n} lines")


if __name__ == "__main__":
    main()
