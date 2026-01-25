"""
C5: Process pool chunking / batching for NLP tasks

This lab shows BOTH regimes:

Regime A (task-overhead dominated):
- per-item work is small
- chunking reduces scheduling/IPC overhead => throughput improves up to a point

Regime B (load-balance / tail dominated):
- per-item work is heavy and heterogeneous
- chunking creates fewer, larger tasks => stragglers dominate => throughput degrades

We reuse from previous labs:
- CShorten/ML-ArXiv-Papers materialized as .txt files

Usage:
  # Tiny task regime (chunking should help)
  python labs/multiprocessing/05_process_pool_chunking_nlp.py --items 1200 --workers 8 --repeat 10 --token-cap 2000 --chunks 1,2,5,10,20,50

  # Heavy task regime (chunking may hurt; stragglers)
  python labs/multiprocessing/05_process_pool_chunking_nlp.py --items 1200 --workers 8 --repeat 200 --token-cap 20000 --chunks 1,2,5,10,20,50
"""

from __future__ import annotations

import argparse
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Sequence

from _bench import measure, print_results

BASE = Path(__file__).parent / "_data"
ARXIV_DIR = BASE / "arxiv_text" / "docs"

TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def _hash_tokens(tokens: list[str], token_cap: int) -> int:
    acc = 0
    # cap controls task weight (helps show both regimes)
    for t in tokens[:token_cap]:
        h = 2166136261
        for ch in t:
            h ^= ord(ch)
            h = (h * 16777619) & 0xFFFFFFFF
        acc ^= h
    return acc


def nlp_one(args: tuple[str, int, int]) -> int:
    """
    Single-doc CPU task.
    args = (path, repeat, token_cap)
    """
    path, repeat, token_cap = args
    text = Path(path).read_text(encoding="utf-8", errors="ignore")
    tokens = TOKEN_RE.findall(text.lower())

    out = 0
    for _ in range(repeat):
        out ^= _hash_tokens(tokens, token_cap)
    return out


def nlp_chunk(paths: list[str], repeat: int, token_cap: int) -> int:
    """
    Chunk CPU task: process multiple docs in one process task.
    """
    out = 0
    for p in paths:
        out ^= nlp_one((p, repeat, token_cap))
    return out


def _chunks(seq: Sequence[str], chunk_size: int) -> List[List[str]]:
    return [list(seq[i : i + chunk_size]) for i in range(0, len(seq), chunk_size)]


def run_chunked(paths: list[str], workers: int, repeat: int, token_cap: int, chunk_size: int) -> None:
    groups = _chunks(paths, chunk_size)
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(nlp_chunk, g, repeat, token_cap) for g in groups]
        for f in as_completed(futs):
            _ = f.result()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--items", type=int, default=1200)
    ap.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) // 4))
    ap.add_argument("--repeat", type=int, default=20, help="repeat passes per doc (controls task weight)")
    ap.add_argument("--token-cap", type=int, default=20000, help="max tokens per doc to hash (controls task weight)")
    ap.add_argument("--chunks", default="1,2,5,10,20,50", help="chunk-size sweep")
    args = ap.parse_args()

    files = sorted(ARXIV_DIR.glob("*.txt"))[: args.items]
    if not files:
        raise SystemExit("ArXiv dataset not found. Run 00_fetch_and_materialize.py first.")
    paths = [str(p) for p in files]

    chunk_list = [int(x) for x in args.chunks.split(",")]

    print(f"Items={len(paths)} | workers={args.workers} | repeat={args.repeat} | token_cap={args.token_cap}")
    results = []

    for cs in chunk_list:
        tasks = (len(paths) + cs - 1) // cs
        print(f"[chunk={cs}] tasks={tasks}")

        results.append(
            measure(
                lambda cs=cs: run_chunked(paths, args.workers, args.repeat, args.token_cap, cs),
                name=f"C5-procs(chunk={cs}) w={args.workers}",
                items=len(paths),
            )
        )

    print_results(results)

    print("\nHow to read this:")
    print("- If tasks are tiny: chunking reduces scheduling/IPC overhead => throughput improves up to a point.")
    print("- If tasks are heavy/heterogeneous: chunking reduces parallelism and increases stragglers => throughput can degrade.")
    print("\nExpected:")
    print("- There is no universally best chunk size; it depends on task weight + variance + worker count.")


if __name__ == "__main__":
    main()
