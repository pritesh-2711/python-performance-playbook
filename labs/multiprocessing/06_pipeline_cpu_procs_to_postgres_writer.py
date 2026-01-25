"""
C6: Multiprocessing CPU pipeline -> Postgres writer.

Input: ArXiv docs (materialized by 00_fetch_and_materialize.py)
Work: compute small features per doc (CPU-bound enough)
Output: write features into Postgres

Modes:
- naive: each worker writes to DB (many writers)
- writer: workers compute only; a single writer batches inserts (recommended)

Usage:
    python labs/multiprocessing/06_postgres_setup.py

    python labs/multiprocessing/06_pipeline_cpu_procs_to_postgres_writer.py --mode naive  --items 1200 --workers 8 --payload-kb 16 --commit-every 1
    python labs/multiprocessing/06_pipeline_cpu_procs_to_postgres_writer.py --mode writer --items 1200 --workers 8 --payload-kb 16 --batch 200 --qmax 1000
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from multiprocessing import get_context
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from _bench import measure, print_results

BASE = Path(__file__).parent / "_data"
ARXIV_DIR = BASE / "arxiv_text" / "docs"
TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")

@dataclass(frozen=True)
class FeatureRow:
    doc_id: str
    tokens_bigint: int
    hash32: int
    bytes_len: int
    payload: str

def compute_features(path: str, payload_kb: int) -> FeatureRow:
    p = Path(path)
    text = p.read_text(encoding="utf-8", errors="ignore")
    tokens = TOKEN_RE.findall(text.lower())

    acc = 0
    for t in tokens[:25000]:
        h = 2166136261
        for ch in t:
            h ^= ord(ch)
            h = (h * 16777619) & 0xFFFFFFFF
        acc ^= h

    # realistic "payload": store first N KB of content + doc_id marker
    # this creates WAL + index work (very common in metadata stores)
    payload = (f"{p.stem}\n" + text)[: payload_kb * 1024]

    return FeatureRow(
        doc_id=p.stem,
        tokens_bigint=len(tokens),
        hash32=acc,
        bytes_len=len(text.encode("utf-8", errors="ignore")),
        payload=payload,
    )

UPSERT_SQL = """
INSERT INTO ppb_mp_features (doc_id, tokens_bigint, hash32, bytes_len, payload)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (doc_id) DO UPDATE SET
  tokens_bigint = EXCLUDED.tokens_bigint,
  hash32 = EXCLUDED.hash32,
  bytes_len = EXCLUDED.bytes_len,
  payload = EXCLUDED.payload;
"""

def writer_loop(dsn: str, q, batch: int) -> None:
    buf: list[FeatureRow] = []
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            while True:
                item = q.get()
                if item is None:
                    break
                buf.append(item)
                if len(buf) >= batch:
                    cur.executemany(
                        UPSERT_SQL,
                        [(r.doc_id, r.tokens_bigint, r.hash32, r.bytes_len, r.payload) for r in buf],
                    )
                    conn.commit()
                    buf.clear()

            if buf:
                cur.executemany(
                    UPSERT_SQL,
                    [(r.doc_id, r.tokens_bigint, r.hash32, r.bytes_len, r.payload) for r in buf],
                )
                conn.commit()

def worker_naive(paths: list[str], dsn: str, payload_kb: int, commit_every: int) -> None:
    # many writers; commit_every controls worst-case anti-pattern
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            n = 0
            for p in paths:
                r = compute_features(p, payload_kb)
                cur.execute(UPSERT_SQL, (r.doc_id, r.tokens_bigint, r.hash32, r.bytes_len, r.payload))
                n += 1
                if commit_every > 0 and (n % commit_every == 0):
                    conn.commit()
            conn.commit()

def worker_compute_only(paths: list[str], q, payload_kb: int) -> None:
    for p in paths:
        q.put(compute_features(p, payload_kb))

def chunk_paths(paths: list[str], chunks: int) -> list[list[str]]:
    n = len(paths)
    out = []
    for i in range(chunks):
        out.append(paths[i*n//chunks : (i+1)*n//chunks])
    return [c for c in out if c]

def run_mode_naive(paths: list[str], dsn: str, workers: int, payload_kb: int, commit_every: int) -> None:
    ctx = get_context("fork")
    groups = chunk_paths(paths, workers)
    ps = []
    for g in groups:
        p = ctx.Process(target=worker_naive, args=(g, dsn, payload_kb, commit_every))
        p.start()
        ps.append(p)

    failed = []
    for p in ps:
        p.join()
        if p.exitcode != 0:
            failed.append(p.exitcode)

    if failed:
        raise RuntimeError(f"naive workers failed, exitcodes={failed}")


def run_mode_writer(paths: list[str], dsn: str, workers: int, payload_kb: int, batch: int, qmax: int) -> None:
    ctx = get_context("fork")
    q = ctx.Queue(maxsize=qmax)

    wp = ctx.Process(target=writer_loop, args=(dsn, q, batch))
    wp.start()

    groups = chunk_paths(paths, workers)
    ps = []
    for g in groups:
        p = ctx.Process(target=worker_compute_only, args=(g, q, payload_kb))
        p.start()
        ps.append(p)

    failed = []
    for p in ps:
        p.join()
        if p.exitcode != 0:
            failed.append(p.exitcode)

    # Always stop writer
    q.put(None)
    wp.join()

    if wp.exitcode != 0:
        raise RuntimeError(f"writer failed, exitcode={wp.exitcode}")
    if failed:
        raise RuntimeError(f"compute workers failed, exitcodes={failed}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["naive", "writer"], default="writer")
    ap.add_argument("--items", type=int, default=1200)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--qmax", type=int, default=1000)
    ap.add_argument("--payload-kb", type=int, default=16, help="bigger => DB becomes bottleneck")
    ap.add_argument("--commit-every", type=int, default=1, help="naive only: commit every N rows (1 is worst)")
    args = ap.parse_args()

    load_dotenv()
    dsn = os.getenv("PPB_PG_DSN")
    if not dsn:
        raise SystemExit("Set PPB_PG_DSN in env/.env")

    files = sorted(ARXIV_DIR.glob("*.txt"))[: args.items]
    if not files:
        raise SystemExit("ArXiv dataset not found. Run 00_fetch_and_materialize.py first.")
    paths = [str(p) for p in files]

    results = []
    if args.mode == "naive":
        results.append(
            measure(
                lambda: run_mode_naive(paths, dsn, args.workers, args.payload_kb, args.commit_every),
                name=f"C6-naive writers={args.workers} payload={args.payload_kb}KB commitEvery={args.commit_every}",
                items=len(paths),
            )
        )
    else:
        results.append(
            measure(
                lambda: run_mode_writer(paths, dsn, args.workers, args.payload_kb, args.batch, args.qmax),
                name=f"C6-writer workers={args.workers} payload={args.payload_kb}KB batch={args.batch}",
                items=len(paths),
            )
        )

    print_results(results)

    print("\nExpected:")
    print("- naive + commitEvery=1: worst-case (per-row commit) => slow, lock/WAL heavy, unstable at scale.")
    print("- writer + batching: stable; DB work is centralized + batched; queue provides backpressure.")

if __name__ == "__main__":
    main()
