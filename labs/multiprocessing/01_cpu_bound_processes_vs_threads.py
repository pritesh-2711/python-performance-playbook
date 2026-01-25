"""
C1: CPU-bound work: compare serial vs threads vs processes using REAL datasets.

Datasets (materialized by 00_fetch_and_materialize.py):
- NLP: ArXiv paper text files (CShorten/ML-ArXiv-Papers)
- Images: DocLayNet page images (docling-project/DocLayNet-v1.2)

Usage:
  python labs/multiprocessing/01_cpu_bound_processes_vs_threads.py --kind nlp --items 300 --repeat 40 --workers 1,2,4,8,16
  python labs/multiprocessing/01_cpu_bound_processes_vs_threads.py --kind image --items 120 --workers 1,2,4,8,16
"""

from __future__ import annotations

import argparse
import os  # noqa: F401
import re
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
from PIL import Image

from _bench import measure, print_results

BASE = Path(__file__).parent / "_data"
ARXIV_DIR = BASE / "arxiv_text" / "docs"
DOCLAY_DIR = BASE / "doclaynet_images" / "images"

TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


# ---------------- NLP CPU work (ArXiv) ----------------
def nlp_feature_hash(text: str) -> int:
    # pure-python CPU work: feature hash accumulation
    tokens = TOKEN_RE.findall(text.lower())
    acc = 0
    for t in tokens[:25000]:
        h = 2166136261
        for ch in t:
            h ^= ord(ch)
            h = (h * 16777619) & 0xFFFFFFFF
        acc ^= h
    return acc


def nlp_worker(args: tuple[str, int]) -> int:
    path, repeat = args
    txt = Path(path).read_text(encoding="utf-8", errors="ignore")
    acc = 0
    for _ in range(repeat):
        acc ^= nlp_feature_hash(txt)
    return acc


# ---------------- Image CPU work (DocLayNet pages) ----------------
def image_worker(path: str) -> float:
    # realistic doc-image CPU preprocessing (vectorized):
    # load -> grayscale -> resize -> gradients -> summary statistic
    im = Image.open(path).convert("L")
    im = im.resize((768, 1024))  # standardize shape
    arr = np.asarray(im, dtype=np.float32) / 255.0

    # gradients (vectorized)
    gx = np.abs(arr[:, 1:] - arr[:, :-1])
    gy = np.abs(arr[1:, :] - arr[:-1, :])

    # "edge density" + "contrast" like metrics
    edge = float(gx.mean() + gy.mean())
    contrast = float(arr.std())

    # return single scalar to keep IPC small
    return edge + 0.25 * contrast


# ---------------- runners ----------------
def run_serial_nlp(paths: list[str], repeat: int) -> None:
    for p in paths:
        _ = nlp_worker((p, repeat))


def run_threads_nlp(paths: list[str], repeat: int, workers: int) -> None:
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(nlp_worker, (p, repeat)) for p in paths]
        for f in as_completed(futs):
            _ = f.result()


def run_processes_nlp(paths: list[str], repeat: int, workers: int) -> None:
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(nlp_worker, (p, repeat)) for p in paths]
        for f in as_completed(futs):
            _ = f.result()


def run_serial_image(paths: list[str]) -> None:
    for p in paths:
        _ = image_worker(p)


def run_threads_image(paths: list[str], workers: int) -> None:
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(image_worker, p) for p in paths]
        for f in as_completed(futs):
            _ = f.result()


def run_processes_image(paths: list[str], workers: int) -> None:
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(image_worker, p) for p in paths]
        for f in as_completed(futs):
            _ = f.result()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=["nlp", "image"], default="nlp")
    ap.add_argument("--items", type=int, default=120)
    ap.add_argument("--repeat", type=int, default=20, help="NLP only: repeat passes to make CPU-bound")
    ap.add_argument("--workers", default="1,2,4,8,16", help="worker sweep")
    args = ap.parse_args()

    if args.kind == "nlp":
        files = sorted(ARXIV_DIR.glob("*.txt"))[: args.items]
    else:
        files = sorted(DOCLAY_DIR.glob("*.png"))[: args.items]

    if not files:
        raise SystemExit("Dataset not found. Run 00_fetch_and_materialize.py first.")

    paths = [str(p) for p in files]
    worker_list = [int(x) for x in args.workers.split(",")]

    results = []

    if args.kind == "nlp":
        results.append(
            measure(lambda: run_serial_nlp(paths, args.repeat), name="C1-serial-nlp", items=len(paths))
        )
        for w in worker_list:
            results.append(
                measure(lambda w=w: run_threads_nlp(paths, args.repeat, w), name=f"C1-threads({w})-nlp", items=len(paths))
            )
            results.append(
                measure(lambda w=w: run_processes_nlp(paths, args.repeat, w), name=f"C1-procs({w})-nlp", items=len(paths))
            )
    else:
        results.append(
            measure(lambda: run_serial_image(paths), name="C1-serial-image", items=len(paths))
        )
        for w in worker_list:
            results.append(
                measure(lambda w=w: run_threads_image(paths, w), name=f"C1-threads({w})-image", items=len(paths))
            )
            results.append(
                measure(lambda w=w: run_processes_image(paths, w), name=f"C1-procs({w})-image", items=len(paths))
            )

    print_results(results)

    print("\nExpected:")
    print("- Threads: limited scaling for CPU-bound work (GIL / contention).")
    print("- Processes: improves until CPU saturation; then overhead dominates.")
    if args.kind == "nlp":
        print("- If NLP is still too fast, increase --repeat (e.g., 40/80).")


if __name__ == "__main__":
    main()
