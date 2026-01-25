"""
C2: Pickle / IPC tax in multiprocessing: paths vs blobs.

Goal:
- Show why "sending big Python objects / numpy arrays to processes" is expensive.
- Show the production pattern: send small identifiers (paths / ids) and load inside worker.

Datasets (materialized by 00_fetch_and_materialize.py):
- NLP: ArXiv paper text files (CShorten/ML-ArXiv-Papers)
- Images: DocLayNet page images (docling-project/DocLayNet-v1.2)

What we measure:
- Wall time and system CPU while running
- Parent RSS delta (approx)
- Speed differences between:
    NLP:  (A) pass full text blobs   vs (B) pass file paths
    IMG:  (A) pass numpy arrays     vs (B) pass file paths

NLP modes:
- paths: send file path (tiny IPC), worker reads text
- blobs: send BIG string payloads (forced size via --blob-mb)

Image modes:
- paths: send file path, worker loads+resizes
- bytes: send BIG raw PNG bytes (forced IPC), worker decodes from bytes (no disk IO)
- arrays: send resized uint8 numpy array (forced IPC), worker computes metric (no decode)

Usage:
  python labs/multiprocessing/02_pickle_ipc_tax_paths_vs_payloads.py --kind nlp --items 500 --workers 8 --blob-mb 4
  python labs/multiprocessing/02_pickle_ipc_tax_paths_vs_payloads.py --kind image --items 250 --workers 8

Notes:
- This lab is intentionally "multiprocessing only" (threads are not the point here).
- Uses picklable top-level functions (not lambdas).
- Increase --blob-mb (e.g., 8, 16) to amplify pickle tax on NLP.
- Increase --items to amplify overhead.

"""
from __future__ import annotations

import argparse
import io
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable

import numpy as np
from PIL import Image

from _bench import measure, print_results

BASE = Path(__file__).parent / "_data"
ARXIV_DIR = BASE / "arxiv_text" / "docs"
DOCLAY_DIR = BASE / "doclaynet_images" / "images"

TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


# ---------------- CPU work (keep constant/small) ----------------
def _hash_tokens(tokens: list[str]) -> int:
    acc = 0
    for t in tokens[:12000]:
        h = 2166136261
        for ch in t:
            h ^= ord(ch)
            h = (h * 16777619) & 0xFFFFFFFF
        acc ^= h
    return acc


def nlp_work_from_text(text: str) -> int:
    tokens = TOKEN_RE.findall(text.lower())
    return _hash_tokens(tokens)


def nlp_work_from_path(path: str) -> int:
    text = Path(path).read_text(encoding="utf-8", errors="ignore")
    return nlp_work_from_text(text)


def image_metric_from_array(arr_u8: np.ndarray) -> float:
    arr = arr_u8.astype(np.float32) / 255.0
    gx = np.abs(arr[:, 1:] - arr[:, :-1])
    gy = np.abs(arr[1:, :] - arr[:-1, :])
    return float(gx.mean() + gy.mean() + 0.25 * arr.std())


def image_work_from_path(path: str) -> float:
    im = Image.open(path).convert("L")
    im = im.resize((768, 1024))
    arr = np.asarray(im, dtype=np.uint8)
    return image_metric_from_array(arr)


def image_work_from_png_bytes(png_bytes: bytes) -> float:
    im = Image.open(io.BytesIO(png_bytes)).convert("L")
    im = im.resize((768, 1024))
    arr = np.asarray(im, dtype=np.uint8)
    return image_metric_from_array(arr)


def image_work_from_array(arr_u8: np.ndarray) -> float:
    return image_metric_from_array(arr_u8)


# ---------------- Pool runners ----------------
def _run_pool(fn, items: Iterable, workers: int) -> None:
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(fn, x) for x in items]
        for f in as_completed(futs):
            _ = f.result()


# ---------------- Helpers ----------------
def _force_big_text(text: str, target_bytes: int) -> str:
    """
    Create a BIG string payload by repeating the same real text.
    This is not synthetic text; it's the same real doc repeated to hit size.
    """
    if target_bytes <= 0:
        return text
    # fast-ish growth
    chunk = text if text else "x"
    out = text
    while len(out.encode("utf-8", errors="ignore")) < target_bytes:
        out += "\n" + chunk
        # exponential-ish growth
        chunk = out
        if len(out) > 50_000_000:  # safety
            break
    return out


def _bytes_mb(b: bytes) -> float:
    return len(b) / (1024 * 1024)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=["nlp", "image"], default="nlp")
    ap.add_argument("--items", type=int, default=300)
    ap.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) // 4))
    ap.add_argument("--blob-mb", type=int, default=4, help="NLP blobs target size in MB per item")
    ap.add_argument("--print-sizes", action="store_true", help="Print payload sizes for sanity")
    args = ap.parse_args()

    if args.kind == "nlp":
        files = sorted(ARXIV_DIR.glob("*.txt"))[: args.items]
        if not files:
            raise SystemExit("ArXiv text files not found. Run 00_fetch_and_materialize.py first.")
        paths = [str(p) for p in files]

        # Build BIG blobs
        target = args.blob_mb * 1024 * 1024
        texts: list[str] = []
        for p in paths:
            t = Path(p).read_text(encoding="utf-8", errors="ignore")
            texts.append(_force_big_text(t, target))

        if args.print_sizes:
            sizes = [len(t.encode("utf-8", errors="ignore")) for t in texts[:5]]
            print(f"[nlp] example blob sizes (bytes): {sizes}")

        results = []
        results.append(
            measure(
                lambda: _run_pool(nlp_work_from_path, paths, args.workers),
                name=f"C2-procs(paths)-nlp w={args.workers}",
                items=len(paths),
            )
        )
        results.append(
            measure(
                lambda: _run_pool(nlp_work_from_text, texts, args.workers),
                name=f"C2-procs(blobs~{args.blob_mb}MB)-nlp w={args.workers}",
                items=len(texts),
            )
        )

        print_results(results)

        print("\nInterpretation (NLP):")
        print("- paths: tiny IPC payloads; workers read locally (typical production pattern).")
        print("- blobs: huge IPC payloads; pickle + pipe transfer dominates as size grows.")
        print("Try: increase --blob-mb to 8/16 and --items to 800 to magnify.")

    else:
        files = sorted(DOCLAY_DIR.glob("*.png"))[: args.items]
        if not files:
            raise SystemExit("DocLayNet images not found. Run 00_fetch_and_materialize.py first.")
        paths = [str(p) for p in files]

        # Build PNG bytes payloads (big IPC but avoids disk IO in workers)
        png_bytes: list[bytes] = []
        for p in paths:
            png_bytes.append(Path(p).read_bytes())

        # Build resized arrays payloads (also big IPC, no decode in worker)
        arrays: list[np.ndarray] = []
        for p in paths:
            im = Image.open(p).convert("L")
            im = im.resize((768, 1024))
            arrays.append(np.asarray(im, dtype=np.uint8))

        if args.print_sizes:
            print(f"[img] example png bytes MB: {[round(_bytes_mb(b), 2) for b in png_bytes[:5]]}")
            arr_mb = [a.nbytes / (1024 * 1024) for a in arrays[:5]]
            print(f"[img] example array MB: {[round(x, 2) for x in arr_mb]}")

        results = []
        results.append(
            measure(
                lambda: _run_pool(image_work_from_path, paths, args.workers),
                name=f"C2-procs(paths)-image w={args.workers}",
                items=len(paths),
            )
        )
        results.append(
            measure(
                lambda: _run_pool(image_work_from_png_bytes, png_bytes, args.workers),
                name=f"C2-procs(png_bytes)-image w={args.workers}",
                items=len(png_bytes),
            )
        )
        results.append(
            measure(
                lambda: _run_pool(image_work_from_array, arrays, args.workers),
                name=f"C2-procs(arrays_u8)-image w={args.workers}",
                items=len(arrays),
            )
        )

        print_results(results)

        print("\nInterpretation (Image):")
        print("- paths: small IPC but each worker pays decode+resize (often dominates at small payloads).")
        print("- png_bytes: big IPC + decode in worker (isolates IPC impact vs disk IO).")
        print("- arrays: big IPC but no decode (isolates pickle/IPC of numpy).")
        print("Try: increase --items to 400–800 to make IPC tax unavoidable.")

if __name__ == "__main__":
    main()