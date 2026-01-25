"""
C4: Shared memory vs pickle-copy for multiprocessing (DocLayNet pages).

Real scenario:
- You have a batch of document page images (DocLayNet).
- You want to run CPU preprocessing / feature extraction in parallel.
- Naive approach: send numpy arrays to worker processes => pickle + copy overhead.
- Better approach: put arrays in shared memory and send only indices/offsets.

This lab:
- Preloads N resized grayscale pages as uint8 arrays (768x1024).
- Compares:
  A) procs(arrays_u8): sends arrays (pickle/copy)
  B) procs(sharedmem): sends offsets; workers read from shared memory

Usage:
  python labs/multiprocessing/04_shared_memory_doc_images.py --items 400 --workers 8
  python labs/multiprocessing/04_shared_memory_doc_images.py --items 800 --workers 16

Notes:
- I have tried this on Linux. Shared memory should work on macOS/Windows too.
- This is a core production pattern for high-throughput pipelines.
"""

from __future__ import annotations

import argparse
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

import numpy as np
from PIL import Image
from multiprocessing import shared_memory

from _bench import measure, print_results

BASE = Path(__file__).parent / "_data"
DOCLAY_DIR = BASE / "doclaynet_images" / "images"

W, H = 768, 1024
DTYPE = np.uint8
BYTES_PER_IMG = W * H  # uint8


def image_metric_from_array(arr_u8: np.ndarray) -> float:
    arr = arr_u8.astype(np.float32) / 255.0
    gx = np.abs(arr[:, 1:] - arr[:, :-1])
    gy = np.abs(arr[1:, :] - arr[:-1, :])
    return float(gx.mean() + gy.mean() + 0.25 * arr.std())


# ---- mode A: pickle/copy arrays ----
def worker_array(arr_u8: np.ndarray) -> float:
    return image_metric_from_array(arr_u8)


def run_pool_arrays(arrays: List[np.ndarray], workers: int) -> None:
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(worker_array, a) for a in arrays]
        for f in as_completed(futs):
            _ = f.result()


# ---- mode B: shared memory ----
def worker_shm(args: Tuple[str, int]) -> float:
    shm_name, offset = args
    shm = shared_memory.SharedMemory(name=shm_name)

    # Create explicit views and release them deterministically
    buf_mv = None
    slice_mv = None
    arr = None
    try:
        buf_mv = shm.buf  # memoryview
        slice_mv = buf_mv[offset : offset + BYTES_PER_IMG]  # memoryview slice
        arr = np.frombuffer(slice_mv, dtype=DTYPE).reshape((H, W))
        return image_metric_from_array(arr)
    finally:
        # Ensure numpy view is gone before closing shared memory
        if arr is not None:
            del arr
        if slice_mv is not None:
            slice_mv.release()
        if buf_mv is not None:
            buf_mv.release()
        shm.close()


def pack_into_shared_memory(arrays: List[np.ndarray]) -> Tuple[shared_memory.SharedMemory, List[int]]:
    total = len(arrays) * BYTES_PER_IMG
    shm = shared_memory.SharedMemory(create=True, size=total)
    offsets: List[int] = []

    for i, a in enumerate(arrays):
        off = i * BYTES_PER_IMG
        offsets.append(off)
        shm.buf[off : off + BYTES_PER_IMG] = a.reshape(-1).tobytes()

    return shm, offsets


def run_pool_sharedmem(shm_name: str, offsets: List[int], workers: int) -> None:
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(worker_shm, (shm_name, off)) for off in offsets]
        for f in as_completed(futs):
            _ = f.result()


def load_arrays(paths: List[str]) -> List[np.ndarray]:
    arrays: List[np.ndarray] = []
    for p in paths:
        im = Image.open(p).convert("L")
        im = im.resize((W, H))
        arrays.append(np.asarray(im, dtype=DTYPE))
    return arrays


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--items", type=int, default=400)
    ap.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) // 4))
    args = ap.parse_args()

    files = sorted(DOCLAY_DIR.glob("*.png"))[: args.items]
    if not files:
        raise SystemExit("DocLayNet images not found. Run 00_fetch_and_materialize.py first.")
    paths = [str(p) for p in files]

    # Preload arrays so we're isolating IPC behavior (not disk IO).
    arrays = load_arrays(paths)

    r1 = measure(lambda: run_pool_arrays(arrays, args.workers), name=f"C4-procs(arrays_u8) w={args.workers}", items=len(arrays))

    shm, offsets = pack_into_shared_memory(arrays)
    try:
        r2 = measure(lambda: run_pool_sharedmem(shm.name, offsets, args.workers), name=f"C4-procs(sharedmem) w={args.workers}", items=len(offsets))
    finally:
        # Parent cleanup
        shm.close()
        shm.unlink()

    print_results([r1, r2])

    print("\nExpected:")
    print("- arrays_u8: slower + more overhead due to pickling/copying each array.")
    print("- sharedmem: faster/stable; only (name, offset) crosses process boundary.")
    print("\nProduction note:")
    print("- Shared memory is key when payloads are large (images, embeddings, feature matrices).")


if __name__ == "__main__":
    main()