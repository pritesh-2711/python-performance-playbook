"""
Fetch + materialize REAL datasets for Section C (Multiprocessing).

Datasets:
- NLP:     CShorten/ML-ArXiv-Papers
- Images:  docling-project/DocLayNet

Why materialize?
- Multiprocessing labs need "paths vs blobs" (pickle/IPC tax) and realistic file-based pipelines.

Output layout:
labs/multiprocessing/_data/
  arxiv_text/
    docs/
      000001.txt ...
    meta.jsonl
  doclaynet_images/
    images/
      000001.png ...
    meta.jsonl

Usage:
  python labs/multiprocessing/00_fetch_and_materialize.py
  python labs/multiprocessing/00_fetch_and_materialize.py --n-text 2000 --n-images 1000

Notes:
- Uses HuggingFace `datasets` with streaming to avoid huge local downloads.
- Deterministic selection via seed + streamed shuffle buffer.
"""

from __future__ import annotations

import argparse
import json
import os
import gc

import re
from pathlib import Path
from typing import Any, Iterable, Optional

from datasets import load_dataset
from PIL import Image
os.environ.setdefault("ARROW_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

BASE = Path(__file__).parent / "_data"
ARXIV_OUT = BASE / "arxiv_text"
DOCLAY_OUT = BASE / "doclaynet_images"
DATASET_ID = "docling-project/DocLayNet-v1.2"


def _safe_filename(i: int) -> str:
    return f"{i:06d}"


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _guess_text_fields(example: dict[str, Any]) -> tuple[Optional[str], Optional[str], list[str]]:
    """
    Best-effort guess of fields for:
    - title
    - abstract/summary
    - full text candidate fields (if present)
    """
    keys = list(example.keys())
    lower = {k.lower(): k for k in keys}

    def pick(cands: list[str]) -> Optional[str]:
        for c in cands:
            if c in lower:
                return lower[c]
        return None

    title = pick(["title", "paper_title", "doc_title"])
    abstract = pick(["abstract", "summary", "paper_abstract", "description"])
    full_candidates = []
    for c in ["text", "content", "body", "paper_text", "article", "full_text"]:
        k = lower.get(c)
        if k:
            full_candidates.append(k)

    return title, abstract, full_candidates


def _normalize_text(s: str, max_chars: int) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = s.strip()
    if max_chars and len(s) > max_chars:
        s = s[:max_chars].rstrip() + "\n\n[TRUNCATED]\n"
    return s


def materialize_arxiv(
    *,
    out_dir: Path,
    n: int,
    seed: int,
    shuffle_buffer: int,
    split: str,
    max_chars: int,
) -> None:
    docs_dir = out_dir / "docs"
    meta_path = out_dir / "meta.jsonl"
    _ensure_dir(docs_dir)

    # If already have enough docs, skip
    existing = len(list(docs_dir.glob("*.txt")))
    if existing >= n:
        print(f"[arxiv] Already materialized: {existing} >= {n}. Skipping.")
        return

    print(f"[arxiv] Loading dataset (streaming): CShorten/ML-ArXiv-Papers split={split}")
    ds = load_dataset("CShorten/ML-ArXiv-Papers", split=split, streaming=True)

    # Streaming shuffle is approximate; still deterministic given seed and buffer.
    ds = ds.shuffle(seed=seed, buffer_size=shuffle_buffer)

    # Probe one example to decide which fields to use
    first = next(iter(ds))
    title_k, abs_k, full_ks = _guess_text_fields(first)

    print("[arxiv] Detected fields:", {"title": title_k, "abstract": abs_k, "full_text_candidates": full_ks})

    def build_doc(ex: dict[str, Any]) -> tuple[str, dict]:
        title = str(ex.get(title_k, "") if title_k else "")
        abstract = str(ex.get(abs_k, "") if abs_k else "")

        # Prefer a "full text" field if present and non-trivial; else use abstract.
        full_text = ""
        for k in full_ks:
            v = ex.get(k)
            if isinstance(v, str) and len(v.strip()) > 200:
                full_text = v
                break

        if not full_text:
            full_text = abstract

        # Compose a realistic "paper-like" text artifact.
        combined = []
        if title.strip():
            combined.append(f"# {title.strip()}\n")
        if abstract.strip() and abstract.strip() != full_text.strip():
            combined.append("## Abstract\n" + abstract.strip() + "\n")
        if full_text.strip():
            combined.append("## Content\n" + full_text.strip() + "\n")

        body = _normalize_text("\n".join(combined), max_chars=max_chars)

        meta = {
            "source": "CShorten/ML-ArXiv-Papers",
            "split": split,
            "title": title.strip(),
            "has_abstract": bool(abstract.strip()),
            "has_fulltext": bool(full_text.strip()),
            # keep a couple more fields if they exist (safe)
            "arxiv_id": ex.get("id") or ex.get("arxiv_id") or ex.get("paper_id"),
            "categories": ex.get("categories") or ex.get("category") or ex.get("primary_category"),
        }
        return body, meta

    # Recreate iterator after probing first (streaming iterators advance)
    ds = load_dataset("CShorten/ML-ArXiv-Papers", split=split, streaming=True).shuffle(
        seed=seed, buffer_size=shuffle_buffer
    )

    written = existing
    metas: list[dict] = []
    for ex in ds:
        if written >= n:
            break
        fname = _safe_filename(written + 1)
        out_txt = docs_dir / f"{fname}.txt"
        if out_txt.exists():
            written += 1
            continue

        body, meta = build_doc(ex)
        out_txt.write_text(body, encoding="utf-8")

        meta_row = {"file": str(out_txt.relative_to(out_dir)), **meta}
        metas.append(meta_row)

        written += 1
        if len(metas) >= 50:
            _write_jsonl(meta_path, metas)
            metas.clear()

        if written % 200 == 0:
            print(f"[arxiv] materialized {written}/{n}")

    if metas:
        _write_jsonl(meta_path, metas)

    print(f"[arxiv] Done. Total materialized: {written} docs at {out_dir}")


def _guess_image_field(example: dict[str, Any]) -> Optional[str]:
    # Common image field names on HF datasets
    for k in ["image", "img", "page_image", "page", "pixel_values"]:
        if k in example:
            return k
    # fallback: find first value that looks like PIL.Image.Image
    for k, v in example.items():
        if isinstance(v, Image.Image):
            return k
    return None


def _to_pil(v: Any) -> Image.Image:
    if isinstance(v, Image.Image):
        return v
    # HF "Image" feature might provide dict with bytes/path in some cases.
    if isinstance(v, dict):
        # Try common keys
        for kk in ["image", "bytes", "data"]:
            if kk in v and isinstance(v[kk], (bytes, bytearray)):
                from io import BytesIO

                return Image.open(BytesIO(v[kk])).convert("RGB")
        if "path" in v and isinstance(v["path"], str):
            return Image.open(v["path"]).convert("RGB")
    raise TypeError(f"Cannot convert to PIL Image: type={type(v)} keys={list(v.keys()) if isinstance(v, dict) else None}")


def materialize_doclaynet(
    *,
    out_dir: Path,
    n: int,
    seed: int,
    shuffle_buffer: int,
    split: str,
    max_side: int,
) -> None:
    img_dir = out_dir / "images"
    meta_path = out_dir / "meta.jsonl"
    _ensure_dir(img_dir)

    existing = len(list(img_dir.glob("*.png")))
    if existing >= n:
        print(f"[doclaynet] Already materialized: {existing} >= {n}. Skipping.")
        return

    print(f"[doclaynet] Loading dataset (streaming): {DATASET_ID} split={split}")

    ds = load_dataset(DATASET_ID, split=split, streaming=True)

    ds = ds.shuffle(seed=seed, buffer_size=shuffle_buffer)

    first = next(iter(ds))
    img_k = _guess_image_field(first)
    if not img_k:
        raise RuntimeError(f"[doclaynet] Could not find an image field in example keys={list(first.keys())}")

    print("[doclaynet] Detected image field:", img_k)
    print("[doclaynet] Example keys:", list(first.keys()))

    # Recreate iterator after probing
    ds = load_dataset(DATASET_ID, split=split, streaming=True).shuffle(
        seed=seed, buffer_size=shuffle_buffer
    )

    written = existing
    metas: list[dict] = []
    for ex in ds:
        if written >= n:
            break

        fname = _safe_filename(written + 1)
        out_png = img_dir / f"{fname}.png"
        if out_png.exists():
            written += 1
            continue

        try:
            pil = _to_pil(ex[img_k]).convert("RGB")
        except Exception as e:
            # Skip problematic rows (rare)
            print(f"[doclaynet] Warning: skipping example {written + 1} due to image load error: {e}")

        # Resize to cap CPU/mem while keeping it realistic (document pages can be huge)
        if max_side and max(pil.size) > max_side:
            pil.thumbnail((max_side, max_side))

        pil.save(out_png)

        meta = {
            "source": DATASET_ID,
            "split": split,
            "file": str(out_png.relative_to(out_dir)),
            "w": pil.size[0],
            "h": pil.size[1],
            # Keep some common fields if present; dataset schemas can vary
            "doc_id": ex.get("doc_id") or ex.get("document_id") or ex.get("id"),
            "page": ex.get("page") or ex.get("page_number"),
            "category": ex.get("category") or ex.get("doc_type"),
        }
        metas.append(meta)

        written += 1
        if len(metas) >= 50:
            _write_jsonl(meta_path, metas)
            metas.clear()

        if written % 200 == 0:
            print(f"[doclaynet] materialized {written}/{n}")

    if metas:
        _write_jsonl(meta_path, metas)

    print(f"[doclaynet] Done. Total materialized: {written} images at {out_dir}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-text", type=int, default=1200, help="How many ArXiv docs to materialize")
    ap.add_argument("--n-images", type=int, default=800, help="How many DocLayNet page images to materialize")
    ap.add_argument("--seed", type=int, default=42, help="Deterministic selection seed")
    ap.add_argument("--shuffle-buffer", type=int, default=10_000, help="Streaming shuffle buffer (bigger=better shuffle, more RAM)")
    ap.add_argument("--arxiv-split", default="train", help="Split for CShorten/ML-ArXiv-Papers (train/validation/test if available)")
    ap.add_argument("--doclaynet-split", default="train", help="Split for DocLayNet (train/validation/test if available)")
    ap.add_argument("--max-text-chars", type=int, default=120_000, help="Cap per-doc chars to keep CPU bounded")
    ap.add_argument("--max-image-side", type=int, default=1400, help="Max width/height for saved page images")
    ap.add_argument("--hard-exit", action="store_true", help="Bypass interpreter finalizers to avoid native shutdown crash")

    args = ap.parse_args()

    _ensure_dir(BASE)
    _ensure_dir(ARXIV_OUT)
    _ensure_dir(DOCLAY_OUT)

    print("Output base:", BASE)

    materialize_arxiv(
        out_dir=ARXIV_OUT,
        n=args.n_text,
        seed=args.seed,
        shuffle_buffer=args.shuffle_buffer,
        split=args.arxiv_split,
        max_chars=args.max_text_chars,
    )

    materialize_doclaynet(
        out_dir=DOCLAY_OUT,
        n=args.n_images,
        seed=args.seed,
        shuffle_buffer=args.shuffle_buffer,
        split=args.doclaynet_split,
        max_side=args.max_image_side,
    )

    print("\nMaterialization complete.")
    print("ArXiv docs      :", ARXIV_OUT / "docs")
    print("ArXiv meta.jsonl:", ARXIV_OUT / "meta.jsonl")
    print("DocLayNet imgs  :", DOCLAY_OUT / "images")
    print("DocLayNet meta  :", DOCLAY_OUT / "meta.jsonl")

    # Best-effort cleanup before exit
    gc.collect()

    # Some environments hit a native finalizer crash (pyarrow/datasets). Hard-exit avoids it.
    if args.hard_exit:
        os._exit(0)


if __name__ == "__main__":
    main()
