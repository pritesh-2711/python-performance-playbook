"""
Lab 05 (Threads): Robust RAG ingest with graceful shutdown + retries + failure handling

Adds to Lab 04:
- retry embeddings with exponential backoff
- deterministic stop behavior on SIGINT
- explicit "poison pill" propagation
- dead-letter logging for extraction/embed/db issues

Run:
  python labs/threads/05_cancel_shutdown_and_error_handling.py --pdf /path/to/file.pdf --doc-id mydoc
Then press Ctrl+C once mid-way to confirm graceful stop.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import queue
import signal
import threading
import time
from dataclasses import dataclass

import httpx
import psycopg
from pypdf import PdfReader

from dotenv import load_dotenv
load_dotenv()

from vectordb_setup import setup_rag_db

@dataclass(frozen=True)
class Chunk:
    doc_id: str
    source_path: str
    chunk_id: int
    content: str
    content_sha1: str


@dataclass(frozen=True)
class Embedded:
    chunk: Chunk
    embedding: list[float]
    model: str


def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def extract_pdf_text(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    parts: list[str] = []
    for page in reader.pages:
        txt = page.extract_text() or ""
        if txt.strip():
            parts.append(txt)
    return "\n".join(parts)


def chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >= 0 and < chunk_size")
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        end = min(i + chunk_size, n)
        c = text[i:end].strip()
        if c:
            out.append(c)
        if end == n:
            break
        i = end - overlap
    return out


def log_failure(dsn: str, *, doc_id: str, source_path: str, chunk_id: int | None, stage: str, error: str) -> None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ppb_rag_failures (doc_id, source_path, chunk_id, stage, error)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (doc_id, source_path, chunk_id, stage, error[:10_000]),
            )
        conn.commit()


def ollama_embed_with_retry(
    client: httpx.Client,
    model: str,
    text: str,
    stop: threading.Event,
    retries: int,
    base_backoff_s: float,
) -> list[float]:
    last: Exception | None = None
    for attempt in range(retries + 1):
        if stop.is_set():
            raise RuntimeError("Stopped")
        try:
            r = client.post(
                "http://127.0.0.1:11434/api/embeddings",
                json={"model": model, "prompt": text},
                timeout=60.0,
            )
            r.raise_for_status()
            data = r.json()
            emb = data.get("embedding")
            if not isinstance(emb, list) or not emb:
                raise RuntimeError(f"Bad embedding response: {data}")
            return [float(x) for x in emb]
        except Exception as e:
            last = e
            if attempt < retries:
                time.sleep(base_backoff_s * (2**attempt))
    raise RuntimeError(f"Ollama embed failed after retries: {last}") from last


def embedding_to_pgvector_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.7f}" for x in vec) + "]"


def insert_batch(dsn: str, batch: list[Embedded]) -> None:
    if not batch:
        return
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ppb_rag_chunks
                  (doc_id, source_path, chunk_id, content, embedding, embedding_dim, embedding_model, content_sha1)
                VALUES
                  (%s, %s, %s, %s, %s::vector, %s, %s, %s)
                ON CONFLICT (doc_id, chunk_id)
                DO UPDATE SET
                  content = EXCLUDED.content,
                  embedding = EXCLUDED.embedding,
                  embedding_dim = EXCLUDED.embedding_dim,
                  embedding_model = EXCLUDED.embedding_model,
                  content_sha1 = EXCLUDED.content_sha1;
                """,
                [
                    (
                        e.chunk.doc_id,
                        e.chunk.source_path,
                        e.chunk.chunk_id,
                        e.chunk.content,
                        embedding_to_pgvector_literal(e.embedding),
                        len(e.embedding),
                        e.model,
                        e.chunk.content_sha1,
                    )
                    for e in batch
                ],
            )
        conn.commit()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--pdf", required=True)
    p.add_argument("--doc-id", required=True)
    p.add_argument("--chunk-size", type=int, default=1200)
    p.add_argument("--overlap", type=int, default=200)
    p.add_argument("--embed-model", default="nomic-embed-text")
    p.add_argument("--embed-workers", type=int, default=8)
    p.add_argument("--queue-max", type=int, default=200)
    p.add_argument("--db-batch", type=int, default=32)
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--backoff", type=float, default=0.3)
    args = p.parse_args()

    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path)
    dsn = os.getenv("PPB_PG_DSN")
    if not dsn:
        raise ValueError("Environment variable PPB_PG_DSN is not set.")
    setup_rag_db(dotenv_path)

    stop = threading.Event()

    def on_sigint(sig, frame):
        print("\nSIGINT received. Stopping...")
        stop.set()

    signal.signal(signal.SIGINT, on_sigint)

    source_path = os.path.abspath(args.pdf)

    # Extract (fail fast)
    try:
        text = extract_pdf_text(source_path)
    except Exception as e:
        log_failure(dsn, doc_id=args.doc_id, source_path=source_path, chunk_id=None, stage="extract", error=str(e))
        raise SystemExit(f"PDF extraction failed: {e}") from e

    if not text.strip():
        err = "No extractable text found (image-only PDF needs OCR)."
        log_failure(dsn, doc_id=args.doc_id, source_path=source_path, chunk_id=None, stage="extract", error=err)
        raise SystemExit(err)

    # Chunk
    chunks_raw = chunk_text(text, args.chunk_size, args.overlap)
    chunks = [Chunk(args.doc_id, source_path, i, c, sha1_text(c)) for i, c in enumerate(chunks_raw)]
    print(f"Chunks: {len(chunks)}")

    q_chunks: queue.Queue[Chunk | None] = queue.Queue(maxsize=args.queue-max if hasattr(args, "queue-max") else args.queue_max)  # safety
    q_emb: queue.Queue[Embedded | None] = queue.Queue(maxsize=args.queue_max)

    # Producer
    def producer() -> None:
        for c in chunks:
            if stop.is_set():
                break
            q_chunks.put(c)
        for _ in range(args.embed_workers):
            q_chunks.put(None)

    # Embed worker
    def embed_worker(worker_id: int) -> None:
        with httpx.Client() as client:
            while not stop.is_set():
                item = q_chunks.get()
                if item is None:
                    q_emb.put(None)
                    return
                try:
                    emb = ollama_embed_with_retry(
                        client,
                        args.embed_model,
                        item.content,
                        stop=stop,
                        retries=args.retries,
                        base_backoff_s=args.backoff,
                    )
                    q_emb.put(Embedded(item, emb, args.embed_model))
                except Exception as e:
                    log_failure(dsn, doc_id=item.doc_id, source_path=item.source_path, chunk_id=item.chunk_id,
                                stage="embed", error=str(e))
                    continue

    # DB writer: if DB fails, stop (high severity)
    def db_writer() -> None:
        batch: list[Embedded] = []
        sentinels = 0
        while not stop.is_set():
            item = q_emb.get()
            if item is None:
                sentinels += 1
                if sentinels >= args.embed_workers:
                    break
                continue
            batch.append(item)
            if len(batch) >= args.db_batch:
                try:
                    insert_batch(dsn, batch)
                except Exception as e:
                    log_failure(dsn, doc_id=args.doc_id, source_path=source_path, chunk_id=None, stage="db", error=str(e))
                    stop.set()
                    break
                batch.clear()
        if batch and not stop.is_set():
            try:
                insert_batch(dsn, batch)
            except Exception as e:
                log_failure(dsn, doc_id=args.doc_id, source_path=source_path, chunk_id=None, stage="db", error=str(e))
                stop.set()

    t0 = time.perf_counter()

    tp = threading.Thread(target=producer, name="producer")
    te = [threading.Thread(target=embed_worker, args=(i,), name=f"embed-{i}") for i in range(args.embed_workers)]
    td = threading.Thread(target=db_writer, name="db-writer")

    tp.start()
    for t in te:
        t.start()
    td.start()

    tp.join()
    for t in te:
        t.join()
    td.join()

    dt = time.perf_counter() - t0
    print(f"Done. stopped={stop.is_set()} time={dt:.2f}s")
    print("Failures (if any) are in ppb_rag_failures.")


if __name__ == "__main__":
    main()
