from __future__ import annotations

import os
import psycopg

from dotenv import load_dotenv
load_dotenv()

DDL = """
-- pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Stores chunk text + embedding
-- NOTE: embedding uses 'vector' without fixed dimension so you can swap models.
-- For ANN indexes (ivfflat/hnsw), you usually want a fixed dimension; we create indexes later.
CREATE TABLE IF NOT EXISTS ppb_rag_chunks (
  id BIGSERIAL PRIMARY KEY,
  doc_id TEXT NOT NULL,
  source_path TEXT,
  chunk_id INT NOT NULL,
  content TEXT NOT NULL,
  embedding vector,
  embedding_dim INT,
  embedding_model TEXT,
  content_sha1 TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (doc_id, chunk_id)
);

-- Minimal dead-letter table for failures
CREATE TABLE IF NOT EXISTS ppb_rag_failures (
  id BIGSERIAL PRIMARY KEY,
  doc_id TEXT,
  source_path TEXT,
  chunk_id INT,
  stage TEXT NOT NULL,         -- e.g., extract/chunk/embed/db
  error TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Helpful indexes (non-ANN)
CREATE INDEX IF NOT EXISTS ppb_rag_chunks_doc_id_idx ON ppb_rag_chunks (doc_id);
CREATE INDEX IF NOT EXISTS ppb_rag_chunks_created_at_idx ON ppb_rag_chunks (created_at);
"""


def setup_rag_db(dotenv_path: str | None = None) -> str:
    load_dotenv(dotenv_path)
    dsn = os.getenv("PPB_PG_DSN")
    if not dsn:
        raise ValueError("Environment variable PPB_PG_DSN is not set.")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

    return dsn


def main() -> None:
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    dsn = setup_rag_db(dotenv_path)
    print(f"RAG DB setup complete. DSN: {dsn}")
    print("\nOptional (after ingest, once model dim is fixed):")
    print("- Create ANN index (ivfflat or hnsw) on embedding.")
    print("- Example (ivfflat, cosine):")
    print("  CREATE INDEX CONCURRENTLY ppb_rag_chunks_embedding_ivfflat")
    print("  ON ppb_rag_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);")


if __name__ == "__main__":
    main()
