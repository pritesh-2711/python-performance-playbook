from __future__ import annotations

import os
import psycopg
from dotenv import load_dotenv

DDL = """
DROP TABLE IF EXISTS ppb_mp_features;

CREATE TABLE ppb_mp_features (
  doc_id TEXT PRIMARY KEY,
  tokens_bigint BIGINT NOT NULL,
  hash32 BIGINT NOT NULL,
  bytes_len BIGINT NOT NULL,
  payload TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Make writes realistically expensive
CREATE INDEX ppb_mp_features_created_at_idx ON ppb_mp_features (created_at);
CREATE INDEX ppb_mp_features_hash32_idx      ON ppb_mp_features (hash32);
CREATE INDEX ppb_mp_features_tokens_idx     ON ppb_mp_features (tokens_bigint);
"""

def main() -> None:
    load_dotenv()
    dsn = os.getenv("PPB_PG_DSN")
    if not dsn:
        raise SystemExit("Set PPB_PG_DSN in env/.env")
    with psycopg.connect(dsn) as conn:
        conn.execute(DDL)
        conn.commit()
    print("C6 DB recreated: ppb_mp_features (payload + indexes)")

if __name__ == "__main__":
    main()
