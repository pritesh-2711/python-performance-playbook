from __future__ import annotations

import argparse
import os
import sys

import asyncpg
from dotenv import load_dotenv
load_dotenv()

DDL = """
CREATE TABLE IF NOT EXISTS ppb_async_users (
  user_id BIGINT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ppb_async_events (
  event_id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES ppb_async_users(user_id),
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ppb_async_events_user_id_idx ON ppb_async_events(user_id);
CREATE INDEX IF NOT EXISTS ppb_async_events_type_idx ON ppb_async_events(event_type);
"""

SEED_USERS = """
INSERT INTO ppb_async_users (user_id, email)
SELECT gs, 'user' || gs::text || '@example.com'
FROM generate_series(1, $1::bigint) gs
ON CONFLICT DO NOTHING;
"""

# events_per_user events per user (small payload but non-trivial)
SEED_EVENTS = """
INSERT INTO ppb_async_events (user_id, event_type, payload)
SELECT
  u.user_id,
  CASE WHEN (u.user_id % 5)=0 THEN 'purchase' ELSE 'click' END,
  'payload-' || u.user_id::text || '-' || ev::text
FROM (SELECT user_id FROM ppb_async_users ORDER BY user_id LIMIT $1::bigint) u
CROSS JOIN generate_series(1, $2::int) ev;
"""


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=50_000)
    ap.add_argument("--events-per-user", type=int, default=3)
    ap.add_argument("--limit-users-for-events", type=int, default=50_000)
    args = ap.parse_args()

    here_env = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(here_env)
    dsn = os.getenv("PPB_PG_DSN_ASYNC")
    if not dsn:
        raise SystemExit("Missing PPB_PG_DSN_ASYNC in labs/async/.env")

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(DDL)

        # Seed users
        await conn.execute(SEED_USERS, args.users)

        # Seed events (optionally limit)
        # If you re-run, events will append; for stable benchmarks you can TRUNCATE manually.
        await conn.execute(SEED_EVENTS, args.limit_users_for_events, args.events_per_user)

        # Quick counts
        u = await conn.fetchval("SELECT COUNT(*) FROM ppb_async_users;")
        e = await conn.fetchval("SELECT COUNT(*) FROM ppb_async_events;")
        print(f"D2 DB ready: ppb_async_users={u} ppb_async_events={e}")
    finally:
        await conn.close()


if __name__ == "__main__":
    if sys.version_info < (3, 11):
        raise SystemExit("Python 3.11+ required (you are on 3.12, so OK).")
    import asyncio

    asyncio.run(main())
