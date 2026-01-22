from __future__ import annotations

import os
import psycopg

from dotenv import load_dotenv
load_dotenv()

DDL = """
CREATE TABLE IF NOT EXISTS ppb_users (
  user_id BIGINT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ppb_events (
  event_id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES ppb_users(user_id),
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- For lock contention lab
CREATE TABLE IF NOT EXISTS ppb_counters (
  id INT PRIMARY KEY,
  value BIGINT NOT NULL
);
"""


def seed_data(dsn: str, users: int = 50_000, events_per_user: int = 3) -> None:
    """
    Creates enough rows so SELECT/JOIN is real.
    Uses COPY via execute many batches (fast enough for local).
    """
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # users
            cur.execute("SELECT COUNT(*) FROM ppb_users;")
            existing_users = cur.fetchone()[0]
            if existing_users < users:
                # cur.execute("TRUNCATE ppb_events;")
                # cur.execute("TRUNCATE ppb_users;")
                cur.execute("TRUNCATE ppb_events, ppb_users;")


                # Insert users
                batch = []
                for uid in range(1, users + 1):
                    batch.append((uid, f"user{uid}@example.com"))
                    if len(batch) >= 5000:
                        cur.executemany(
                            "INSERT INTO ppb_users (user_id, email) VALUES (%s, %s);",
                            batch,
                        )
                        batch.clear()
                if batch:
                    cur.executemany(
                        "INSERT INTO ppb_users (user_id, email) VALUES (%s, %s);",
                        batch,
                    )

                # Insert events
                # Keep payload small but non-trivial
                batch = []
                for uid in range(1, users + 1):
                    for j in range(events_per_user):
                        batch.append((uid, "click", f"payload-{uid}-{j}"))
                        if len(batch) >= 5000:
                            cur.executemany(
                                "INSERT INTO ppb_events (user_id, event_type, payload) VALUES (%s, %s, %s);",
                                batch,
                            )
                            batch.clear()
                if batch:
                    cur.executemany(
                        "INSERT INTO ppb_events (user_id, event_type, payload) VALUES (%s, %s, %s);",
                        batch,
                    )

            # counters
            cur.execute("TRUNCATE ppb_counters;")
            # hot row
            cur.execute("INSERT INTO ppb_counters (id, value) VALUES (1, 0);")
            # sharded rows
            for i in range(2, 2 + 256):
                cur.execute("INSERT INTO ppb_counters (id, value) VALUES (%s, 0);", (i,))

        conn.commit()


def setup_db(dotenv_path: str | None = None) -> str:
    """
    Ensures tables exist + data seeded.
    Returns DSN used.
    """
    load_dotenv(dotenv_path)
    dsn = os.getenv("PPB_PG_DSN")
    if not dsn:
        raise SystemExit('Set PPB_PG_DSN, e.g. export PPB_PG_DSN="postgresql://localhost:5432/postgres"')

    with psycopg.connect(dsn) as conn:
        conn.execute(DDL)
        conn.commit()

    # seed controlled dataset
    seed_data(dsn)
    return dsn


if __name__ == "__main__":
    # Running directly will setup DB using labs/threads/.env if present
    here_env = os.path.join(os.path.dirname(__file__), ".env")
    dsn = setup_db(here_env)
    print(f"DB setup complete. DSN: {dsn}")
