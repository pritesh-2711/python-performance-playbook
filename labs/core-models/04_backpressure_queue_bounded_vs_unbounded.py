"""
Backpressure demo: bounded vs unbounded queue in a producer/consumer pipeline.

What it shows
- If producer is faster than consumer:
  - Unbounded queue -> queue grows without limit (memory + latency risk)
  - Bounded queue   -> producer blocks when queue is full (backpressure), system stabilizes

Run:
  python labs/core-models/04_backpressure_queue_bounded_vs_unbounded.py

Tip:
- This is a *streaming* mental model demo (infinite-ish production), not batch.
"""

from __future__ import annotations

import argparse
import queue
import threading
import time
from dataclasses import dataclass


@dataclass
class Stats:
    produced: int = 0
    consumed: int = 0
    max_qsize: int = 0


def run_pipeline(
    *,
    duration_s: float,
    qmax: int | None,
    produce_every_s: float,
    consume_time_s: float,
) -> Stats:
    """
    qmax:
      - None -> unbounded queue (queue.SimpleQueue)
      - int  -> bounded queue.Queue(maxsize=qmax)
    """

    stop = threading.Event()
    stats = Stats()

    if qmax is None:
        q: queue.SimpleQueue[int] | queue.Queue[int] = queue.SimpleQueue()
    else:
        q = queue.Queue(maxsize=qmax)

    def producer() -> None:
        i = 0
        while not stop.is_set():
            # Produce work items quickly
            if isinstance(q, queue.Queue):
                # bounded: put() blocks when queue is full -> backpressure
                q.put(i)
            else:
                # unbounded: never blocks
                q.put(i)

            stats.produced += 1
            i += 1

            # For bounded queue we can observe qsize(); for SimpleQueue, qsize() may exist
            # but is not reliable; we compute max_qsize only when safe/available.
            try:
                qs = q.qsize()  # type: ignore[attr-defined]
                if qs > stats.max_qsize:
                    stats.max_qsize = qs
            except Exception:
                pass

            time.sleep(produce_every_s)

    def consumer() -> None:
        while not stop.is_set():
            try:
                item = q.get(timeout=0.1)  # blocks waiting for work
            except Exception:
                continue
            _ = item  # simulate "processing"
            time.sleep(consume_time_s)
            stats.consumed += 1

    tp = threading.Thread(target=producer, name="producer", daemon=True)
    tc = threading.Thread(target=consumer, name="consumer", daemon=True)

    tp.start()
    tc.start()

    time.sleep(duration_s)
    stop.set()

    # allow threads to exit
    tp.join(timeout=1.0)
    tc.join(timeout=1.0)

    # final queue size (bounded Queue only reliably)
    try:
        qs = q.qsize()  # type: ignore[attr-defined]
    except Exception:
        qs = -1

    # store last observed as max when possible
    if qs > stats.max_qsize:
        stats.max_qsize = qs

    return stats


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--duration", type=float, default=5.0, help="Run duration in seconds.")
    p.add_argument(
        "--produce-every",
        type=float,
        default=0.001,
        help="Seconds between produces (smaller => faster producer).",
    )
    p.add_argument(
        "--consume-time",
        type=float,
        default=0.005,
        help="Seconds to process one item (larger => slower consumer).",
    )
    p.add_argument(
        "--bounded-maxsize",
        type=int,
        default=200,
        help="Max size for bounded queue demo.",
    )
    args = p.parse_args()

    print("\n=== Unbounded queue (NO backpressure) ===")
    s1 = run_pipeline(
        duration_s=args.duration,
        qmax=None,
        produce_every_s=args.produce_every,
        consume_time_s=args.consume_time,
    )
    backlog1 = s1.produced - s1.consumed
    print(f"Produced : {s1.produced}")
    print(f"Consumed : {s1.consumed}")
    print(f"Backlog  : {backlog1} (keeps growing if producer > consumer)")
    print(f"Max qsize: {s1.max_qsize} (qsize for unbounded may be unreliable)")

    print("\n=== Bounded queue (WITH backpressure) ===")
    s2 = run_pipeline(
        duration_s=args.duration,
        qmax=args.bounded_maxsize,
        produce_every_s=args.produce_every,
        consume_time_s=args.consume_time,
    )
    backlog2 = s2.produced - s2.consumed
    print(f"Produced : {s2.produced}")
    print(f"Consumed : {s2.consumed}")
    print(f"Backlog  : {backlog2} (should be bounded, approx <= maxsize)")
    print(f"Max qsize: {s2.max_qsize} (should stay near maxsize when saturated)")

    print("\nInterpretation:")
    print("- Unbounded queue: backlog grows => memory/latency risk.")
    print("- Bounded queue  : producer blocks => system stays stable under load.\n")


if __name__ == "__main__":
    main()
