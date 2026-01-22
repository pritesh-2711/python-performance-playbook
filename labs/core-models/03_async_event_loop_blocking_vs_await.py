"""
Async demo: blocking the event loop destroys concurrency.
- asyncio.sleep(...) yields control => concurrency works
- time.sleep(...) blocks the loop => tasks run effectively serially

Run:
  python labs/core_models/03_async_event_loop_blocking_vs_await.py
"""

from __future__ import annotations

import asyncio
import time


async def good_task(i: int, wait_s: float = 0.2) -> int:
    await asyncio.sleep(wait_s) # this yields control to event loop
    return i


async def bad_task(i: int, wait_s: float = 0.2) -> int:
    time.sleep(wait_s)  # blocks the event loop
    return i


async def run_good(n: int) -> float:
    t0 = time.perf_counter()
    await asyncio.gather(*(good_task(i) for i in range(n)))
    return time.perf_counter() - t0


async def run_bad(n: int) -> float:
    t0 = time.perf_counter()
    await asyncio.gather(*(bad_task(i) for i in range(n)))
    return time.perf_counter() - t0


async def main() -> None:
    n = 50
    good = await run_good(n)
    bad = await run_bad(n)

    print(f"Tasks: {n}, each waits ~0.2s")
    print(f"Good async (await sleep): {good:.3f}s")
    print(f"Bad async (time.sleep) : {bad:.3f}s")
    print("\nExpected: good async much faster; bad async ~ serial.")


if __name__ == "__main__":
    asyncio.run(main())
