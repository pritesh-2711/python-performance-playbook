from __future__ import annotations

import os
import time
from fastapi import FastAPI, Query

app = FastAPI()

MODE = os.getenv("PPB_HTTP_MODE", "io")  # io | mem
IO_DELAY_MS = float(os.getenv("PPB_HTTP_IO_DELAY_MS", "20"))  # used in io mode

# simple “binary search like” CPU step, but bounded; not a toy endpoint response-wise.
def _cpu_work(q: str) -> int:
    # deterministic small CPU cost proportional-ish to query length
    x = 0
    for _ in range(2000):
        for ch in q:
            x = (x * 131 + ord(ch)) & 0xFFFFFFFF
    return x

@app.get("/search")
def search(q: str = Query(default="python concurrency")):
    """
    Two modes:
      - io: simulates realistic IO wait (like upstream call) via sleep
      - mem: returns immediately (in-memory), still does small CPU work
    """
    _ = _cpu_work(q)

    if MODE == "io":
        time.sleep(IO_DELAY_MS / 1000.0)  # blocks worker thread (intentional)
    # In real service, you’d hit DB/HTTP here.

    return {"q": q, "mode": MODE, "ok": True}

if __name__ == "__main__":
    import uvicorn
    
    #uvicorn labs.async.server_fastapi:app --host 127.0.0.1 --port 8008
    uvicorn.run(app, host="127.0.0.1", port=8008)