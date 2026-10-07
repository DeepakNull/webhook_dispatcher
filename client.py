"""Flaky mock subscriber: the 'customer server' our dispatcher delivers to.

Run:  uvicorn client:app --port 9000
Returns 503 with probability FAIL_RATE (default 0.5), otherwise 200.
"""
import os
import random

from fastapi import FastAPI, Response

app = FastAPI(title="Flaky mock client")

FAIL_RATE = float(os.getenv("FAIL_RATE", "0.5"))
stats = {"received": 0, "ok": 0, "failed": 0}


@app.post("/receive")
def receive():
    stats["received"] += 1
    if random.random() < FAIL_RATE:
        stats["failed"] += 1
        return Response(status_code=503, content="Service Unavailable")
    stats["ok"] += 1
    return {"status": "ok"}


@app.get("/stats")
def get_stats():
    return stats 