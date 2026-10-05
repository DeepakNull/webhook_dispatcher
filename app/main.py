import logging
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from fastapi import FastAPI, HTTPException
from psycopg.types.json import Jsonb
from pydantic import BaseModel, HttpUrl

from .broker import publish_delivery
from .db import pool

logger = logging.getLogger("ingestion")


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool.open()
    pool.wait()
    yield
    pool.close()


app = FastAPI(title="Webhook Dispatcher - Ingestion API", lifespan=lifespan)


class PublishRequest(BaseModel):
    subscriber_url: HttpUrl
    payload: dict[str, Any]


@app.post("/events/publish", status_code=202)
def publish_event(req: PublishRequest):
    # 1) Persist first. If we crash after this, the row is still in Postgres.
    with pool.connection() as conn:  # commits on clean exit
        webhook = conn.execute(
            "INSERT INTO webhooks (subscriber_url, payload) VALUES (%s, %s) RETURNING id",
            (str(req.subscriber_url), Jsonb(req.payload)),
        ).fetchone()
        delivery = conn.execute(
            "INSERT INTO deliveries (webhook_id) VALUES (%s) RETURNING id",
            (webhook["id"],),
        ).fetchone()

    # 2) Then hand it to RabbitMQ. A failure here leaves a PENDING row that the
    #    sweeper (added with the worker) will re-publish.
    try:
        publish_delivery(str(delivery["id"]))
    except Exception:
        logger.exception("publish failed for delivery %s; sweeper will retry", delivery["id"])

    return {"tracking_id": delivery["id"], "status": "PENDING"}


@app.get("/events/{event_id}")
def get_event(event_id: UUID):
    with pool.connection() as conn:
        delivery = conn.execute(
            """
            SELECT d.id, d.status, d.attempt_count, d.next_attempt_at,
                   d.created_at, d.updated_at, w.subscriber_url
            FROM deliveries d JOIN webhooks w ON w.id = d.webhook_id
            WHERE d.id = %s
            """,
            (event_id,),
        ).fetchone()
        if delivery is None:
            raise HTTPException(status_code=404, detail="event not found")

        attempts = conn.execute(
            """
            SELECT attempt_number, status_code, error, duration_ms, attempted_at
            FROM delivery_attempts WHERE delivery_id = %s ORDER BY attempt_number
            """,
            (event_id,),
        ).fetchall()

    return {**delivery, "attempts": attempts}