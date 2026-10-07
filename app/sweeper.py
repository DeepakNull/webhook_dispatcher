"""Sweeper.  Run:  python -m app.sweeper

Closes the dual-write gap: the API commits to Postgres first, then publishes to
RabbitMQ. If it dies in between, a delivery sits in PENDING with no message.
This process periodically finds PENDING rows older than PENDING_STALE_SECONDS and
re-publishes them.
"""
import logging
import time

from . import config
from .broker import publish_delivery
from .db import pool

log = logging.getLogger("sweeper")
BATCH = 100


def claim_stale() -> list[str]:
    """Atomically claim stale PENDING rows.

    Bumping updated_at "claims" a row so it isn't picked again until it goes stale
    again, and FOR UPDATE SKIP LOCKED lets several sweepers run without blocking
    or double-claiming.
    """
    with pool.connection() as conn:
        rows = conn.execute(
            """
            UPDATE deliveries SET updated_at = now()
            WHERE id IN (
                SELECT id FROM deliveries
                WHERE status = 'PENDING'
                  AND updated_at < now() - make_interval(secs => %s::double precision)
                ORDER BY created_at
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
            """,
            (config.PENDING_STALE_SECONDS, BATCH),
        ).fetchall()
    return [str(r["id"]) for r in rows]


def sweep_once() -> int:
    ids = claim_stale()
    for delivery_id in ids:
        try:
            publish_delivery(delivery_id)
            log.info("re-published stuck delivery %s", delivery_id)
        except Exception:
            # Row was already claimed; it becomes eligible again after the stale window
            log.exception("re-publish failed for %s", delivery_id)
    return len(ids)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    pool.open()
    pool.wait()
    log.info("sweeper started (interval=%ss, stale=%ss)",
             config.SWEEP_INTERVAL, config.PENDING_STALE_SECONDS)
    try:
        while True:
            sweep_once()
            time.sleep(config.SWEEP_INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        pool.close()


if __name__ == "__main__":
    main()