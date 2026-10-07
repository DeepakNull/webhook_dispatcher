"""Delivery worker.  Run:  python -m app.worker

For each message {"delivery_id": ...} from webhook_main:
  1. load the delivery from Postgres (source of truth)
  2. POST the payload to the subscriber (strict timeout)
  3. record the attempt + new state in Postgres
  4. route: DELIVERED -> done | RETRY -> retry queue | FAILED -> DLQ
  5. ack the message ONLY after all of the above succeeded
"""
import json
import logging
import time
from uuid import UUID

import httpx
import pika

from . import config, topology
from .db import pool

log = logging.getLogger("worker")


def load_delivery(delivery_id: str):
    with pool.connection() as conn:
        return conn.execute(
            """
            SELECT d.id, d.status, d.attempt_count, w.subscriber_url, w.payload
            FROM deliveries d JOIN webhooks w ON w.id = d.webhook_id
            WHERE d.id = %s
            """,
            (delivery_id,),
        ).fetchone()


def attempt_http(http: httpx.Client, row, attempt_number: int):
    """Returns (ok, status_code, error, duration_ms). Never raises."""
    start = time.perf_counter()
    code, err, ok = None, None, False
    try:
        r = http.post(
            row["subscriber_url"],
            json=row["payload"],
            headers={
                "X-Delivery-Id": str(row["id"]),  # receivers can dedupe on this
                "X-Attempt": str(attempt_number),
            },
        )
        code = r.status_code
        ok = 200 <= code < 300
        if not ok:
            err = f"HTTP {code}"
    except httpx.TimeoutException:
        err = "timeout"
    except Exception as e:  # connection refused, DNS failure, bad URL, ...
        err = f"{type(e).__name__}: {e}"
    return ok, code, err, int((time.perf_counter() - start) * 1000)


def record_outcome(delivery_id, attempt_number, status, delay, code, err, duration_ms):
    """One transaction: append to the attempt log AND update the state machine."""
    with pool.connection() as conn:
        conn.execute(
            """
            INSERT INTO delivery_attempts
                (delivery_id, attempt_number, status_code, error, duration_ms)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (delivery_id, attempt_number, code, err, duration_ms),
        )
        conn.execute(
            """
            UPDATE deliveries
            SET status = %s::delivery_status,
                attempt_count = %s,
                next_attempt_at = now() + make_interval(secs => %s::double precision),
                updated_at = now()
            WHERE id = %s
            """,
            (status, attempt_number, delay, delivery_id),  # delay=None -> NULL
        )


def publish(ch, exchange, routing_key, delivery_id, headers=None):
    ch.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps({"delivery_id": delivery_id}),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
            headers=headers,
        ),
        mandatory=True,
    )


def process(ch, http, delivery_id: str) -> None:
    row = load_delivery(delivery_id)
    if row is None:
        log.warning("delivery %s not found; dropping message", delivery_id)
        return
    if row["status"] in ("DELIVERED", "FAILED"):
        log.info("delivery %s already %s; skipping duplicate", delivery_id, row["status"])
        return

    attempt_number = row["attempt_count"] + 1
    ok, code, err, duration_ms = attempt_http(http, row, attempt_number)

    # Decide the next state
    if ok:
        status, delay = "DELIVERED", None
    elif attempt_number < config.MAX_ATTEMPTS:
        status, delay = "RETRY", topology.backoff_seconds(attempt_number)
    else:
        status, delay = "FAILED", None

    record_outcome(delivery_id, attempt_number, status, delay, code, err, duration_ms)
    log.info("delivery %s attempt %d -> %s (%s, %dms)",
             delivery_id, attempt_number, status, code or err, duration_ms)

    # Route the message
    if status == "RETRY":
        publish(ch, topology.EXCHANGE_RETRY, str(attempt_number), delivery_id)
    elif status == "FAILED":
        publish(ch, topology.EXCHANGE_DLQ, topology.DLQ_KEY, delivery_id,
                headers={"x-attempts": attempt_number, "x-last-error": err})


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    pool.open()
    pool.wait()

    conn = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
    ch = conn.channel()
    ch.confirm_delivery()
    topology.declare(ch)
    ch.basic_qos(prefetch_count=1)  # one in-flight message per worker; scale by running more workers

    with httpx.Client(timeout=config.HTTP_TIMEOUT) as http:

        def on_message(ch, method, props, body):
            try:
                delivery_id = str(UUID(json.loads(body)["delivery_id"]))
            except (ValueError, KeyError, TypeError):
                log.error("malformed message %r; dropping", body)
                ch.basic_ack(method.delivery_tag)
                return
            try:
                process(ch, http, delivery_id)
                ch.basic_ack(method.delivery_tag)  # only after state + routing are done
            except Exception:
                log.exception("failed processing %s; requeueing", delivery_id)
                time.sleep(1)  # avoid a hot loop if Postgres/RabbitMQ is down
                ch.basic_nack(method.delivery_tag, requeue=True)

        ch.basic_consume(queue=topology.MAIN_QUEUE, on_message_callback=on_message)
        log.info("worker started, waiting for messages")
        try:
            ch.start_consuming()
        except KeyboardInterrupt:
            ch.stop_consuming()
        finally:
            conn.close()
            pool.close()


if __name__ == "__main__":
    main()