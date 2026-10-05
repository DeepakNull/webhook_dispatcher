"""RabbitMQ topology, shared by the API and the worker.

Flow:
  publish -> [webhooks] --deliver--> webhook_main -> worker
  failure -> [webhooks.retry] --n--> webhook_retry_n (fixed TTL, no consumer)
             -> TTL expires -> dead-lettered back to [webhooks] -> webhook_main
  exhausted -> [webhooks.dlq] --dead--> webhook_dlq
"""
from . import config

EXCHANGE_MAIN = "webhooks"
EXCHANGE_RETRY = "webhooks.retry"
EXCHANGE_DLQ = "webhooks.dlq"

MAIN_QUEUE = "webhook_main"
DLQ = "webhook_dlq"

MAIN_KEY = "deliver"
DLQ_KEY = "dead"


def retry_queue(attempt_count: int) -> str:
    return f"webhook_retry_{attempt_count}"


def backoff_seconds(attempt_count: int) -> int:
    # attempt_count is the number of attempts made so far: 1 -> 2s, 2 -> 4s, 3 -> 8s, 4 -> 16s
    return 1 * (2 ** attempt_count)


def declare(ch) -> None:
    """Idempotent: safe to call on every startup from API and worker."""
    for ex in (EXCHANGE_MAIN, EXCHANGE_RETRY, EXCHANGE_DLQ):
        ch.exchange_declare(exchange=ex, exchange_type="direct", durable=True)

    ch.queue_declare(queue=MAIN_QUEUE, durable=True)
    ch.queue_bind(queue=MAIN_QUEUE, exchange=EXCHANGE_MAIN, routing_key=MAIN_KEY)

    # One retry queue per delay level. A queue-level TTL means every message in it
    # expires in order, which avoids head-of-line blocking from per-message TTLs.
    for n in range(1, config.MAX_ATTEMPTS):
        q = retry_queue(n)
        ch.queue_declare(
            queue=q,
            durable=True,
            arguments={
                "x-message-ttl": backoff_seconds(n) * 1000,
                "x-dead-letter-exchange": EXCHANGE_MAIN,
                "x-dead-letter-routing-key": MAIN_KEY,
            },
        )
        ch.queue_bind(queue=q, exchange=EXCHANGE_RETRY, routing_key=str(n))

    ch.queue_declare(queue=DLQ, durable=True)
    ch.queue_bind(queue=DLQ, exchange=EXCHANGE_DLQ, routing_key=DLQ_KEY)