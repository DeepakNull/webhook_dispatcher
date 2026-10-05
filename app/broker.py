"""Publisher side of RabbitMQ.

pika's BlockingConnection is not thread-safe, and FastAPI runs sync endpoints in a
thread pool, so each thread gets its own connection/channel (lazily created, and
recreated if RabbitMQ dropped it).
"""
import json
import threading

import pika
from pika.exceptions import AMQPError

from . import config, topology

_local = threading.local()


def _connect():
    conn = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
    ch = conn.channel()
    ch.confirm_delivery()  # broker confirms the message is safely accepted
    topology.declare(ch)
    return conn, ch


def _channel():
    conn = getattr(_local, "conn", None)
    ch = getattr(_local, "ch", None)
    if conn is None or conn.is_closed or ch is None or ch.is_closed:
        _local.conn, _local.ch = _connect()
    return _local.ch


def publish_delivery(delivery_id: str) -> None:
    body = json.dumps({"delivery_id": delivery_id})
    props = pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,  # survive a broker restart
        content_type="application/json",
    )
    for attempt in (1, 2):  # one reconnect-and-retry for stale connections
        try:
            _channel().basic_publish(
                exchange=topology.EXCHANGE_MAIN,
                routing_key=topology.MAIN_KEY,
                body=body,
                properties=props,
                mandatory=True,  # error if the message can't be routed to a queue
            )
            return
        except AMQPError:
            _local.conn = None
            if attempt == 2:
                raise