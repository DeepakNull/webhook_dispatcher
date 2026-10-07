"""Wipe state for a clean test run: truncate tables, purge all queues, delete tracking_ids.txt."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pika
import psycopg

from app import config, topology

with psycopg.connect(config.DATABASE_URL, autocommit=True) as conn:
    conn.execute("TRUNCATE delivery_attempts, deliveries, webhooks")

conn = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
ch = conn.channel()
topology.declare(ch)
queues = [topology.MAIN_QUEUE, topology.DLQ] + [
    topology.retry_queue(n) for n in range(1, config.MAX_ATTEMPTS)
]
for q in queues:
    ch.queue_purge(q)
conn.close()

if os.path.exists("tracking_ids.txt"):
    os.remove("tracking_ids.txt")
print("reset done: tables truncated, queues purged")