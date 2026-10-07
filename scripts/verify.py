"""Reconcile the system after a load test.

Usage: python scripts/verify.py [--timeout 180] [--client http://localhost:9000]

Checks (exit code 0 only if all pass):
  1. every tracking ID reached a terminal state (DELIVERED or FAILED)  -> nothing stuck
  2. every FAILED delivery has exactly MAX_ATTEMPTS attempts
  3. every FAILED delivery's message is sitting in webhook_dlq          -> no lost payloads
"""
import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
import pika
import psycopg
from psycopg.rows import dict_row

from app import config, topology


def wait_for_terminal(conn, ids, timeout):
    start = time.time()
    while True:
        pending = conn.execute(
            "SELECT count(*) AS n FROM deliveries WHERE id = ANY(%s) "
            "AND status NOT IN ('DELIVERED', 'FAILED')", (ids,),
        ).fetchone()["n"]
        print(f"\r  waiting... {pending} deliveries not yet terminal   ", end="", flush=True)
        if pending == 0 or time.time() - start > timeout:
            print()
            return pending
        time.sleep(2)


def read_dlq_ids():
    """Read every message in the DLQ without consuming it (unacked -> requeued on close)."""
    conn = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
    ch = conn.channel()
    ids = []
    while True:
        method, _, body = ch.basic_get(topology.DLQ, auto_ack=False)
        if method is None:
            break
        ids.append(json.loads(body)["delivery_id"])
    ch.close()  # unacked messages go back to the queue
    conn.close()
    return ids


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--timeout", type=int, default=180)
    p.add_argument("--client", default="http://localhost:9000")
    args = p.parse_args()

    ids = [UUID(x) for x in Path("tracking_ids.txt").read_text().split()]
    total = len(ids)
    print(f"Verifying {total} events\n")

    with psycopg.connect(config.DATABASE_URL, row_factory=dict_row) as conn:
        stuck = wait_for_terminal(conn, ids, args.timeout)

        by_status = {r["status"]: r["n"] for r in conn.execute(
            "SELECT status, count(*) AS n FROM deliveries WHERE id = ANY(%s) GROUP BY status", (ids,))}
        delivered, failed = by_status.get("DELIVERED", 0), by_status.get("FAILED", 0)

        hist = conn.execute(
            "SELECT status, attempt_count, count(*) AS n FROM deliveries WHERE id = ANY(%s) "
            "GROUP BY status, attempt_count ORDER BY status, attempt_count", (ids,)).fetchall()

        bad_failed = conn.execute(
            "SELECT count(*) AS n FROM deliveries WHERE id = ANY(%s) AND status='FAILED' "
            "AND attempt_count <> %s", (ids, config.MAX_ATTEMPTS)).fetchone()["n"]

        total_attempts = conn.execute(
            "SELECT count(*) AS n FROM delivery_attempts WHERE delivery_id = ANY(%s)", (ids,)
        ).fetchone()["n"]

        lat = conn.execute(
            """
            SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY lat) AS p
            FROM (
                SELECT EXTRACT(EPOCH FROM (max(a.attempted_at) - d.created_at)) AS lat
                FROM deliveries d JOIN delivery_attempts a ON a.delivery_id = d.id
                WHERE d.id = ANY(%s) AND d.status = 'DELIVERED'
                GROUP BY d.id, d.created_at
            ) t
            """, (ids,)).fetchone()["p"]

        failed_ids = {str(r["id"]) for r in conn.execute(
            "SELECT id FROM deliveries WHERE id = ANY(%s) AND status='FAILED'", (ids,))}

    dlq_ids = read_dlq_ids()
    dlq_counts = Counter(dlq_ids)
    missing_from_dlq = failed_ids - set(dlq_counts)
    dup_in_dlq = [i for i in failed_ids if dlq_counts[i] > 1]

    print("Postgres state")
    print(f"  DELIVERED: {delivered}   FAILED: {failed}   stuck/non-terminal: {stuck}")
    print("  attempts histogram (status, attempts -> count):")
    for r in hist:
        print(f"    {r['status']:<10} {r['attempt_count']} attempt(s): {r['n']}")
    print(f"  total HTTP attempts logged: {total_attempts}  (avg {total_attempts/total:.2f} per event)")
    if lat:
        print(f"  delivery latency (created -> delivered): "
              f"p50={lat[0]:.2f}s  p95={lat[1]:.2f}s  p99={lat[2]:.2f}s")

    print("\nRabbitMQ")
    print(f"  FAILED rows in DB: {len(failed_ids)}   of those found in webhook_dlq: "
          f"{len(failed_ids) - len(missing_from_dlq)}")
    if len(dlq_ids) > len(failed_ids):
        print(f"  (note: DLQ holds {len(dlq_ids)} total; extras are from earlier runs)")

    try:
        s = httpx.get(f"{args.client}/stats", timeout=5).json()
        print("\nMock client (cumulative since it started)")
        print(f"  received {s['received']} requests: {s['ok']} x 200, {s['failed']} x 503")
        print(f"  200s sent vs DELIVERED rows: {s['ok']} vs {delivered}"
              f"  {'(no duplicate deliveries)' if s['ok'] == delivered else '(duplicates or stale counters)'}")
    except Exception:
        print("\n(mock client stats unavailable; skipped)")

    checks = {
        "no deliveries stuck": stuck == 0,
        f"every FAILED has exactly {config.MAX_ATTEMPTS} attempts": bad_failed == 0,
        "every FAILED payload is in webhook_dlq": not missing_from_dlq,
        "no duplicate DLQ entries": not dup_in_dlq,
        "DELIVERED + FAILED == total": delivered + failed == total,
    }
    print("\nChecks")
    for name, ok in checks.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    ok_all = all(checks.values())
    print(f"\n{'ALL CHECKS PASSED' if ok_all else 'VERIFICATION FAILED'}")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()