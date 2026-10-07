"""Fire N events at the ingestion API and save the tracking IDs.

Usage: python scripts/fire_events.py -n 1000
"""
import argparse
import asyncio
import time

import httpx


async def main(n: int, api: str, target: str, concurrency: int) -> None:
    sem = asyncio.Semaphore(concurrency)
    ids: list[str] = []
    errors = 0

    async with httpx.AsyncClient(base_url=api, timeout=10) as client:

        async def send(i: int) -> None:
            nonlocal errors
            async with sem:
                try:
                    r = await client.post(
                        "/events/publish",
                        json={"subscriber_url": target, "payload": {"event_no": i}},
                    )
                    r.raise_for_status()
                    ids.append(r.json()["tracking_id"])
                except httpx.HTTPError:
                    errors += 1

        start = time.perf_counter()
        await asyncio.gather(*(send(i) for i in range(n)))
        elapsed = time.perf_counter() - start

    with open("tracking_ids.txt", "w") as f:
        f.write("\n".join(ids))

    print(f"accepted: {len(ids)}  errors: {errors}  in {elapsed:.2f}s")
    print("tracking IDs saved to tracking_ids.txt")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("-n", type=int, default=1000)
    p.add_argument("--api", default="http://localhost:8000")
    p.add_argument("--target", default="http://localhost:9000/receive")
    p.add_argument("--concurrency", type=int, default=50)
    a = p.parse_args()
    asyncio.run(main(a.n, a.api, a.target, a.concurrency))