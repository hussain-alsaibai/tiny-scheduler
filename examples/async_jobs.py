"""Running async functions with tiny_scheduler.

The scheduler detects coroutine functions and runs them via asyncio.run().
Each async job gets its own event loop in its worker thread.

Run with:
    python examples/async_jobs.py
"""
import asyncio
import logging
import signal
import time

from tiny_scheduler import Scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


async def fetch_status():
    """Async job: simulate fetching a status page and log the result."""
    # In real life: async with aiohttp.ClientSession() as s: ...
    await asyncio.sleep(0.1)  # simulate network latency
    print(f"[async] status fetched at {time.time():.2f}")


async def batch_process():
    """Async job: process a batch of items concurrently."""
    items = list(range(5))

    async def work(i: int):
        await asyncio.sleep(0.05)
        return i * 2

    results = await asyncio.gather(*(work(i) for i in items))
    print(f"[async] processed {len(items)} items, results={results}")


async def shutdown_async():
    """Async cleanup — useful for graceful async shutdown."""
    await asyncio.sleep(0.05)
    print("[async] cleanup done")


def main():
    sched = Scheduler()

    # Async functions are detected via inspect.iscoroutinefunction().
    sched.add_interval(fetch_status, seconds=10, name="status-check")
    sched.add_interval(batch_process, minutes=1, name="batch")
    sched.add_once(shutdown_async, delay=1, name="startup-cleanup")

    def stop(signum, frame):
        sched.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, stop)

    print(f"scheduler started with {len(sched.jobs)} async jobs")
    sched.start()
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()