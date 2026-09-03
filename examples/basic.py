"""Basic examples: cron, interval, and one-shot timers.

Run with:
    python examples/basic.py
"""
import logging
import signal
import sys
import time
from datetime import datetime

from tiny_scheduler import Scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def cron_job(name: str):
    """A cron-style job — runs every minute."""
    print(f"[cron] {name} fired at {datetime.utcnow().isoformat()}")


def interval_job():
    """An interval-style job — runs every 5 seconds."""
    print(f"[interval] heartbeat at {datetime.utcnow().isoformat()}")


def one_shot_job():
    """A one-shot job — runs once after the delay."""
    print(f"[once] startup complete at {datetime.utcnow().isoformat()}")


def main():
    sched = Scheduler()

    # Cron: every minute.
    sched.add_cron(cron_job, "* * * * *", name="every-minute", args=("every-minute",))

    # Cron: every 5 minutes during business hours.
    sched.add_cron(
        cron_job,
        "*/5 9-17 * * mon-fri",
        name="biz-hours",
        args=("biz-hours",),
        tags=("business", "weekday"),
    )

    # Interval: every 5 seconds.
    sched.add_interval(interval_job, seconds=5, name="heartbeat", tags=("monitoring",))

    # Interval with jitter (avoid thundering herd on coordinated jobs).
    sched.add_interval(interval_job, seconds=10, name="jittered", jitter=2.0)

    # One-shot: fire once after 2 seconds.
    sched.add_once(one_shot_job, delay=2, name="startup-banner")

    # Graceful shutdown on Ctrl-C.
    def shutdown(signum, frame):
        print("\nshutting down...")
        sched.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    print(f"scheduler started with {len(sched.jobs)} jobs")
    sched.start()
    try:
        # Keep main thread alive.
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(0, None)


if __name__ == "__main__":
    main()