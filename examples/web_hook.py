"""Tiny router integration: periodic tasks trigger webhook-style handlers.

This example shows how to use tiny_scheduler alongside a tiny HTTP framework
(such as FastAPI, Flask, or stdlib http.server). The scheduler runs in the
background while the web server handles incoming requests.

Run with:
    python examples/web_hook.py

Then hit http://localhost:8765/ to see the latest job stats.
"""
import json
import logging
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

from tiny_scheduler import Scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

# Shared state populated by the scheduler and read by the HTTP handler.
state = {
    "hits": 0,
    "errors": 0,
    "last_sync": None,
    "last_report": None,
    "history": [],
}


def sync_external():
    """Pretend to call out to an external system every 5 seconds."""
    state["hits"] += 1
    state["last_sync"] = datetime.utcnow().isoformat()
    state["history"].append({"kind": "sync", "at": state["last_sync"]})
    state["history"] = state["history"][-50:]
    print(f"[sync] hit #{state['hits']}")


def hourly_report():
    """Build a report once an hour."""
    state["last_report"] = datetime.utcnow().isoformat()
    state["history"].append({"kind": "report", "at": state["last_report"]})
    print(f"[report] generated at {state['last_report']}")


def flaky_health_check():
    """Demonstrates that a failing job doesn't break the scheduler."""
    state["errors"] += 1
    if state["errors"] % 3 == 0:
        raise RuntimeError("simulated health-check failure")


class StatsHandler(BaseHTTPRequestHandler):
    """Tiny HTTP handler that returns scheduler statistics."""

    def do_GET(self):
        if self.path == "/":
            payload = json.dumps(
                {
                    "hits": state["hits"],
                    "errors": state["errors"],
                    "last_sync": state["last_sync"],
                    "last_report": state["last_report"],
                    "history_len": len(state["history"]),
                },
                indent=2,
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Silence default logging.
        pass


def run_http(port: int = 8765):
    server = HTTPServer(("0.0.0.0", port), StatsHandler)
    print(f"[http] listening on http://localhost:{port}")
    server.serve_forever()


def main():
    sched = Scheduler()

    # Web: every 5 seconds, mark a hit and update last_sync.
    sched.add_interval(sync_external, seconds=5, name="sync", tags=("io",))

    # Cron: at the top of every hour.
    sched.add_cron(hourly_report, "0 * * * *", name="hourly-report", tags=("report",))

    # Cron: every 30 seconds (with jitter) — also demonstrate resilience.
    sched.add_interval(flaky_health_check, seconds=30, name="health", jitter=5)

    http_thread = threading.Thread(target=run_http, daemon=True)
    http_thread.start()

    print(f"scheduler running with {len(sched.jobs)} jobs")
    sched.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nshutting down...")
        sched.stop()


if __name__ == "__main__":
    main()