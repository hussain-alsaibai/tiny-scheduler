# tiny-scheduler

[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Zero deps](https://img.shields.io/badge/dependencies-zero-success.svg)](#)
[![Tests](https://img.shields.io/badge/tests-57%20passing-brightgreen.svg)](test_tiny_scheduler.py)

> **Zero-dependency cron scheduler for Python. Cron expressions, intervals, one-shot timers, async. Pure stdlib.**

A single-file (~700 LOC) scheduler that gives you cron-style scheduling, interval timers, one-shot timers, and async support with **zero external dependencies**. Use it as a library in your application, run it as a long-lived process, or integrate it into an existing service.

## Features

- 🕒 **Cron expression parsing & matching** — full 5-field syntax including `*`, lists (`1,3,5`), ranges (`1-5`), steps (`*/5`), and named days/months (`mon-fri`, `jan`).
- ⏱️ **Interval-based timers** — every N seconds / minutes / hours, optionally with a `start_at` baseline.
- 💥 **One-shot timers** — fire once after a delay, then auto-remove.
- 🔄 **Async function support** — coroutine functions are detected and run via `asyncio.run()` automatically.
- 🌍 **Timezone-aware scheduling** — naive datetimes are treated as UTC; works correctly with any tzinfo.
- 🏷️ **Job metadata** — name, args, kwargs, tags for filtering and grouping.
- 🛑 **Max runs / TTL limits** — bound how many times a job can fire.
- 🧹 **Graceful shutdown** — `stop()` waits for in-flight jobs and joins the loop thread; context-manager friendly.
- 🎲 **Jitter** — random delay before execution to prevent thundering-herd on coordinated jobs.
- 🧵 **Thread pool execution** — scheduler ticks on a background thread; jobs run in a worker pool.
- 🚨 **Resilient by design** — exceptions in jobs are logged; the scheduler keeps running.
- 🎨 **Decorator-based registration** — `@scheduled("*/5 * * * *")` for declarative style.
- 📦 **Zero external dependencies** — pure Python 3.8+ stdlib (`asyncio`, `concurrent.futures`, `dataclasses`, `datetime`, `threading`).
- 📜 **MIT licensed** — permissive, do what you want.

## Installation

```bash
pip install tiny_scheduler
```

Or just drop `tiny_scheduler.py` into your project — it's a single file.

## Quick Start

### Cron-style scheduling

```python
from tiny_scheduler import Scheduler

def backup_database():
    print("running pg_dump...")

sched = Scheduler()
sched.add_cron(backup_database, "0 2 * * *", name="daily-backup")  # 2 AM every day
sched.start()

# Block forever, or do other work on the main thread.
```

### Interval-based scheduling

```python
sched.add_interval(check_health, seconds=30, name="health-check")
sched.add_interval(send_metrics, minutes=5, name="metrics")
```

### One-shot timers

```python
sched.add_once(send_welcome_email, delay=5, args=(user.email,), name="welcome")
```

### Async functions

Async coroutine functions are detected automatically and run via `asyncio.run()`:

```python
async def fetch_status():
    # ... use aiohttp, asyncpg, etc.
    return {"ok": True}

sched.add_interval(fetch_status, seconds=60, name="status")
```

## Cron Syntax

| Field | Range | Special |
|-------|-------|---------|
| minute | 0-59 | `*/15` = every 15 minutes |
| hour | 0-23 | `9-17` = business hours |
| day | 1-31 | `1,15` = 1st and 15th |
| month | 1-12 | `jan,apr,jul,oct` (names work) |
| dow | 0-6 (Sun=0) | `mon-fri` (names work) |

### Examples

| Expression | Meaning |
|-----------|---------|
| `* * * * *` | Every minute |
| `*/5 * * * *` | Every 5 minutes |
| `0 * * * *` | Top of every hour |
| `0 0 * * *` | Midnight every day |
| `0 9-17 * * mon-fri` | Hourly, 9 AM–5 PM, weekdays only |
| `30 14 * * fri` | 2:30 PM every Friday |
| `0 0 1 jan *` | New Year's Day |
| `*/10 9-12,14-18 * * *` | Every 10 min, 9-12 and 14-18 |

## Decorator Usage

The `@scheduled` decorator stores scheduling metadata on a function. You can then use `Scheduler.bind_all()` (or attach manually) to register them.

```python
from tiny_scheduler import scheduled, Scheduler

@scheduled("*/5 * * * *")
def sync_external():
    ...

@scheduled(interval=60, name="heartbeat", tags=("monitoring",))
def ping():
    ...

@scheduled(interval=30, jitter=5.0)
def scrape():  # fires every ~30s with 0-5s jitter
    ...

sched = Scheduler()
# Manual registration using the captured metadata:
for fn in [sync_external, ping, scrape]:
    meta = fn.__tiny_schedule__
    if meta["kind"] == "cron":
        sched.add_cron(fn, meta["value"], name=meta["name"], tags=meta["tags"])
    elif meta["kind"] == "interval":
        sched.add_interval(fn, seconds=meta["value"], name=meta["name"], tags=meta["tags"])
sched.start()
```

## Timezone Handling

```python
from datetime import datetime, timezone

# Naive datetimes are treated as UTC.
job = sched.add_cron(my_func, "0 9 * * *", name="morning")

# Use timezone-aware datetimes for start_at:
from datetime import datetime, timezone, timedelta
tz = timezone(timedelta(hours=-5))  # EST
sched.add_interval(my_func, hours=1, name="hourly", start_at=datetime.now(tz))
```

All internally-tracked times are stored as tz-aware UTC datetimes, so DST and timezone math are well-defined.

## Shutdown Handling

```python
# Context manager: stops on exit.
with Scheduler() as sched:
    sched.add_cron(my_func, "*/5 * * * *")

# Manual:
sched = Scheduler()
sched.start()
# ... later ...
sched.stop(wait=True)  # wait for in-flight jobs to finish
```

Jobs that raise are caught and logged. The scheduler never crashes because of a single bad job.

## Comparison

| Feature | tiny_scheduler | schedule | APScheduler |
|---------|:-:|:-:|:-:|
| External dependencies | **0** | 0 | 3+ |
| Cron expressions | ✅ full | ⚠️ limited | ✅ full |
| Interval timers | ✅ | ✅ | ✅ |
| One-shot timers | ✅ | ⚠️ `every().day.at()` hack | ✅ |
| Async functions | ✅ auto-detect | ❌ | ✅ |
| Job metadata & tags | ✅ | ❌ | ✅ |
| Max runs / TTL | ✅ | ❌ | ❌ |
| Jitter | ✅ | ❌ | ⚠️ via plugin |
| Thread pool | ✅ | ❌ | ✅ |
| Process pool | ❌ | ❌ | ✅ |
| Persistent job store | ❌ | ❌ | ✅ |
| Lines of code | **~700** | ~1500 | ~5000 |

**Pick tiny_scheduler** when you want a small, dependency-free, in-process scheduler for cron-style work.

**Pick APScheduler** when you need persistent jobs, multi-process scheduling, or advanced calendar triggers.

## Ecosystem

tiny-scheduler pairs well with:

- **tiny-logger** — structured logging with zero deps (planned).
- **tiny-config** — TOML-based configuration (planned).
- **tiny-router** — micro HTTP router (planned).
- **tiny-orm** — minimal SQL toolkit (planned).

## API Reference

### `Scheduler`

| Method | Description |
|--------|-------------|
| `add_cron(func, expr, name=None, tags=None, max_runs=None, jitter=None, args=(), kwargs=None)` | Register a cron job. |
| `add_interval(func, seconds=None, minutes=None, hours=None, start_at=None, name=None, tags=None, max_runs=None, jitter=None, args=(), kwargs=None)` | Register an interval job. |
| `add_once(func, delay, name=None, tags=None, args=(), kwargs=None)` | Register a one-shot job. |
| `add(name, schedule, func, *args, **kwargs)` | Generic registration with an explicit schedule object. |
| `remove(job_or_name)` | Remove a job by reference or name. |
| `cancel(job_or_name)` | Alias for `remove`. |
| `get_jobs(tag=None)` | List jobs, optionally filtered by tag. |
| `next_run(job_name=None)` | Get the next scheduled run. |
| `start(block=False)` | Start the scheduler (background thread by default). |
| `stop(wait=True)` | Stop the scheduler. |
| `jobs` (property) | Snapshot of all jobs. |
| `running` (property) | Whether the scheduler thread is alive. |
| `__enter__` / `__exit__` | Context-manager protocol. |

### `Job`

| Attribute / Method | Description |
|--------------------|-------------|
| `name` | Human-readable identifier. |
| `next_run` | datetime of the next firing. |
| `last_run` | datetime of the most recent run, or None. |
| `run_count` | Number of executions so far. |
| `due` (property) | Whether the job is due right now. |
| `overdue_seconds` (property) | Seconds past `next_run` (0 if not overdue). |
| `cancelled` (property) | Whether the job has been cancelled. |
| `tags` | Tuple of tags. |
| `run()` | Execute the job synchronously. |
| `cancel()` | Mark the job as cancelled. |
| `tick()` | Advance `next_run` to the next firing. |

### `IntervalSchedule(seconds=None, minutes=None, hours=None, start_at=None)`

At least one of `seconds`, `minutes`, or `hours` must be set (positive).

### `CronSchedule(expr)`

Parses and evaluates 5-field cron expressions.

### `@scheduled(expr=None, *, interval=None, name=None, tags=None, **kwargs)`

Decorator that records scheduling metadata on the function for later registration. Requires either `expr` or `interval`.

## Development

```bash
# Run tests
python -m pytest test_tiny_scheduler.py -v

# Run an example
PYTHONPATH=. python examples/basic.py
PYTHONPATH=. python examples/web_hook.py
PYTHONPATH=. python examples/async_jobs.py
```

## License

MIT — see [LICENSE](LICENSE).

---

Built with ❤ by [Hussain Al-Saibai](https://github.com/hussain-alsaibai).