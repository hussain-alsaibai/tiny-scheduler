"""tiny_scheduler — Zero-dependency cron-style scheduler for Python.

A single-file scheduler supporting:
- Cron expressions (5-field: min hour dom mon dow)
- Interval timers (every N seconds/minutes/hours)
- One-shot timers (run once after delay)
- Async function support
- Timezone-aware scheduling
- Job metadata (name, args, kwargs, tags)
- Max runs / TTL limits
- Graceful shutdown with event
- Jitter (random delay to prevent thundering herd)

Usage:
    from tiny_scheduler import Scheduler, scheduled

    sched = Scheduler()
    sched.add_cron(my_func, "*/5 * * * *", name="every-5-min")
    sched.start()
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import random
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, List, Optional, Union

__version__ = "0.1.0"
__all__ = [
    "Job",
    "CronSchedule",
    "IntervalSchedule",
    "Scheduler",
    "scheduled",
]

log = logging.getLogger("tiny_scheduler")

# ---------------------------------------------------------------------------
# Cron expression parsing & matching
# ---------------------------------------------------------------------------

# Day-of-week name mapping (cron accepts both 0-6 and mon-sun).
_DOW_NAMES = {
    "sun": 0, "mon": 1, "tue": 2, "wed": 3,
    "thu": 4, "fri": 5, "sat": 6,
}

# Field ranges: (min, max)
_CRON_FIELDS = [
    ("minute", 0, 59),
    ("hour", 0, 23),
    ("day", 1, 31),
    ("month", 1, 12),
    ("dow", 0, 6),  # Sunday=0
]

# Month name mapping for convenience.
_MONTH_NAMES = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _expand_field(token: str, lo: int, hi: int, names: Optional[dict] = None) -> set:
    """Expand a single cron field token into a set of matching integer values.

    Supports:
      *         -> all values in [lo, hi]
      5         -> specific value
      1,3,5     -> list of values
      1-5       -> inclusive range
      */5       -> step (every 5)
      1-10/2    -> range with step
    """
    names = names or {}
    result: set = set()

    for part in token.split(","):
        # Handle step syntax: */N or A-B/N
        if "/" in part:
            range_part, step = part.split("/", 1)
            step = int(step)
        else:
            range_part = part
            step = 1

        # Handle range or wildcard
        if range_part == "*":
            start, end = lo, hi
        elif "-" in range_part:
            start_s, end_s = range_part.split("-", 1)
            start = _parse_value(start_s, names)
            end = _parse_value(end_s, names)
        else:
            start = end = _parse_value(range_part, names)

        # Clamp and iterate.
        start = max(lo, min(start, hi))
        end = max(lo, min(end, hi))
        result.update(range(start, end + 1, step))

    return result


def _parse_value(s: str, names: dict) -> int:
    """Parse a value token, handling mon/tue/... and jan/feb/... aliases."""
    s = s.strip().lower()
    if s in names:
        return names[s]
    return int(s)


class CronSchedule:
    """Parsed cron expression (5-field: minute hour day month day-of-week).

    Supports the common subset of cron syntax:
      *           any
      5           specific value
      1,3,5       list
      1-5         range
      */5         step (every 5)
      mon,tue     day-of-week names
    """

    def __init__(self, expr: str) -> None:
        if not expr or not isinstance(expr, str):
            raise ValueError(f"cron expression must be a non-empty string, got {expr!r}")

        parts = expr.strip().split()
        if len(parts) != 5:
            raise ValueError(
                f"cron expression must have exactly 5 fields (min hour dom mon dow), got {len(parts)}: {expr!r}"
            )

        # Expand each field into its set of valid integer values.
        self.minute = _expand_field(parts[0], 0, 59)
        self.hour = _expand_field(parts[1], 0, 23)
        self.day = _expand_field(parts[2], 1, 31)
        self.month = _expand_field(parts[3], 1, 12, _MONTH_NAMES)
        self.dow = _expand_field(parts[4], 0, 6, _DOW_NAMES)

        self.expr = expr.strip()

    def matches(self, t: datetime) -> bool:
        """Check whether the given datetime matches this cron expression."""
        # Day-of-week vs day-of-month: in classic cron, if both are restricted
        # (i.e., not '*'), they're OR'd. If either is '*', both must match.
        # We mirror that behaviour here for compatibility.
        minute_ok = t.minute in self.minute
        hour_ok = t.hour in self.hour
        month_ok = t.month in self.month
        day_ok = t.day in self.day
        # Python weekday(): Mon=0..Sun=6. Our dow: Sun=0..Sat=6.
        cron_dow = (t.weekday() + 1) % 7
        dow_ok = cron_dow in self.dow

        # Day field is '*' and dow is restricted -> dow-only matching.
        day_field = self.expr.split()[2]
        dow_field = self.expr.split()[4]
        if day_field == "*" and dow_field != "*":
            return minute_ok and hour_ok and month_ok and dow_ok
        if dow_field == "*" and day_field != "*":
            return minute_ok and hour_ok and month_ok and day_ok
        # Both restricted: OR semantics; otherwise AND.
        if day_field != "*" and dow_field != "*":
            return minute_ok and hour_ok and month_ok and (day_ok or dow_ok)
        return minute_ok and hour_ok and month_ok and day_ok and dow_ok

    def next(self, from_time: Optional[datetime] = None) -> datetime:
        """Compute the next datetime (>= from_time) matching this cron.

        If from_time is naive, it is treated as UTC. Resolution is minute-level.
        """
        if from_time is None:
            from_time = datetime.now(timezone.utc)
        elif from_time.tzinfo is None:
            from_time = from_time.replace(tzinfo=timezone.utc)

        # Start at the next minute to avoid matching 'from_time' itself.
        candidate = from_time.replace(second=0, microsecond=0) + timedelta(minutes=1)

        # Bound the search to avoid pathological loops; 4 years covers all
        # month/day combinations including leap years.
        deadline = candidate + timedelta(days=366 * 4)

        while candidate < deadline:
            if candidate.month not in self.month:
                # Jump to the first of the next valid month.
                candidate = _next_month(candidate)
                continue
            if candidate.day not in self.day:
                candidate = candidate + timedelta(days=1)
                candidate = candidate.replace(hour=0, minute=0)
                continue
            if candidate.hour not in self.hour:
                candidate = candidate + timedelta(hours=1)
                candidate = candidate.replace(minute=0)
                continue
            if candidate.minute not in self.minute:
                candidate = candidate + timedelta(minutes=1)
                continue
            return candidate
        raise RuntimeError(f"could not find next match for cron {self.expr!r} within 4 years")


def _next_month(t: datetime) -> datetime:
    """Return the first instant of the next month."""
    if t.month == 12:
        return t.replace(year=t.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return t.replace(month=t.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Interval schedule
# ---------------------------------------------------------------------------


class IntervalSchedule:
    """Repeating schedule firing every N seconds/minutes/hours."""

    def __init__(
        self,
        seconds: Optional[Union[int, float]] = None,
        minutes: Optional[Union[int, float]] = None,
        hours: Optional[Union[int, float]] = None,
        start_at: Optional[datetime] = None,
    ) -> None:
        # At least one of the interval units must be set.
        units = [v for v in (seconds, minutes, hours) if v is not None]
        if not units:
            raise ValueError("IntervalSchedule requires seconds, minutes, or hours")
        for v in units:
            if v <= 0:
                raise ValueError("interval units must be positive")

        self.seconds = seconds or 0
        self.minutes = minutes or 0
        self.hours = hours or 0
        self.start_at = start_at

        self._delta = timedelta(
            seconds=self.seconds,
            minutes=self.minutes,
            hours=self.hours,
        )

    def next(self, from_time: Optional[datetime] = None) -> datetime:
        """Compute the next run time given a starting reference."""
        if from_time is None:
            from_time = datetime.now(timezone.utc)
        elif from_time.tzinfo is None:
            from_time = from_time.replace(tzinfo=timezone.utc)

        if self.start_at is not None:
            # If start_at is in the future, use it as the first run.
            if self.start_at.tzinfo is None:
                start = self.start_at.replace(tzinfo=timezone.utc)
            else:
                start = self.start_at
            if from_time < start:
                return start

        return from_time + self._delta


# ---------------------------------------------------------------------------
# Job & Scheduler
# ---------------------------------------------------------------------------


@dataclass
class Job:
    """A scheduled unit of work.

    Attributes:
        name:        Human-readable identifier.
        func:        Callable to execute.
        args/kwargs: Positional/keyword arguments passed to func.
        schedule:    CronSchedule, IntervalSchedule, or one-shot delay.
        next_run:    Next datetime this job should fire (None = unscheduled).
        last_run:    datetime of the most recent execution, if any.
        run_count:   Number of times this job has executed.
        max_runs:    Maximum number of executions (None = unlimited).
        jitter:      Maximum random delay (seconds) added before execution.
        tags:        Free-form tags for filtering/grouping.
        one_shot:    If True, the job is removed after its first execution.
        cancel_event: Threading event signalling that the job was cancelled.
    """

    name: str
    func: Callable
    schedule: Any
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    run_count: int = 0
    max_runs: Optional[int] = None
    jitter: float = 0
    tags: tuple = field(default_factory=tuple)
    one_shot: bool = False
    cancel_event: threading.Event = field(default_factory=threading.Event)

    @property
    def due(self) -> bool:
        """True if the job is due to run right now."""
        if self.next_run is None or self.cancelled:
            return False
        now = datetime.now(timezone.utc)
        return now >= self.next_run

    @property
    def overdue_seconds(self) -> float:
        """How many seconds past `next_run` the current time is (0 if not overdue)."""
        if self.next_run is None:
            return 0.0
        delta = (datetime.now(timezone.utc) - self.next_run).total_seconds()
        return max(0.0, delta)

    @property
    def cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def run(self) -> Any:
        """Execute the job, return its result. Async funcs are awaited."""
        if self.cancelled:
            return None
        result = _invoke(self.func, self.args, self.kwargs)
        self.last_run = datetime.now(timezone.utc)
        self.run_count += 1
        return result

    def cancel(self) -> None:
        """Mark this job as cancelled (it will be removed on the next tick)."""
        self.cancel_event.set()

    def matches_cron(self, cron_expr: str) -> bool:
        """Return True if this job's schedule is a CronSchedule matching expr."""
        if not isinstance(self.schedule, CronSchedule):
            return False
        return self.schedule.expr == cron_expr.strip()

    def tick(self) -> None:
        """Advance next_run based on the schedule (no-op for one_shot)."""
        if self.cancelled or self.one_shot:
            return
        if self.schedule is None:
            return
        self.next_run = self.schedule.next()


def _invoke(func: Callable, args: tuple, kwargs: dict) -> Any:
    """Invoke func, awaiting it if it's a coroutine function."""
    if inspect.iscoroutinefunction(func):
        # Run the coroutine to completion in a fresh event loop.
        return asyncio.run(func(*args, **kwargs))
    return func(*args, **kwargs)


class Scheduler:
    """Background-driven scheduler. Jobs run in a thread pool.

    Usage:
        sched = Scheduler(timezone="America/New_York")
        sched.add_cron(my_func, "*/5 * * * *", name="every-5-min")
        sched.start()
    """

    def __init__(
        self,
        timezone: Optional[str] = None,
        max_workers: int = 4,
        daemon_threads: bool = True,
    ) -> None:
        import concurrent.futures as cf

        self._jobs: List[Job] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._timezone = timezone
        self._executor = cf.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="tiny-sched-worker",
        )
        self._executor.daemon = daemon_threads

    # -- registration ------------------------------------------------------

    def add_cron(
        self,
        func: Callable,
        expr: str,
        name: Optional[str] = None,
        tags: Optional[Iterable[str]] = None,
        max_runs: Optional[int] = None,
        jitter: Optional[float] = None,
        args: tuple = (),
        kwargs: Optional[dict] = None,
    ) -> Job:
        """Register a cron-style job."""
        schedule = CronSchedule(expr)
        name = name or func.__name__
        job = Job(
            name=name,
            func=func,
            schedule=schedule,
            args=args,
            kwargs=kwargs or {},
            tags=tuple(tags or ()),
            max_runs=max_runs,
            jitter=jitter or 0,
        )
        job.next_run = schedule.next()
        with self._lock:
            self._jobs.append(job)
        return job

    def add_interval(
        self,
        func: Callable,
        seconds: Optional[float] = None,
        minutes: Optional[float] = None,
        hours: Optional[float] = None,
        start_at: Optional[datetime] = None,
        name: Optional[str] = None,
        tags: Optional[Iterable[str]] = None,
        max_runs: Optional[int] = None,
        jitter: Optional[float] = None,
        args: tuple = (),
        kwargs: Optional[dict] = None,
    ) -> Job:
        """Register an interval-style job."""
        schedule = IntervalSchedule(seconds=seconds, minutes=minutes, hours=hours, start_at=start_at)
        name = name or func.__name__
        job = Job(
            name=name,
            func=func,
            schedule=schedule,
            args=args,
            kwargs=kwargs or {},
            tags=tuple(tags or ()),
            max_runs=max_runs,
            jitter=jitter or 0,
        )
        job.next_run = schedule.next()
        with self._lock:
            self._jobs.append(job)
        return job

    def add_once(
        self,
        func: Callable,
        delay: float,
        name: Optional[str] = None,
        tags: Optional[Iterable[str]] = None,
        args: tuple = (),
        kwargs: Optional[dict] = None,
    ) -> Job:
        """Register a one-shot job firing once after `delay` seconds."""
        schedule = IntervalSchedule(seconds=delay)
        name = name or func.__name__
        job = Job(
            name=name,
            func=func,
            schedule=schedule,
            args=args,
            kwargs=kwargs or {},
            tags=tuple(tags or ()),
            one_shot=True,
        )
        job.next_run = schedule.next()
        with self._lock:
            self._jobs.append(job)
        return job

    def add(self, name: str, schedule: Any, func: Callable, *args: Any, **kwargs: Any) -> Job:
        """Generic job registration with an explicit schedule object."""
        job = Job(name=name, func=func, schedule=schedule, args=args, kwargs=kwargs)
        job.next_run = schedule.next()
        with self._lock:
            self._jobs.append(job)
        return job

    # -- job management -----------------------------------------------------

    def remove(self, job_or_name: Union[Job, str]) -> bool:
        """Remove a job by reference or name. Returns True if removed."""
        target = self._resolve(job_or_name)
        if target is None:
            return False
        target.cancel()
        with self._lock:
            try:
                self._jobs.remove(target)
            except ValueError:
                return False
        return True

    def cancel(self, job_or_name: Union[Job, str]) -> bool:
        """Cancel a job (alias for remove)."""
        return self.remove(job_or_name)

    def get_jobs(self, tag: Optional[str] = None) -> List[Job]:
        """Return a snapshot of jobs, optionally filtered by tag."""
        with self._lock:
            jobs = list(self._jobs)
        if tag is None:
            return jobs
        return [j for j in jobs if tag in j.tags]

    def next_run(self, job_name: Optional[str] = None) -> Optional[datetime]:
        """Return the next scheduled run. If job_name is given, that job's next_run;
        otherwise the soonest upcoming run across all jobs."""
        with self._lock:
            jobs = list(self._jobs)
        if not jobs:
            return None
        if job_name is not None:
            for j in jobs:
                if j.name == job_name:
                    return j.next_run
            return None
        upcoming = [j.next_run for j in jobs if j.next_run is not None]
        return min(upcoming) if upcoming else None

    def _resolve(self, job_or_name: Union[Job, str]) -> Optional[Job]:
        if isinstance(job_or_name, Job):
            return job_or_name
        with self._lock:
            for j in self._jobs:
                if j.name == job_or_name:
                    return j
        return None

    # -- lifecycle ----------------------------------------------------------

    def start(self, block: bool = False) -> None:
        """Start the scheduler. If block=True, run on the calling thread."""
        if self.running:
            return
        self._stop_event.clear()
        if block:
            self._run_loop()
            return
        self._thread = threading.Thread(target=self._run_loop, name="tiny-sched", daemon=True)
        self._thread.start()

    def stop(self, wait: bool = True) -> None:
        """Stop the scheduler. If wait=True, block until the loop exits."""
        self._stop_event.set()
        self._wake_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        if wait:
            self._executor.shutdown(wait=True, cancel_futures=False)

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def jobs(self) -> List[Job]:
        with self._lock:
            return list(self._jobs)

    def __enter__(self) -> "Scheduler":
        self.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.stop()

    # -- main loop ----------------------------------------------------------

    def _run_loop(self) -> None:
        """Background loop: tick once per second, dispatch due jobs."""
        log.debug("scheduler loop entered")
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:
                log.exception("scheduler tick failed")
            # Sleep up to 1s, but wake immediately on stop / explicit wake.
            self._wake_event.wait(timeout=1.0)
            self._wake_event.clear()
        log.debug("scheduler loop exited")

    def _tick(self) -> None:
        """Scan jobs, execute any due ones in the worker pool, advance next_run."""
        now = datetime.now(timezone.utc)
        with self._lock:
            snapshot = list(self._jobs)
        for job in snapshot:
            if job.cancelled:
                continue
            if job.next_run is None or job.next_run > now:
                continue
            # Submit to worker pool; don't block the loop.
            self._executor.submit(self._execute_job, job)

    def _execute_job(self, job: Job) -> None:
        """Run a single job, with jitter, error handling, and rescheduling."""
        if job.cancelled:
            return

        # Optional jitter to avoid thundering herd on heavy jobs.
        if job.jitter and job.jitter > 0:
            time.sleep(random.uniform(0, job.jitter))

        # Max-runs check: skip if already exhausted.
        if job.max_runs is not None and job.run_count >= job.max_runs:
            with self._lock:
                if job in self._jobs:
                    self._jobs.remove(job)
            return

        try:
            job.run()
        except Exception:
            log.exception("job %s raised", job.name)
            # A failed job still advances its schedule so we don't retry forever.
        finally:
            if job.one_shot:
                with self._lock:
                    if job in self._jobs:
                        self._jobs.remove(job)
            else:
                # Reschedule for the next firing.
                job.tick()
                if job.max_runs is not None and job.run_count >= job.max_runs:
                    with self._lock:
                        if job in self._jobs:
                            self._jobs.remove(job)


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------


def scheduled(
    expr: Optional[str] = None,
    *,
    interval: Optional[float] = None,
    name: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
    **kwargs: Any,
) -> Callable:
    """Mark a function for later registration with a Scheduler.

    The decorator stores configuration on the function; use `Scheduler.bind()`
    or attach the decorated function with `Scheduler.add()`.

    Two usage styles:
        @scheduled("*/5 * * * *")
        def my_func(): ...

        @scheduled(interval=60)
        def my_func(): ...
    """
    def decorator(func: Callable) -> Callable:
        if expr is not None:
            func.__tiny_schedule__ = {  # type: ignore[attr-defined]
                "kind": "cron",
                "value": expr,
                "name": name or func.__name__,
                "tags": tuple(tags or ()),
                "kwargs": kwargs,
            }
        elif interval is not None:
            func.__tiny_schedule__ = {  # type: ignore[attr-defined]
                "kind": "interval",
                "value": interval,
                "name": name or func.__name__,
                "tags": tuple(tags or ()),
                "kwargs": kwargs,
            }
        else:
            raise ValueError("@scheduled requires either `expr` or `interval`")
        return func
    return decorator