#!/usr/bin/env python3
"""Run every configured Klaviyo stream up to a fixed cutoff within one time budget.

Intended for one or two scheduled runs a day. The batch fixes its goal at
start time (now minus five minutes), then repeatedly runs bounded workers for
streams that are behind: short event cycles first, then one long profile
cycle per round. It stops when every stream reaches the goal, when the
budget runs out (a pause, not a failure), or after three consecutive
failures of one stream.

Workers keep their own transactional checkpoints and leases, so retries never
reset cursors or watermarks. After each observation the batch writes a small
JSON snapshot (klaviyo-batch-freshness.json) that monitoring can read; see
check_klaviyo_capacity.py.

Run it under a lock so two batches never overlap, for example:
  flock -n state/klaviyo-run.lock python3 scripts/klaviyo_batch.py
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
MAX_SECONDS = 10800
SNAPSHOT_NAME = "klaviyo-batch-freshness.json"
# One long profile cycle reuses a single JVM, Snowflake session and lease
# instead of restarting every few pages.
PROFILE_WORKER_SECONDS = 1200
PROFILE_WORKER_PAGES = 1000
EVENT_WORKER_SECONDS = 120
MIN_WORKER_SECONDS = 60
# Room for the final page-group commit and JVM shutdown after the budget.
WORKER_EXIT_GRACE_SECONDS = 180
BATCH_STOP_MARGIN_SECONDS = 300
CONTROL_TIMEOUT_SECONDS = 230
GOAL_LAG_SECONDS = 300
MAX_STREAM_FAILURES = 3
VALID_STATUSES = {"PENDING", "RUNNING", "INCOMPLETE", "CAUGHT_UP", "PROGRESS", "FAILED", "IDLE"}
METRIC_ID = re.compile(r"^[A-Za-z0-9]{1,32}$")


def job_keys_from(metrics_csv):
    """The configured job keys: profiles plus one event stream per metric ID."""
    keys = {"profiles"}
    if not (metrics_csv or "").strip():
        return frozenset(keys)
    for raw in metrics_csv.split(","):
        metric = raw.strip()
        if not METRIC_ID.match(metric):
            raise ValueError("Invalid metric ID in KLAVIYO_EVENT_METRICS")
        key = "event:" + metric
        if key in keys:
            raise ValueError("Duplicate metric ID in KLAVIYO_EVENT_METRICS")
        keys.add(key)
    return frozenset(keys)


def worker_budget(key, remaining_seconds):
    """Seconds one worker may use without running past the batch deadline."""
    wanted = PROFILE_WORKER_SECONDS if key == "profiles" else EVENT_WORKER_SECONDS
    budget = min(wanted, int(remaining_seconds) - WORKER_EXIT_GRACE_SECONDS)
    return budget if budget >= MIN_WORKER_SECONDS else None


def validate_state(state, now, job_keys):
    jobs = state["jobs"]
    if len(jobs) != len(job_keys) or {job["job_key"] for job in jobs} != set(job_keys):
        raise ValueError("State rows do not match the configured Klaviyo streams")
    if not 0 < int(state["target_cutoff_epoch"]) <= now + 60:
        raise ValueError("Invalid batch cutoff")
    for job in jobs:
        for key in ("watermark_epoch", "last_progress_epoch"):
            if not 0 < int(job[key]) <= now + 60:
                raise ValueError("Invalid stream timestamp")
        if job["status"] not in VALID_STATUSES:
            raise ValueError("Invalid stream status")
    return state


def write_snapshot(path, state, *, started, failures, outcome):
    payload = {**state, "observed_at_epoch": int(time.time()),
               "batch_started_epoch": started, "consecutive_failed_batches": failures,
               "outcome": outcome}
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


class Runtime:
    """Runs the Java entry points. Tests replace this with a fake."""

    def __init__(self, job_keys):
        self.job_keys = job_keys
        self.scripts = Path(os.environ.get("KLAVIYO_SCRIPT_ROOT", str(SCRIPT_DIR)))
        classpath = os.environ.get(
            "KLAVIYO_CLASSPATH",
            f"{PROJECT_ROOT / 'target' / 'classes'}:{PROJECT_ROOT / 'target' / 'lib'}/*",
        )
        self.java = [os.environ.get("KLAVIYO_JAVA_BIN", "java"), "-classpath", classpath,
                     "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
                     "com.greyfielddata.klaviyo.CohortHealth"]

    def command(self, args, *, env=None, timeout=CONTROL_TIMEOUT_SECONDS):
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, env=env)
        try:
            output, _ = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.communicate(timeout=60)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
            return 124, ""
        # Never echo JDBC diagnostics or source payloads. Worker checkpoints
        # and aggregate progress lines are enough for operational logs.
        for line in output.splitlines():
            if line.startswith("KLAVIYO_") and not line.startswith("KLAVIYO_BATCH_STATE "):
                print(line, flush=True)
        return process.returncode, output

    def state(self):
        code, output = self.command(self.java + ["--batch-status"])
        if code:
            raise RuntimeError(f"Could not read Klaviyo checkpoint state (exit {code})")
        lines = [line for line in output.splitlines() if line.startswith("KLAVIYO_BATCH_STATE ")]
        if len(lines) != 1:
            raise RuntimeError("Missing Klaviyo checkpoint response")
        return validate_state(json.loads(lines[0].split(" ", 1)[1]), int(time.time()),
                              self.job_keys)

    def worker(self, key, budget_seconds):
        env = {**os.environ, "KLAVIYO_MAX_SECONDS": str(int(budget_seconds))}
        env.pop("KLAVIYO_MAX_PAGES", None)
        if key == "profiles":
            env["KLAVIYO_MAX_PAGES"] = str(PROFILE_WORKER_PAGES)
        return self.command([str(self.scripts / "klaviyo-incremental.sh"), key], env=env,
                            timeout=int(budget_seconds) + WORKER_EXIT_GRACE_SECONDS)[0]

    def advance(self):
        code, _ = self.command(self.java)
        if code:
            raise RuntimeError(f"Could not advance completed Klaviyo batch (exit {code})")


def run_batch(runtime, snapshot, *, max_seconds=MAX_SECONDS, clock=time.time, sleep=time.sleep):
    started = int(clock())
    goal = started - GOAL_LAG_SECONDS
    deadline = started + max_seconds
    previous_failures = 0
    if snapshot.exists():
        previous = json.loads(snapshot.read_text(encoding="utf-8"))
        previous_failures = int(previous.get("consecutive_failed_batches", 0))
    state = None
    failures = {}
    try:
        state = runtime.state()
        while True:
            write_snapshot(snapshot, state, started=started, failures=previous_failures, outcome="running")
            if min(job["watermark_epoch"] for job in state["jobs"]) >= goal:
                if all(job["watermark_epoch"] >= state["target_cutoff_epoch"] for job in state["jobs"]):
                    runtime.advance()
                    state = runtime.state()
                write_snapshot(snapshot, state, started=started, failures=0, outcome="complete")
                print("KLAVIYO_BATCH_COMPLETE", flush=True)
                return 0
            # Short event cycles first, then the long profile cycle, so one
            # round never delays event streams by a full profile budget.
            pending = sorted((job for job in state["jobs"]
                              if job["watermark_epoch"] < state["target_cutoff_epoch"]),
                             key=lambda job: (job["job_key"] == "profiles", job["job_key"]))
            if not pending:
                runtime.advance()
                state = runtime.state()
                continue
            if worker_budget("profiles", deadline - BATCH_STOP_MARGIN_SECONDS - clock()) is None:
                write_snapshot(snapshot, state, started=started, failures=0, outcome="incomplete")
                print("KLAVIYO_BATCH_PAUSED reason=budget checkpoints_preserved=true", flush=True)
                return 0
            for job in pending:
                key = job["job_key"]
                budget = worker_budget(key, deadline - BATCH_STOP_MARGIN_SECONDS - clock())
                if budget is None:
                    break
                code = runtime.worker(key, budget)
                failures[key] = failures.get(key, 0) + 1 if code else 0
                if code:
                    print(f"KLAVIYO_BATCH_RETRY job={key} exit={code} attempt={failures[key]}", flush=True)
                    if failures[key] >= MAX_STREAM_FAILURES:
                        raise RuntimeError(f"{key} failed {MAX_STREAM_FAILURES} attempts (exit {code})")
                    sleep(60)
            state = runtime.state()
    except Exception as exc:
        # Re-read state for the failure snapshot. If that is impossible, keep
        # the previous snapshot: a stale snapshot must never look freshly verified.
        try:
            state = runtime.state()
        except Exception:
            state = None
        if state is not None:
            write_snapshot(snapshot, state, started=started,
                           failures=previous_failures + 1, outcome="failed")
        print(f"KLAVIYO_BATCH_FAILED {type(exc).__name__}: {exc}", flush=True)
        return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--observe-only", action="store_true",
                        help="refresh the snapshot from current state without running workers")
    args = parser.parse_args()
    job_keys = job_keys_from(os.environ.get("KLAVIYO_EVENT_METRICS", ""))
    directory = Path(os.environ.get("KLAVIYO_STATUS_DIR", str(PROJECT_ROOT / "state")))
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    snapshot = directory / SNAPSHOT_NAME
    runtime = Runtime(job_keys)
    if args.observe_only:
        previous = json.loads(snapshot.read_text()) if snapshot.exists() else {}
        write_snapshot(snapshot, runtime.state(), started=previous.get("batch_started_epoch", 0),
                       failures=previous.get("consecutive_failed_batches", 0),
                       outcome=previous.get("outcome", "incomplete"))
        print(f"KLAVIYO_BATCH_OBSERVED streams={len(job_keys)}")
        return 0
    return run_batch(runtime, snapshot)


if __name__ == "__main__":
    raise SystemExit(main())
