#!/usr/bin/env python3
"""Early warning when the Klaviyo profile sync is falling behind.

Reads the batch snapshot written by klaviyo_batch.py
(klaviyo-batch-freshness.json) and a small history file of past batches.
Each finished batch contributes one sample: how far the profiles watermark
trailed the batch's observation time. When that lag grows across consecutive
batches, profiles are changing faster than the sync processes them, and the
lag will eventually cross the freshness limit. This warns before that.

Output: JSON {ok, issues: [{label, status, reason, reason_code, lag_seconds}]}.
Exit code 0 when healthy or inconclusive, 1 when a warning is raised.
A missing or unreadable snapshot is reported as skipped, not as a failure;
alert on snapshot staleness separately.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


HOUR = 3600
MAX_SAMPLES = 6  # for example three days of twice-daily batches
LIMIT_SECONDS = 36 * HOUR  # default freshness limit; see --limit-hours
LABEL = "klaviyo-profiles"


def profiles_sample(snapshot) -> dict | None:
    try:
        if snapshot["outcome"] == "running":
            return None
        observed = int(snapshot["observed_at_epoch"])
        profiles = [job for job in snapshot["jobs"] if job["job_key"] == "profiles"]
        watermark = int(profiles[0]["watermark_epoch"])
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    # Tolerate clock skew: a watermark may lead the observation by up to a minute.
    if observed <= 0 or not 0 < watermark <= observed + 60:
        return None
    return {"observed_at_epoch": observed, "lag_seconds": max(0, observed - watermark)}


def update_history(samples: list[dict], sample: dict | None) -> list[dict]:
    if sample is None or (samples and sample["observed_at_epoch"] <= samples[-1]["observed_at_epoch"]):
        return list(samples)
    return [*samples, sample][-MAX_SAMPLES:]


def hours(seconds: float) -> str:
    return f"{seconds / HOUR:.0f}h"


def evaluate(samples: list[dict], *, consecutive_growth: int = 2, min_growth_seconds: int = HOUR,
             min_lag_seconds: int = 6 * HOUR, limit_seconds: int = LIMIT_SECONDS) -> dict:
    window = samples[-(consecutive_growth + 1):]
    result = {"ok": True, "issues": [], "samples": len(samples)}
    if len(window) < consecutive_growth + 1:
        return result
    lags = [row["lag_seconds"] for row in window]
    growing = all(later - earlier >= min_growth_seconds for earlier, later in zip(lags, lags[1:]))
    latest = lags[-1]
    if not growing or latest < min_lag_seconds or latest >= limit_seconds:
        return result
    elapsed = window[-1]["observed_at_epoch"] - window[0]["observed_at_epoch"]
    rate = (latest - lags[0]) / elapsed if elapsed > 0 else 0
    limit = hours(limit_seconds)
    eta = f"; at this pace it reaches the {limit} limit in about {hours((limit_seconds - latest) / rate)}" if rate > 0 else ""
    result["ok"] = False
    result["issues"].append({
        "label": LABEL,
        "status": "stale_data",
        "reason_code": "klaviyo_profiles_falling_behind",
        "reason": (
            f"Klaviyo profiles lag grew across {len(window)} batches "
            f"({' -> '.join(hours(lag) for lag in lags)}); the sync is falling behind the "
            f"{limit_seconds // HOUR}-hour freshness limit{eta}"
        ),
        "lag_seconds": latest,
    })
    return result


def load_history(path: Path) -> list[dict]:
    try:
        samples = json.loads(path.read_text(encoding="utf-8"))["samples"]
        return [
            {"observed_at_epoch": int(row["observed_at_epoch"]), "lag_seconds": int(row["lag_seconds"])}
            for row in samples
        ][-MAX_SAMPLES:]
    except (OSError, ValueError, KeyError, TypeError):
        return []


def run(freshness_path: Path, history_path: Path, args: argparse.Namespace) -> tuple[dict, int]:
    try:
        snapshot = json.loads(freshness_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        snapshot = None
    history = load_history(history_path)
    sample = profiles_sample(snapshot)
    updated = update_history(history, sample)
    if updated != history or not history_path.exists():
        if updated:
            history_path.write_text(json.dumps({"samples": updated}, indent=2), encoding="utf-8")
    result = evaluate(
        updated,
        consecutive_growth=args.consecutive_growth,
        min_growth_seconds=int(args.min_growth_hours * HOUR),
        min_lag_seconds=int(args.min_lag_hours * HOUR),
        limit_seconds=int(args.limit_hours * HOUR),
    )
    if sample is None:
        result["skipped"] = "Klaviyo batch freshness snapshot unavailable, invalid, or still running"
    elif len(updated) < args.consecutive_growth + 1:
        result["skipped"] = "Insufficient completed-batch history to verify capacity trend"
    return result, 0 if result["ok"] else 1


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--freshness", type=Path, default=Path("klaviyo-batch-freshness.json"))
    parser.add_argument("--history", type=Path, default=Path("klaviyo-lag-history.json"))
    parser.add_argument("--consecutive-growth", type=int, default=2)
    parser.add_argument("--min-growth-hours", type=float, default=1.0)
    parser.add_argument("--min-lag-hours", type=float, default=6.0)
    parser.add_argument("--limit-hours", type=float, default=LIMIT_SECONDS / HOUR)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    result, code = run(args.freshness, args.history, args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
