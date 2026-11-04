import datetime as dt
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from typing import Optional


from helpers import METRICS, METRICS_CSV, SCRIPTS

SCRIPT = SCRIPTS / "klaviyo-catchup.sh"
ROTATION = [key for metric in METRICS for key in ("profiles", f"event:{metric}")]


class KlaviyoCatchupTests(unittest.TestCase):
    def run_catchup(
        self,
        *,
        cycle_body: str = "#!/usr/bin/env bash\necho KLAVIYO_COHORT_PROGRESS\n",
        timeout_body: Optional[str] = None,
        flock_body: str = "#!/usr/bin/env bash\nexit 0\n",
        date_body: Optional[str] = None,
        **overrides: str,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = root / "calls"
            cycle = root / "klaviyo-sync-cycle.sh"
            cycle.write_text(cycle_body, encoding="utf-8")
            cycle.chmod(0o755)

            timeout_stub = root / "timeout"
            timeout_stub.write_text(
                timeout_body
                or (
                    "#!/usr/bin/env bash\n"
                    "printf '%s|%s|%s|%s\\n' "
                    '"$KLAVIYO_JOB_KEY" "$1" "$2" "$3" '
                    '>> "$KLAVIYO_TEST_CALLS"\n'
                    'exec "$3"\n'
                ),
                encoding="utf-8",
            )
            timeout_stub.chmod(0o755)

            flock = root / "flock"
            flock.write_text(flock_body, encoding="utf-8")
            flock.chmod(0o755)

            date_bin = root / "date"
            if date_body is not None:
                date_bin.write_text(date_body, encoding="utf-8")
                date_bin.chmod(0o755)

            status_dir = root / "status"
            env = {
                **os.environ,
                "KLAVIYO_CYCLE_SCRIPT": str(cycle),
                "KLAVIYO_TIMEOUT_BIN": str(timeout_stub),
                "KLAVIYO_EVENT_METRICS": METRICS_CSV,
                "KLAVIYO_RUN_LOCK_FILE": str(root / "run.lock"),
                "KLAVIYO_STATUS_DIR": str(status_dir),
                "KLAVIYO_FLOCK_BIN": str(flock),
                "KLAVIYO_CATCHUP_SLEEP_SECONDS": "0",
                "KLAVIYO_CATCHUP_MAX_SECONDS": "600",
                "KLAVIYO_CATCHUP_MAX_ITERATIONS": "4",
                "KLAVIYO_TEST_CALLS": str(calls),
                **overrides,
            }
            if date_body is not None:
                env["KLAVIYO_DATE_BIN"] = str(date_bin)
            result = subprocess.run(
                ["bash", str(SCRIPT)],
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )
            recorded = (
                calls.read_text(encoding="utf-8").splitlines()
                if calls.exists()
                else []
            )
            latch = status_dir / "klaviyo-catchup-failure.json"
            latch_text = (
                latch.read_text(encoding="utf-8") if latch.exists() else None
            )
            return result, recorded, latch_text

    def test_runs_fair_order_with_bounded_slices(self):
        result, calls, latch_text = self.run_catchup()

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(
            [
                "profiles|--kill-after=30|230|"
                + calls[0].split("|", 3)[3],
                "event:MetricA1|--kill-after=30|230|"
                + calls[1].split("|", 3)[3],
                "profiles|--kill-after=30|230|"
                + calls[2].split("|", 3)[3],
                "event:MetricB2|--kill-after=30|230|"
                + calls[3].split("|", 3)[3],
            ],
            calls,
        )
        self.assertTrue(
            all(call.endswith("/klaviyo-sync-cycle.sh") for call in calls)
        )
        self.assertIn("KLAVIYO_CATCHUP_INCOMPLETE", result.stdout)
        self.assertIsNotNone(latch_text)
        self.assertIn('"reason":"budget_reached"', latch_text)

    def test_stops_after_completing_a_fresh_cohort(self):
        cutoff = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        result, calls, _ = self.run_catchup(
            cycle_body=(
                "#!/usr/bin/env bash\n"
                f"echo 'KLAVIYO_COHORT_COMPLETE cutoff={cutoff}'\n"
            ),
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(1, len(calls))
        self.assertIn("KLAVIYO_CATCHUP_COMPLETE", result.stdout)

    def test_defers_health_until_every_stream_gets_a_recovery_slice(self):
        cutoff = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        result, calls, _ = self.run_catchup(
            timeout_body=(
                "#!/usr/bin/env bash\n"
                "printf '%s|%s|%s|%s|%s\\n' "
                '"$KLAVIYO_JOB_KEY" "$1" "$2" "$3" '
                '"$KLAVIYO_DEFER_COHORT_HEALTH" '
                '>> "$KLAVIYO_TEST_CALLS"\n'
                'exec "$3"\n'
            ),
            cycle_body=(
                "#!/usr/bin/env bash\n"
                'if [ "$KLAVIYO_DEFER_COHORT_HEALTH" = true ]; then\n'
                "  echo KLAVIYO_COHORT_PROGRESS\n"
                "else\n"
                f"  echo 'KLAVIYO_COHORT_COMPLETE cutoff={cutoff}'\n"
                "fi\n"
            ),
            KLAVIYO_CATCHUP_MAX_ITERATIONS=str(len(ROTATION)),
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(len(ROTATION), len(calls))
        flags = [call.rsplit("|", 1)[1] for call in calls]
        self.assertEqual(["true"] * (len(ROTATION) - 1) + ["false"], flags)
        self.assertEqual(ROTATION, [call.split("|", 1)[0] for call in calls])

    def test_old_cohort_completion_continues_until_fresh_cohort_completes(self):
        cutoff = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        result, calls, _ = self.run_catchup(
            cycle_body=(
                "#!/usr/bin/env bash\n"
                'counter_file="$KLAVIYO_TEST_CALLS.counter"\n'
                'counter="$(cat "$counter_file" 2>/dev/null || echo 0)"\n'
                'counter=$((counter + 1))\n'
                'printf "%s\\n" "$counter" > "$counter_file"\n'
                'if [ "$counter" -eq 1 ]; then\n'
                "  echo 'KLAVIYO_COHORT_COMPLETE "
                "cutoff=2020-01-01T00:00:00Z'\n"
                "else\n"
                f"  echo 'KLAVIYO_COHORT_COMPLETE cutoff={cutoff}'\n"
                "fi\n"
            ),
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(2, len(calls))
        self.assertIn("continuing_to_fresh_cutoff", result.stdout)

    def test_freshness_is_recomputed_against_wall_clock_at_completion(self):
        started_epoch = 100_000
        stale_completed_epoch = 100_100
        current_epoch = 108_000
        stale_cutoff = dt.datetime.fromtimestamp(
            stale_completed_epoch, tz=dt.timezone.utc
        ).isoformat().replace("+00:00", "Z")
        fresh_cutoff = dt.datetime.fromtimestamp(
            current_epoch, tz=dt.timezone.utc
        ).isoformat().replace("+00:00", "Z")
        result, calls, _ = self.run_catchup(
            date_body=(
                "#!/usr/bin/env bash\n"
                'counter_file="$KLAVIYO_TEST_CALLS.date-counter"\n'
                'counter="$(cat "$counter_file" 2>/dev/null || echo 0)"\n'
                'counter=$((counter + 1))\n'
                'printf "%s\\n" "$counter" > "$counter_file"\n'
                f'if [ "$counter" -eq 1 ]; then echo {started_epoch}; '
                f"else echo {current_epoch}; fi\n"
            ),
            cycle_body=(
                "#!/usr/bin/env bash\n"
                'counter_file="$KLAVIYO_TEST_CALLS.cycle-counter"\n'
                'counter="$(cat "$counter_file" 2>/dev/null || echo 0)"\n'
                'counter=$((counter + 1))\n'
                'printf "%s\\n" "$counter" > "$counter_file"\n'
                'if [ "$counter" -eq 1 ]; then\n'
                f"  echo 'KLAVIYO_COHORT_COMPLETE cutoff={stale_cutoff}'\n"
                "else\n"
                f"  echo 'KLAVIYO_COHORT_COMPLETE cutoff={fresh_cutoff}'\n"
                "fi\n"
            ),
            KLAVIYO_CATCHUP_MAX_SECONDS="86400",
            KLAVIYO_CATCHUP_MAX_COMPLETED_CUTOFF_AGE_SECONDS="3600",
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(2, len(calls))
        self.assertIn("continuing_to_fresh_cutoff", result.stdout)

    def test_slice_failure_fails_closed(self):
        result, calls, latch_text = self.run_catchup(
            timeout_body=(
                "#!/usr/bin/env bash\n"
                "printf '%s|%s|%s|%s\\n' "
                '"$KLAVIYO_JOB_KEY" "$1" "$2" "$3" '
                '>> "$KLAVIYO_TEST_CALLS"\n'
                "exit 23\n"
            ),
        )

        self.assertEqual(23, result.returncode)
        self.assertEqual(1, len(calls))
        self.assertIn("KLAVIYO_CATCHUP_FAIL", result.stdout)
        self.assertIsNotNone(latch_text)
        self.assertIn('"status":"failed"', latch_text)

    def test_lock_contention_prevents_any_slice(self):
        result, calls, latch_text = self.run_catchup(
            flock_body="#!/usr/bin/env bash\nexit 1\n",
        )

        self.assertEqual(75, result.returncode)
        self.assertEqual([], calls)
        self.assertIn("KLAVIYO_CATCHUP_BUSY", result.stdout)
        self.assertIsNone(latch_text)

    def test_rejects_invalid_runtime_configuration(self):
        result, calls, _ = self.run_catchup(
            KLAVIYO_CATCHUP_MAX_SECONDS="not-a-number",
        )

        self.assertEqual(2, result.returncode)
        self.assertEqual([], calls)
        self.assertIn("invalid KLAVIYO_CATCHUP_MAX_SECONDS", result.stderr)


if __name__ == "__main__":
    unittest.main()
