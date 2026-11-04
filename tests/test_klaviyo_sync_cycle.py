import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from helpers import METRICS, METRICS_CSV, SCRIPTS

SCRIPT = SCRIPTS / "klaviyo-sync-cycle.sh"


class KlaviyoSyncCycleTests(unittest.TestCase):
    def run_cycle(self, **overrides):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # Copy the cycle next to a fake worker so script_root points here.
            (root / "lib.sh").write_text((SCRIPTS / "lib.sh").read_text(), encoding="utf-8")
            cycle = root / "klaviyo-sync-cycle.sh"
            cycle.write_text(SCRIPT.read_text(), encoding="utf-8")
            cycle.chmod(0o755)
            calls = root / "calls"
            worker = root / "klaviyo-incremental.sh"
            worker.write_text(
                "#!/usr/bin/env bash\n"
                "printf 'worker:%s\\n' \"$1\" >> \"$KLAVIYO_TEST_CALLS\"\n"
                "exit \"${KLAVIYO_TEST_WORKER_EXIT:-0}\"\n",
                encoding="utf-8",
            )
            worker.chmod(0o755)
            java = root / "java"
            java.write_text(
                "#!/usr/bin/env bash\n"
                "printf 'health:%s\\n' \"$*\" >> \"$KLAVIYO_TEST_CALLS\"\n"
                "exit \"${KLAVIYO_TEST_HEALTH_EXIT:-0}\"\n",
                encoding="utf-8",
            )
            java.chmod(0o755)
            flock = root / "flock"
            flock.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            flock.chmod(0o755)
            env = {
                **os.environ,
                "PATH": f"{root}:{os.environ.get('PATH', '')}",
                "KLAVIYO_JAVA_BIN": str(java),
                "KLAVIYO_CLASSPATH": "/nonexistent/test-classpath",
                "KLAVIYO_CYCLE_LOCK_FILE": str(root / "lock"),
                "KLAVIYO_EVENT_METRICS": METRICS_CSV,
                "KLAVIYO_CYCLE_SLOT": "0",
                "KLAVIYO_TEST_CALLS": str(calls),
                **overrides,
            }
            if "KLAVIYO_JOB_KEY" not in overrides:
                env.pop("KLAVIYO_JOB_KEY", None)
            result = subprocess.run(
                ["bash", str(cycle)],
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
            return result, recorded

    def test_rotation_covers_every_stream_and_interleaves_profiles(self):
        expected = []
        for metric in METRICS:
            expected += ["profiles", f"event:{metric}"]
        seen = []
        for slot in range(len(expected) * 2):
            result, calls = self.run_cycle(KLAVIYO_CYCLE_SLOT=str(slot))
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertIn("CohortHealth", calls[1])
            seen.append(calls[0].split(":", 1)[1])
        self.assertEqual(expected * 2, seen)

    def test_profiles_only_configuration_runs_profiles(self):
        result, calls = self.run_cycle(KLAVIYO_EVENT_METRICS="", KLAVIYO_CYCLE_SLOT="7")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("worker:profiles", calls[0])

    def test_job_key_override_is_passed_to_worker(self):
        result, calls = self.run_cycle(KLAVIYO_JOB_KEY="event:MetricA1")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("worker:event:MetricA1", calls[0])

    def test_invalid_slot_or_metrics_fail_closed(self):
        for overrides in ({"KLAVIYO_CYCLE_SLOT": "abc"},
                          {"KLAVIYO_EVENT_METRICS": "bad id"}):
            with self.subTest(**overrides):
                result, calls = self.run_cycle(**overrides)
                self.assertEqual(2, result.returncode)
                self.assertEqual([], calls)

    def test_catchup_can_defer_aggregate_health_until_round_end(self):
        result, calls = self.run_cycle(
            KLAVIYO_DEFER_COHORT_HEALTH="true",
            KLAVIYO_TEST_HEALTH_EXIT="1",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(["worker:profiles"], calls)
        self.assertIn("KLAVIYO_CYCLE_HEALTH_DEFERRED", result.stdout)

    def test_worker_or_aggregate_failure_fails_parent(self):
        worker_result, _ = self.run_cycle(KLAVIYO_TEST_WORKER_EXIT="1")
        health_result, _ = self.run_cycle(KLAVIYO_TEST_HEALTH_EXIT="1")
        self.assertEqual(1, worker_result.returncode)
        self.assertEqual(1, health_result.returncode)


if __name__ == "__main__":
    unittest.main()
