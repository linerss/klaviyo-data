import json
from pathlib import Path
import tempfile
import unittest

from helpers import METRICS_CSV, load_script

BATCH = load_script("klaviyo_batch", "klaviyo_batch.py")
JOB_KEYS = BATCH.job_keys_from(METRICS_CSV)
NOW = 200_000


class FakeRuntime:
    def __init__(self, failures=0, pending=None):
        self.failures = failures
        self.calls = []
        self.cutoff = NOW - 300
        self.watermarks = {key: self.cutoff for key in JOB_KEYS}
        for key in (pending or ["profiles"]):
            self.watermarks[key] = NOW - 7200
        self.advanced = 0
        self.budgets = []

    def state(self):
        return {"target_cutoff_epoch": self.cutoff, "jobs": [
            {"job_key": key, "watermark_epoch": value,
             "last_progress_epoch": NOW - 10,
             "status": "CAUGHT_UP" if value >= self.cutoff else "INCOMPLETE"}
            for key, value in self.watermarks.items()]}

    def worker(self, key, budget_seconds=None):
        self.calls.append(key)
        self.budgets.append((key, budget_seconds))
        if self.failures:
            self.failures -= 1
            return 1
        self.watermarks[key] = self.cutoff
        return 0

    def advance(self):
        self.advanced += 1
        self.cutoff += 300


class BatchTests(unittest.TestCase):
    def execute(self, runtime, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / BATCH.SNAPSHOT_NAME
            code = BATCH.run_batch(runtime, path, clock=lambda: NOW,
                                   sleep=lambda _: None, **kwargs)
            return code, json.loads(path.read_text())

    def test_skip_caught_up_streams_and_stop_at_fixed_goal(self):
        runtime = FakeRuntime()
        code, snapshot = self.execute(runtime)
        self.assertEqual(0, code)
        self.assertEqual(["profiles"], runtime.calls)
        self.assertEqual(1, runtime.advanced)
        self.assertEqual("complete", snapshot["outcome"])

    def test_transient_worker_error_retries_without_failed_batch(self):
        runtime = FakeRuntime(failures=1)
        code, snapshot = self.execute(runtime)
        self.assertEqual(0, code)
        self.assertEqual(["profiles", "profiles"], runtime.calls)
        self.assertEqual(0, snapshot["consecutive_failed_batches"])

    def test_three_worker_failures_fail_batch_after_exactly_three_attempts(self):
        runtime = FakeRuntime(failures=3)
        code, snapshot = self.execute(runtime)
        self.assertEqual(1, code)
        self.assertEqual(["profiles"] * 3, runtime.calls)
        self.assertEqual("failed", snapshot["outcome"])
        self.assertEqual(1, snapshot["consecutive_failed_batches"])

    def test_completed_window_progress_resumes_until_batch_cutoff(self):
        # The Java worker writes PROGRESS after every completed window,
        # including the final window. It does not write CAUGHT_UP at that point.
        class WindowRuntime(FakeRuntime):
            def __init__(self):
                super().__init__()
                self.watermarks["profiles"] = self.cutoff - 2 * 86400

            def state(self):
                state = super().state()
                for job in state["jobs"]:
                    if job["job_key"] == "profiles" and self.calls:
                        job["status"] = "PROGRESS"
                return BATCH.validate_state(state, NOW, JOB_KEYS)

            def worker(self, key, budget_seconds=None):
                self.calls.append(key)
                self.watermarks[key] = min(self.cutoff, self.watermarks[key] + 86400)
                return 0

        runtime = WindowRuntime()
        code, snapshot = self.execute(runtime)
        self.assertEqual(0, code)
        self.assertEqual(["profiles", "profiles"], runtime.calls)
        self.assertEqual(1, runtime.advanced)
        self.assertEqual("complete", snapshot["outcome"])
        self.assertEqual(0, snapshot["consecutive_failed_batches"])

    def test_profiles_get_long_worker_cycles(self):
        runtime = FakeRuntime(pending=JOB_KEYS)
        self.execute(runtime)
        budgets = dict(runtime.budgets)
        self.assertEqual(BATCH.PROFILE_WORKER_SECONDS, budgets["profiles"])
        self.assertGreaterEqual(BATCH.PROFILE_WORKER_SECONDS, 600)
        self.assertLessEqual(BATCH.PROFILE_WORKER_SECONDS, 1800)
        for key in JOB_KEYS - {"profiles"}:
            self.assertEqual(BATCH.EVENT_WORKER_SECONDS, budgets[key])

    def test_worker_budget_never_runs_past_the_batch_deadline(self):
        remaining = 1000
        budget = BATCH.worker_budget("profiles", remaining)
        self.assertLessEqual(budget + BATCH.WORKER_EXIT_GRACE_SECONDS, remaining)
        self.assertIsNone(BATCH.worker_budget("profiles", BATCH.WORKER_EXIT_GRACE_SECONDS + 30))
        runtime = FakeRuntime()
        code, snapshot = self.execute(runtime, max_seconds=300 + remaining)
        self.assertEqual(0, code)
        self.assertEqual([("profiles", budget)], runtime.budgets)

    def test_events_run_before_the_long_profile_cycle_each_round(self):
        runtime = FakeRuntime(pending=JOB_KEYS)
        code, _ = self.execute(runtime)
        self.assertEqual(0, code)
        self.assertEqual("profiles", runtime.calls[-1])
        self.assertEqual(JOB_KEYS - {"profiles"}, set(runtime.calls[:-1]))

    def test_too_little_time_for_a_worker_pauses_instead_of_polling(self):
        # Budget exhaustion is an expected pause (exit 0), not a failure,
        # from zero remaining time up to one second short of a worker slot.
        for remaining in (0, BATCH.WORKER_EXIT_GRACE_SECONDS + BATCH.MIN_WORKER_SECONDS - 1):
            with self.subTest(remaining=remaining):
                runtime = FakeRuntime()
                code, snapshot = self.execute(runtime, max_seconds=300 + remaining)
                self.assertEqual(0, code)
                self.assertEqual([], runtime.calls)
                self.assertEqual("incomplete", snapshot["outcome"])

    def test_runtime_passes_budget_and_page_cap_to_profile_worker(self):
        runtime = BATCH.Runtime(JOB_KEYS)
        seen = {}

        def fake_command(args, *, env=None, timeout=None):
            seen.update(args=args, env=env, timeout=timeout)
            return 0, ""

        runtime.command = fake_command
        self.assertEqual(0, runtime.worker("profiles", 1200))
        self.assertEqual("1200", seen["env"]["KLAVIYO_MAX_SECONDS"])
        self.assertEqual(str(BATCH.PROFILE_WORKER_PAGES), seen["env"]["KLAVIYO_MAX_PAGES"])
        self.assertEqual(1200 + BATCH.WORKER_EXIT_GRACE_SECONDS, seen["timeout"])
        self.assertTrue(seen["args"][0].endswith("klaviyo-incremental.sh"))

        runtime.worker("event:MetricA1", BATCH.EVENT_WORKER_SECONDS)
        self.assertEqual(str(BATCH.EVENT_WORKER_SECONDS), seen["env"]["KLAVIYO_MAX_SECONDS"])
        self.assertNotIn("KLAVIYO_MAX_PAGES", seen["env"])

    def test_missing_duplicate_or_future_stream_state_rejected(self):
        state = FakeRuntime().state()
        state["jobs"].append(state["jobs"][0])
        with self.assertRaises(ValueError):
            BATCH.validate_state(state, NOW, JOB_KEYS)
        state = FakeRuntime().state()
        state["jobs"].pop()
        with self.assertRaises(ValueError):
            BATCH.validate_state(state, NOW, JOB_KEYS)
        state = FakeRuntime().state()
        state["jobs"][0]["watermark_epoch"] = NOW + 100
        with self.assertRaises(ValueError):
            BATCH.validate_state(state, NOW, JOB_KEYS)




class JobKeyTests(unittest.TestCase):
    def test_job_keys_follow_configuration(self):
        self.assertEqual({"profiles"}, BATCH.job_keys_from(""))
        self.assertEqual({"profiles", "event:MetricA1"}, BATCH.job_keys_from(" MetricA1 "))
        for bad in ("MetricA1,MetricA1", "bad id", "MetricA1,", "x'),or(1"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                BATCH.job_keys_from(bad)


if __name__ == "__main__":
    unittest.main()
