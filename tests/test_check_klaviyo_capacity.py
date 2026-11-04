import json
import tempfile
import unittest
from pathlib import Path

from helpers import METRICS, load_script

MODULE = load_script("check_klaviyo_capacity", "check_klaviyo_capacity.py")

HOUR = 3600
BATCH = 12 * HOUR
START = 1_790_000_000


def snapshot(observed, profiles_lag_hours, outcome="complete"):
    jobs = [{"job_key": "profiles", "watermark_epoch": observed - int(profiles_lag_hours * HOUR)}]
    jobs += [{"job_key": f"event:{key}", "watermark_epoch": observed - HOUR}
             for key in METRICS]
    return {"observed_at_epoch": observed, "outcome": outcome, "consecutive_failed_batches": 0, "jobs": jobs}


def history(*lags_hours):
    return [{"observed_at_epoch": START + index * BATCH, "lag_seconds": int(lag * HOUR)}
            for index, lag in enumerate(lags_hours)]


class SampleTests(unittest.TestCase):
    def test_sample_is_profiles_lag_at_observation(self):
        self.assertEqual({"observed_at_epoch": START, "lag_seconds": 10 * HOUR},
                         MODULE.profiles_sample(snapshot(START, 10)))

    def test_running_or_invalid_snapshots_are_not_sampled(self):
        self.assertIsNone(MODULE.profiles_sample(snapshot(START, 10, outcome="running")))
        self.assertIsNone(MODULE.profiles_sample({"jobs": []}))
        self.assertIsNone(MODULE.profiles_sample(None))
        bad = snapshot(START, 10)
        bad["jobs"][0]["watermark_epoch"] = START + 10 * HOUR  # ahead of observation
        self.assertIsNone(MODULE.profiles_sample(bad))

    def test_history_records_each_batch_once_and_stays_small(self):
        samples = history(1, 2)
        same = MODULE.update_history(samples, samples[-1])
        self.assertEqual(samples, same)
        grown = samples
        for index in range(2, 12):
            grown = MODULE.update_history(grown, {"observed_at_epoch": START + index * BATCH, "lag_seconds": HOUR})
        self.assertEqual(MODULE.MAX_SAMPLES, len(grown))
        self.assertEqual(START + 11 * BATCH, grown[-1]["observed_at_epoch"])
        older = MODULE.update_history(samples, {"observed_at_epoch": START - BATCH, "lag_seconds": HOUR})
        self.assertEqual(samples, older)


class TrendTests(unittest.TestCase):
    def test_growing_lag_warns_before_the_36_hour_limit(self):
        result = MODULE.evaluate(history(8, 14, 20))
        self.assertFalse(result["ok"])
        issue = result["issues"][0]
        self.assertEqual("klaviyo-profiles", issue["label"])
        self.assertEqual("stale_data", issue["status"])
        self.assertEqual("klaviyo_profiles_falling_behind", issue["reason_code"])
        self.assertIn("8h -> 14h -> 20h", issue["reason"])
        self.assertIn("36-hour", issue["reason"])

    def test_catching_up_or_flat_lag_is_healthy(self):
        for lags in [(20, 14, 8), (8, 14, 12), (8, 8, 8), (8, 8.5, 9)]:
            with self.subTest(lags=lags):
                self.assertTrue(MODULE.evaluate(history(*lags))["ok"])

    def test_small_lag_or_already_breached_limit_does_not_warn(self):
        self.assertTrue(MODULE.evaluate(history(1, 2.5, 4))["ok"])
        # Beyond the limit, plain staleness alerting takes over.
        self.assertTrue(MODULE.evaluate(history(26, 32, 38))["ok"])

    def test_not_enough_history_is_healthy(self):
        self.assertTrue(MODULE.evaluate(history(8, 20))["ok"])
        self.assertTrue(MODULE.evaluate([])["ok"])

    def test_thresholds_are_configurable(self):
        self.assertTrue(MODULE.evaluate(history(8, 14, 20), min_lag_seconds=24 * HOUR)["ok"])
        self.assertTrue(MODULE.evaluate(history(8, 14, 20), limit_seconds=18 * HOUR)["ok"])
        limited = MODULE.evaluate(history(8, 14, 20), limit_seconds=48 * HOUR)
        self.assertIn("48-hour", limited["issues"][0]["reason"])
        self.assertFalse(MODULE.evaluate(history(8, 14), consecutive_growth=1)["ok"])


class CliTests(unittest.TestCase):
    def run_cli(self, directory, freshness, prior=None):
        root = Path(directory)
        freshness_path = root / "klaviyo-batch-freshness.json"
        history_path = root / "klaviyo-lag-history.json"
        if freshness is not None:
            freshness_path.write_text(json.dumps(freshness))
        if prior is not None:
            history_path.write_text(json.dumps({"samples": prior}))
        result, code = MODULE.run(freshness_path, history_path, MODULE.parse_args([
            "--freshness", str(freshness_path), "--history", str(history_path)]))
        stored = json.loads(history_path.read_text()) if history_path.exists() else None
        return result, code, stored

    def test_appends_sample_and_warns(self):
        with tempfile.TemporaryDirectory() as directory:
            result, code, stored = self.run_cli(directory, snapshot(START + 2 * BATCH, 20), history(8, 14))
        self.assertEqual(1, code)
        self.assertFalse(result["ok"])
        self.assertEqual(3, len(stored["samples"]))

    def test_missing_snapshot_is_skipped_quietly(self):
        with tempfile.TemporaryDirectory() as directory:
            result, code, stored = self.run_cli(directory, None, history(8, 14))
        self.assertEqual(0, code)
        self.assertTrue(result["ok"])
        self.assertIn("skipped", result)

    def test_running_snapshot_cannot_prove_recovery_from_old_healthy_history(self):
        with tempfile.TemporaryDirectory() as directory:
            result, code, stored = self.run_cli(directory, snapshot(START + 3 * BATCH, 2, outcome="running"), history(8, 7, 6))
        self.assertEqual(0, code)
        self.assertIn("skipped", result)
        self.assertEqual(3, len(stored["samples"]))

    def test_insufficient_history_is_explicitly_inconclusive(self):
        with tempfile.TemporaryDirectory() as directory:
            result, code, stored = self.run_cli(directory, snapshot(START, 5))
        self.assertEqual(0, code)
        self.assertIn("Insufficient", result["skipped"])

    def test_corrupt_history_starts_over(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "klaviyo-lag-history.json").write_text("{not json")
            result, code, stored = self.run_cli(directory, snapshot(START, 5))
        self.assertEqual(0, code)
        self.assertEqual(1, len(stored["samples"]))


if __name__ == "__main__":
    unittest.main()
