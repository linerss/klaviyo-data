import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from helpers import METRICS_CSV, SCRIPTS

SCRIPT = SCRIPTS / "klaviyo-incremental.sh"


class KlaviyoIncrementalScriptTests(unittest.TestCase):
    def run_script(self, *args, metrics=METRICS_CSV):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            calls_path = tmp_path / "calls.log"
            java_stub = tmp_path / "java-stub"
            java_stub.write_text(
                "#!/usr/bin/env bash\n"
                "printf '%s\\n' \"$*\" >> \"$KLAVIYO_TEST_CALLS\"\n"
                "exit 0\n",
                encoding="utf-8",
            )
            java_stub.chmod(0o755)
            env = {
                **os.environ,
                "KLAVIYO_JAVA_BIN": str(java_stub),
                "KLAVIYO_CLASSPATH": "/nonexistent/test-classpath",
                "KLAVIYO_EVENT_METRICS": metrics,
                "KLAVIYO_TEST_CALLS": str(calls_path),
            }
            result = subprocess.run(
                ["bash", str(SCRIPT), *args],
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )
            calls = (
                calls_path.read_text(encoding="utf-8").splitlines()
                if calls_path.exists()
                else []
            )
            return result, calls

    def test_requires_a_job_key(self):
        result, calls = self.run_script()
        self.assertEqual(2, result.returncode)
        self.assertEqual([], calls)

    def test_runs_a_single_bounded_worker(self):
        for key in ("profiles", "event:MetricB2"):
            with self.subTest(key=key):
                result, calls = self.run_script(key)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertEqual(1, len(calls))
                self.assertIn("com.greyfielddata.klaviyo.IncrementalSync " + key, calls[0])
                self.assertIn("-classpath /nonexistent/test-classpath", calls[0])

    def test_rejects_unconfigured_or_malformed_keys_before_java(self):
        for key in ("event:NotConfigured", "event:bad-id", "orders", "profiles;id"):
            with self.subTest(key=key):
                result, calls = self.run_script(key)
                self.assertEqual(2, result.returncode)
                self.assertEqual([], calls)

    def test_rejects_malformed_metric_configuration(self):
        for metrics in ("MetricA1,bad id", "MetricA1,", ",MetricA1", "MetricA1,,MetricB2"):
            with self.subTest(metrics=metrics):
                result, calls = self.run_script("profiles", metrics=metrics)
                self.assertEqual(2, result.returncode)
                self.assertEqual([], calls)


if __name__ == "__main__":
    unittest.main()
