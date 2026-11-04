"""Static checks that the Java worker keeps its safety properties.

These complement the JUnit tests (which cover the pure planning logic) by
pinning down protocol details that only show up in the JDBC code.
"""
import re
import unittest

from helpers import ROOT

JAVA = ROOT / "src" / "main" / "java" / "com" / "greyfielddata" / "klaviyo"
RUNNER = JAVA / "IncrementalSync.java"
HEALTH = JAVA / "CohortHealth.java"
SESSION = JAVA / "SnowflakeSession.java"
STATE_DDL = ROOT / "sql" / "03_sync_state.sql"


def read(path):
    return path.read_text(encoding="utf-8")


class SourceContractTests(unittest.TestCase):
    def test_runner_uses_atomic_cursor_checkpoint_protocol(self):
        source = read(RUNNER)
        for needle in ("setAutoCommit(false)", "MERGE INTO", "NEXT_CURSOR",
                       "ACTIVE_WINDOW_START_TZ", "ACTIVE_WINDOW_END_TZ", "TARGET_CUTOFF_TZ",
                       "LAST_PROGRESS_AT=current_timestamp()", "LEASE_TOKEN",
                       "conn.commit()", "conn.rollback()"):
            with self.subTest(needle=needle):
                self.assertIn(needle, source)
        # Watermarks come from the rows actually committed, never from max() over the table.
        self.assertNotIn("max(updated)", source.lower())
        self.assertNotIn("max(event_datetime)", source.lower())

    def test_profiles_commit_a_monotonic_progressive_watermark(self):
        compact = re.sub(r"\s+", "", read(RUNNER)).replace('"+"', "")
        self.assertIn(
            "COMMITTED_WATERMARK_TZ=iff(?,ACTIVE_WINDOW_END_TZ,"
            "greatest(COMMITTED_WATERMARK_TZ,?::timestamp_tz))",
            compact,
        )
        source = read(RUNNER)
        self.assertIn("SyncPlan.progressiveWatermark(", source)
        self.assertIn("SyncPlan.groupMaxUpdated(", source)
        self.assertIn("WHERE JOB_KEY=? AND LEASE_TOKEN=? AND VERSION=?", source)

    def test_merges_are_idempotent_and_never_regress_profiles(self):
        source = read(RUNNER)
        self.assertIn("ON target.ID=source.ID", source)
        self.assertIn("WHEN MATCHED AND source.UPDATED>=target.UPDATED", source)
        self.assertIn("ON target.METRIC_ID=source.METRIC_ID AND target.ID=source.ID", source)
        self.assertEqual(2, source.count("QUALIFY row_number() over(partition by"))

    def test_cursorless_resume_and_grouped_commits(self):
        source = read(RUNNER)
        self.assertIn("SyncPlan.resumeUrl(", source)
        self.assertIn("SyncPlan.pagesPerCommit(", source)
        self.assertIn("KLAVIYO_PAGES_PER_COMMIT", source)
        self.assertIn("SyncPlan.MAX_WORKER_SECONDS", source)
        self.assertIn("SyncPlan.MAX_WORKER_PAGES", source)
        self.assertIn("SyncPlan.leaseSeconds(", source)
        # A failed request must not discard fully fetched pages.
        self.assertIn("commitPages(conn, state, pending)", source)

    def test_rate_limit_policy_is_shared_with_unit_tests(self):
        source = read(RUNNER)
        self.assertIn("code == 429 || code >= 500", source)
        self.assertIn('connection.getHeaderField("Retry-After")', source)
        self.assertIn("SyncPlan.retryDelayMillis(", source)
        self.assertIn("sleepWithinBudget(", source)

    def test_snowflake_sessions_guard_and_clean_up_transactions(self):
        session = read(SESSION)
        for needle in ("LOCK_TIMEOUT=120", "STATEMENT_TIMEOUT_IN_SECONDS=200",
                       "ABORT_DETACHED_QUERY=TRUE", "addShutdownHook", "removeShutdownHook",
                       "cleanup(true)", "cleanup(false)", "connection.rollback()",
                       "connection.close()", "snowflake_jwt"):
            with self.subTest(needle=needle):
                self.assertIn(needle, session)
        for path in (RUNNER, HEALTH):
            source = read(path)
            with self.subTest(path=path.name):
                self.assertLess(source.index("new SnowflakeSession.ShutdownGuard"),
                                source.index("session.configure(conn);"))

    def test_runner_bounds_network_and_runtime(self):
        source = read(RUNNER)
        for needle in ("setConnectTimeout", "setReadTimeout", "deadlineNanos",
                       '"INCOMPLETE"', '"FAILED"', "missing required data or links"):
            with self.subTest(needle=needle):
                self.assertIn(needle, source)

    def test_fault_injection_is_dev_schema_only(self):
        source = read(RUNNER)
        self.assertIn("after-merge-before-checkpoint", source)
        self.assertIn("after-checkpoint-before-commit", source)
        self.assertIn("devSchema.equals(session.schema)", source)

    def test_secrets_are_external_and_identifiers_validated(self):
        for path in (RUNNER, HEALTH, SESSION):
            source = read(path)
            with self.subTest(path=path.name):
                self.assertNotRegex(source, r"pk_[A-Za-z0-9_-]{20,}")
                self.assertNotRegex(source, r'password\s*=\s*"[^"]+"')
                self.assertNotIn("snowflakecomputing.com/?", source)
        runner = read(RUNNER)
        self.assertIn('readSecretFile("KLAVIYO_API_KEY_FILE")', runner)
        self.assertIn("requireKlaviyoUrl(url)", runner)
        self.assertIn("requireIdentifier", read(SESSION))

    def test_cohort_health_requires_all_streams_before_advancing(self):
        source = read(HEALTH)
        self.assertIn("health.reached == jobKeys.size()", source)
        self.assertIn("!seen.equals(jobKeys)", source)
        self.assertIn("cohort-cutoff-mismatch", source)
        self.assertIn("changed != expectedRows", source)
        self.assertIn("COMMITTED_WATERMARK_TZ>=TARGET_CUTOFF_TZ", source)

    def test_state_ddl_has_every_column_the_worker_uses(self):
        ddl = read(STATE_DDL)
        self.assertIn("create table if not exists klaviyo_sync_state", ddl.lower())
        for column in ("JOB_KEY", "COMMITTED_WATERMARK_TZ", "TARGET_CUTOFF_TZ",
                       "ACTIVE_WINDOW_START_TZ", "ACTIVE_WINDOW_END_TZ", "NEXT_CURSOR",
                       "PAGE_SEQ", "STATUS", "RUN_ID", "LEASE_TOKEN", "LEASE_EXPIRES_AT",
                       "VERSION", "ROWS_WRITTEN", "LAST_ERROR", "LAST_PROGRESS_AT", "UPDATED_AT"):
            with self.subTest(column=column):
                self.assertIn(column, ddl)


if __name__ == "__main__":
    unittest.main()
