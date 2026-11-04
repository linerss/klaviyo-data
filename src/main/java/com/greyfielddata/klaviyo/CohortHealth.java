package com.greyfielddata.klaviyo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Checks the whole cohort of job keys against their shared target cutoff.
 *
 * All streams work towards the same TARGET_CUTOFF_TZ. When every stream's
 * committed watermark has reached it, the cohort is complete and every row
 * moves to a new cutoff (now minus the lateness allowance) in one statement.
 * A cohort never advances while any stream is behind, so downstream readers
 * can treat the previous cutoff as a consistent "loaded through" point.
 *
 * Without arguments: validate, then advance the cohort if complete.
 * With --batch-status: print the state as one KLAVIYO_BATCH_STATE JSON line
 * and change nothing.
 */
public final class CohortHealth {
    private static final int DEFAULT_MAX_NO_PROGRESS_HOURS = 10;

    private CohortHealth() {
    }

    public static void main(String[] args) throws Exception {
        boolean batchStatus = args.length == 1 && "--batch-status".equals(args[0]);
        if (args.length != 0 && !batchStatus) {
            throw new IllegalArgumentException("Expected no arguments or --batch-status");
        }
        Set<String> jobKeys = SyncPlan.configuredJobKeys();
        int maxNoProgressHours = SyncPlan.boundedSetting(
            "KLAVIYO_MAX_NO_PROGRESS_HOURS",
            System.getenv("KLAVIYO_MAX_NO_PROGRESS_HOURS"),
            DEFAULT_MAX_NO_PROGRESS_HOURS,
            1,
            168
        );
        SnowflakeSession session = SnowflakeSession.fromEnv();

        try (Connection conn = session.connect();
             SnowflakeSession.ShutdownGuard shutdownGuard =
                 new SnowflakeSession.ShutdownGuard(conn, "klaviyo-cohort-health-shutdown")) {
            session.configure(conn);
            conn.setAutoCommit(false);
            Health health = readHealth(conn, session, jobKeys, maxNoProgressHours);
            if (batchStatus) {
                if (health.issues.contains("state-key-set-mismatch")
                    || health.issues.contains("cohort-cutoff-mismatch")) {
                    throw new IllegalStateException("Invalid Klaviyo batch state structure");
                }
                JSONObject payload = new JSONObject();
                payload.put("target_cutoff_epoch", health.targetCutoff.getEpochSecond());
                payload.put("jobs", health.jobs);
                conn.rollback(); // This mode observes state; it never advances the cohort.
                System.out.println("KLAVIYO_BATCH_STATE " + payload.toJSONString());
                return;
            }
            validate(health);
            if (health.allReached) {
                advanceCohort(conn, session, jobKeys.size(), health.targetCutoff);
                System.out.println("KLAVIYO_COHORT_COMPLETE cutoff=" + health.targetCutoff);
            } else {
                conn.commit();
                System.out.println(
                    "KLAVIYO_COHORT_PROGRESS cutoff=" + health.targetCutoff
                    + " reached=" + health.reached + "/" + jobKeys.size()
                );
            }
        }
    }

    private static Health readHealth(
        Connection conn,
        SnowflakeSession session,
        Set<String> jobKeys,
        int maxNoProgressHours
    ) throws SQLException {
        String sql = "SELECT JOB_KEY,STATUS,COMMITTED_WATERMARK_TZ,"
            + "TARGET_CUTOFF_TZ,LEASE_EXPIRES_AT,LAST_PROGRESS_AT"
            + " FROM " + session.table("KLAVIYO_SYNC_STATE")
            + " ORDER BY JOB_KEY";
        Health health = new Health();
        Set<Instant> cutoffs = new LinkedHashSet<Instant>();
        Set<String> seen = new LinkedHashSet<String>();
        Instant now = Instant.now();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String jobKey = rs.getString(1);
                String status = rs.getString(2);
                Instant watermark = requiredInstant(rs, 3);
                Instant cutoff = requiredInstant(rs, 4);
                Instant leaseExpires = nullableInstant(rs, 5);
                Instant progressAt = requiredInstant(rs, 6);
                JSONObject job = new JSONObject();
                job.put("job_key", jobKey);
                job.put("status", status);
                job.put("watermark_epoch", watermark.getEpochSecond());
                job.put("last_progress_epoch", progressAt.getEpochSecond());
                health.jobs.add(job);
                seen.add(jobKey);
                cutoffs.add(cutoff);
                if (!watermark.isBefore(cutoff)) {
                    health.reached++;
                }
                if ("FAILED".equals(status)) {
                    health.issues.add(jobKey + ":failed");
                }
                if ("RUNNING".equals(status)
                    && leaseExpires != null
                    && leaseExpires.isBefore(now)) {
                    health.issues.add(jobKey + ":expired-lease");
                }
                if (watermark.isBefore(cutoff)
                    && progressAt.isBefore(now.minus(Duration.ofHours(maxNoProgressHours)))) {
                    health.issues.add(jobKey + ":no-progress");
                }
            }
        }
        if (!seen.equals(jobKeys)) {
            health.issues.add("state-key-set-mismatch");
        }
        if (cutoffs.size() != 1) {
            health.issues.add("cohort-cutoff-mismatch");
        } else {
            health.targetCutoff = cutoffs.iterator().next();
        }
        health.allReached = health.reached == jobKeys.size();
        return health;
    }

    private static void validate(Health health) {
        if (!health.issues.isEmpty()) {
            throw new IllegalStateException(
                "Klaviyo cohort unhealthy: " + String.join(",", health.issues)
            );
        }
    }

    private static void advanceCohort(
        Connection conn,
        SnowflakeSession session,
        int expectedRows,
        Instant completedCutoff
    ) throws SQLException {
        String nextCohort = "UPDATE " + session.table("KLAVIYO_SYNC_STATE")
            + " SET TARGET_CUTOFF_TZ=dateadd(second,-?,current_timestamp()),"
            + " STATUS='PENDING',VERSION=VERSION+1,UPDATED_AT=current_timestamp()"
            + " WHERE TARGET_CUTOFF_TZ=?"
            + " AND ACTIVE_WINDOW_START_TZ IS NULL"
            + " AND ACTIVE_WINDOW_END_TZ IS NULL"
            + " AND COMMITTED_WATERMARK_TZ>=TARGET_CUTOFF_TZ";
        try (PreparedStatement stmt = conn.prepareStatement(nextCohort)) {
            stmt.setInt(1, SyncPlan.LATENESS_SECONDS);
            stmt.setTimestamp(2, Timestamp.from(completedCutoff));
            int changed = stmt.executeUpdate();
            if (changed != expectedRows) {
                throw new SQLException("Klaviyo cohort advance changed " + changed + " rows");
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    private static Instant requiredInstant(ResultSet rs, int column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        if (value == null) {
            throw new SQLException("Required cohort timestamp is null");
        }
        return value.toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, int column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private static final class Health {
        final JSONArray jobs = new JSONArray();
        int reached;
        boolean allReached;
        Instant targetCutoff;
        final List<String> issues = new ArrayList<String>();
    }
}
