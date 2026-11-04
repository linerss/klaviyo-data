package com.greyfielddata.klaviyo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;

/**
 * Seeds and inspects KLAVIYO_SYNC_STATE for the configured job keys.
 *
 *   seed     insert a state row for every configured job key that has none.
 *            Existing rows are never changed. New rows start at
 *            KLAVIYO_INITIAL_WATERMARK and join the current cohort cutoff
 *            (or now minus the lateness allowance if the table is empty).
 *   inspect  print one line per state row plus raw table row counts.
 *
 * Tables must already exist (see sql/).
 */
public final class StateBootstrap {
    private StateBootstrap() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 || !("seed".equals(args[0]) || "inspect".equals(args[0]))) {
            throw new IllegalArgumentException("Expected seed or inspect");
        }
        SnowflakeSession session = SnowflakeSession.fromEnv();
        try (Connection conn = session.connect();
             SnowflakeSession.ShutdownGuard guard =
                 new SnowflakeSession.ShutdownGuard(conn, "klaviyo-bootstrap-shutdown")) {
            session.configure(conn);
            if ("inspect".equals(args[0])) {
                inspect(conn, session);
            } else {
                seed(conn, session, SyncPlan.configuredJobKeys());
            }
        }
    }

    private static void seed(Connection conn, SnowflakeSession session, Set<String> jobKeys)
        throws Exception {
        Instant initial = Instant.parse(
            SnowflakeSession.env("KLAVIYO_INITIAL_WATERMARK", "2020-01-01T00:00:00Z")
        );
        String state = session.table("KLAVIYO_SYNC_STATE");
        String sql = "MERGE INTO " + state + " target USING ("
            + " SELECT ? JOB_KEY, ? DATASET, ? METRIC_ID,"
            + " ?::timestamp_tz COMMITTED_WATERMARK_TZ,"
            + " coalesce((SELECT max(TARGET_CUTOFF_TZ) FROM " + state + "),"
            + " dateadd(second,-?,current_timestamp())) TARGET_CUTOFF_TZ"
            + ") source ON target.JOB_KEY=source.JOB_KEY"
            + " WHEN NOT MATCHED THEN INSERT (JOB_KEY,DATASET,METRIC_ID,"
            + "COMMITTED_WATERMARK_TZ,TARGET_CUTOFF_TZ,STATUS,VERSION,ROWS_WRITTEN,"
            + "LAST_PROGRESS_AT,UPDATED_AT) VALUES (source.JOB_KEY,source.DATASET,"
            + "source.METRIC_ID,source.COMMITTED_WATERMARK_TZ,source.TARGET_CUTOFF_TZ,"
            + "'IDLE',0,0,current_timestamp(),current_timestamp())";
        int inserted = 0;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (String jobKey : jobKeys) {
                stmt.setString(1, jobKey);
                stmt.setString(2, SyncPlan.isProfiles(jobKey) ? "profiles" : "events");
                stmt.setString(3, SyncPlan.metricId(jobKey));
                stmt.setTimestamp(4, Timestamp.from(initial));
                stmt.setInt(5, SyncPlan.LATENESS_SECONDS);
                inserted += stmt.executeUpdate();
            }
        }
        System.out.println("KLAVIYO_STATE_SEEDED inserted=" + inserted + " configured=" + jobKeys.size());
    }

    private static void inspect(Connection conn, SnowflakeSession session) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                "SELECT JOB_KEY,STATUS,PAGE_SEQ,ROWS_WRITTEN,"
                + "COMMITTED_WATERMARK_TZ,TARGET_CUTOFF_TZ,"
                + "NEXT_CURSOR is not null HAS_CURSOR"
                + " FROM " + session.table("KLAVIYO_SYNC_STATE")
                + " ORDER BY JOB_KEY"
            )) {
                while (rs.next()) {
                    System.out.println(
                        "KLAVIYO_STATE job=" + rs.getString(1)
                        + " status=" + rs.getString(2)
                        + " page=" + rs.getInt(3)
                        + " rows=" + rs.getLong(4)
                        + " watermark=" + rs.getString(5)
                        + " cutoff=" + rs.getString(6)
                        + " has_cursor=" + rs.getBoolean(7)
                    );
                }
            }
            try (ResultSet rs = stmt.executeQuery(
                "SELECT (SELECT count(*) FROM " + session.table("RAW_KLAVIYO_PROFILES") + "),"
                + " (SELECT count(*) FROM " + session.table("RAW_KLAVIYO_EVENTS") + ")"
            )) {
                rs.next();
                System.out.println(
                    "KLAVIYO_COUNTS profiles=" + rs.getLong(1) + " events=" + rs.getLong(2)
                );
            }
        }
    }
}
