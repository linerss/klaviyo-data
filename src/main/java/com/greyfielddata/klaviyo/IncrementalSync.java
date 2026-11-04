package com.greyfielddata.klaviyo;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * One bounded worker cycle for one job key ("profiles" or "event:METRIC_ID").
 *
 * The worker takes a lease on its state row, works through the active 24-hour
 * window page by page, and commits staged rows, the MERGE, the next cursor and
 * the watermark in one lease- and VERSION-guarded transaction. It stops when
 * its time or page budget runs out and the next run resumes from the saved
 * cursor (or, if Klaviyo rejects the cursor, from the committed watermark).
 *
 * Exit codes: 0 progress or caught up, 75 another worker holds the lease,
 * non-zero otherwise on failure.
 */
public final class IncrementalSync {
    private static final int CONNECT_TIMEOUT_MILLIS = 15_000;
    private static final int READ_TIMEOUT_MILLIS = 60_000;

    private final String jobKey;
    private final SnowflakeSession session;
    private final String devSchema;
    private final String apiKey;
    private final int maxPages;
    private final int pagesPerCommit;
    private final int leaseSeconds;
    private final long deadlineNanos;
    private final String runId;
    private final String leaseToken;

    private IncrementalSync(
        String jobKey,
        SnowflakeSession session,
        String devSchema,
        String apiKey,
        int maxPages,
        int pagesPerCommit,
        int maxSeconds,
        long deadlineNanos
    ) {
        this.jobKey = SyncPlan.requireJobKey(jobKey);
        this.session = session;
        this.devSchema = devSchema;
        this.apiKey = apiKey;
        this.maxPages = maxPages;
        this.pagesPerCommit = pagesPerCommit;
        this.leaseSeconds = SyncPlan.leaseSeconds(maxSeconds);
        this.deadlineNanos = deadlineNanos;
        this.runId = UUID.randomUUID().toString();
        this.leaseToken = UUID.randomUUID().toString();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected one configured Klaviyo job key");
        }
        String jobKey = SyncPlan.requireJobKey(args[0], SyncPlan.configuredJobKeys());

        int maxSeconds = boundedInt(
            "KLAVIYO_MAX_SECONDS",
            SyncPlan.DEFAULT_MAX_SECONDS,
            30,
            SyncPlan.MAX_WORKER_SECONDS
        );
        int maxPages = boundedInt(
            "KLAVIYO_MAX_PAGES",
            SyncPlan.DEFAULT_MAX_PAGES,
            1,
            SyncPlan.MAX_WORKER_PAGES
        );
        int pagesPerCommit = SyncPlan.pagesPerCommit(
            jobKey,
            System.getenv("KLAVIYO_PAGES_PER_COMMIT")
        );
        long deadlineNanos = System.nanoTime()
            + Duration.ofSeconds(maxSeconds).toNanos();
        String apiKey = SnowflakeSession.readSecretFile("KLAVIYO_API_KEY_FILE");
        SnowflakeSession session = SnowflakeSession.fromEnv();
        String devSchema = SnowflakeSession.requireIdentifier(
            SnowflakeSession.env("KLAVIYO_DEV_SCHEMA", "KLAVIYO_SYNC_DEV").toUpperCase()
        );

        IncrementalSync sync = new IncrementalSync(
            jobKey,
            session,
            devSchema,
            apiKey,
            maxPages,
            pagesPerCommit,
            maxSeconds,
            deadlineNanos
        );
        if (!sync.run()) {
            System.exit(75);
        }
    }

    private boolean run() throws Exception {
        try (Connection conn = session.connect();
             SnowflakeSession.ShutdownGuard shutdownGuard =
                 new SnowflakeSession.ShutdownGuard(conn, "klaviyo-incremental-shutdown")) {
            session.configure(conn);
            conn.setAutoCommit(false);
            if (!acquireLease(conn)) {
                System.out.println("KLAVIYO_SYNC_BUSY job=" + jobKey);
                return false;
            }

            try {
                State state = loadState(conn);
                state = ensureActiveWindow(conn, state);
                if (state == null) {
                    System.out.println("KLAVIYO_SYNC_CAUGHT_UP job=" + jobKey);
                    return true;
                }
                createStage(conn);
                processPages(conn, state);
                return true;
            } catch (CursorInvalidException e) {
                conn.rollback();
                resetCursor(conn, e.getMessage());
                System.out.println("KLAVIYO_SYNC_CURSOR_RESET job=" + jobKey);
                return true;
            } catch (Exception e) {
                conn.rollback();
                markFailed(conn, safeError(e));
                throw e;
            }
        }
    }

    private boolean acquireLease(Connection conn) throws SQLException {
        String sql = "UPDATE " + stateTable()
            + " SET LEASE_TOKEN=?, LEASE_EXPIRES_AT=dateadd(second,?,current_timestamp()),"
            + " RUN_ID=?, STATUS='RUNNING', LAST_ERROR=null,"
            + " VERSION=VERSION+1, UPDATED_AT=current_timestamp()"
            + " WHERE JOB_KEY=?"
            + " AND (LEASE_TOKEN IS NULL OR LEASE_EXPIRES_AT<current_timestamp())";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, leaseToken);
            stmt.setInt(2, leaseSeconds);
            stmt.setString(3, runId);
            stmt.setString(4, jobKey);
            int changed = stmt.executeUpdate();
            conn.commit();
            if (changed == 0) {
                return false;
            }
            if (changed != 1) {
                throw new SQLException("Lease update changed an unexpected number of rows");
            }
            return true;
        }
    }

    private State loadState(Connection conn) throws SQLException {
        String sql = "SELECT COMMITTED_WATERMARK_TZ,ACTIVE_WINDOW_START_TZ,"
            + "ACTIVE_WINDOW_END_TZ,NEXT_CURSOR,PAGE_SEQ,VERSION,TARGET_CUTOFF_TZ"
            + " FROM " + stateTable() + " WHERE JOB_KEY=?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, jobKey);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Missing preseeded Klaviyo state row");
                }
                State state = new State();
                state.committedWatermark = instant(rs, 1);
                state.windowStart = nullableInstant(rs, 2);
                state.windowEnd = nullableInstant(rs, 3);
                state.nextCursor = rs.getString(4);
                state.pageSeq = rs.getInt(5);
                state.version = rs.getLong(6);
                state.targetCutoff = instant(rs, 7);
                if (rs.next()) {
                    throw new SQLException("Duplicate Klaviyo state rows");
                }
                return state;
            }
        }
    }

    private State ensureActiveWindow(Connection conn, State state) throws SQLException {
        if (state.windowStart != null && state.windowEnd != null) {
            return state;
        }

        Instant cutoff = state.targetCutoff;
        if (!state.committedWatermark.isBefore(cutoff)) {
            updateTerminalState(conn, "CAUGHT_UP", null);
            return null;
        }
        Instant end = state.committedWatermark.plus(
            Duration.ofHours(SyncPlan.WINDOW_HOURS)
        );
        if (end.isAfter(cutoff)) {
            end = cutoff;
        }

        String sql = "UPDATE " + stateTable()
            + " SET ACTIVE_WINDOW_START_TZ=?,ACTIVE_WINDOW_END_TZ=?,NEXT_CURSOR=null,"
            + " PAGE_SEQ=0,STATUS='RUNNING',VERSION=VERSION+1,UPDATED_AT=current_timestamp()"
            + " WHERE JOB_KEY=? AND LEASE_TOKEN=? AND VERSION=?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setTimestamp(1, Timestamp.from(state.committedWatermark));
            stmt.setTimestamp(2, Timestamp.from(end));
            stmt.setString(3, jobKey);
            stmt.setString(4, leaseToken);
            stmt.setLong(5, state.version);
            requireSingleUpdate(stmt.executeUpdate(), "window initialization");
            conn.commit();
        }
        return loadState(conn);
    }

    private void createStage(Connection conn) throws SQLException {
        String sql;
        if (SyncPlan.isProfiles(jobKey)) {
            sql = "CREATE TEMP TABLE IF NOT EXISTS KLAVIYO_PROFILE_PAGE_STAGE ("
                + "ID varchar,EMAIL varchar,EMAIL_MARKETING boolean,"
                + "SMS_MARKETING boolean,UPDATED timestamp_tz)";
        } else {
            sql = "CREATE TEMP TABLE IF NOT EXISTS KLAVIYO_EVENT_PAGE_STAGE ("
                + "ID varchar,METRIC_ID varchar,PROFILE_ID varchar,"
                + "EVENT_DATETIME timestamp_tz)";
        }
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
        conn.commit();
    }

    private void processPages(Connection conn, State initialState) throws Exception {
        long cycleStarted = System.nanoTime();
        long fetchNanos = 0;
        long rowsFetched = 0;
        State state = initialState;
        List<ApiPage> pending = new ArrayList<ApiPage>();
        String cursor = state.nextCursor;
        int pagesThisRun = 0;
        while (pagesThisRun < maxPages && hasTimeForRequest()) {
            boolean cursorRequest = cursor != null && cursor.length() > 0;
            String url = cursorRequest
                ? cursor
                : SyncPlan.resumeUrl(
                    jobKey,
                    state.windowStart,
                    state.windowEnd,
                    state.committedWatermark
                );
            ApiPage page;
            try {
                long fetchStarted = System.nanoTime();
                page = request(url, cursorRequest);
                fetchNanos += System.nanoTime() - fetchStarted;
                rowsFetched += page.rowCount();
            } catch (Exception e) {
                // Keep fully fetched pages durable before surfacing the error.
                if (!pending.isEmpty()) {
                    try {
                        state = commitPages(conn, state, pending);
                        pending.clear();
                    } catch (SQLException commitError) {
                        e.addSuppressed(commitError);
                    }
                }
                throw e;
            }
            pending.add(page);
            pagesThisRun++;
            cursor = page.nextCursor;
            System.out.println(
                "KLAVIYO_SYNC_PROGRESS job=" + jobKey
                + " pages=" + pagesThisRun
                + " rows=" + page.rowCount()
            );

            if (page.nextCursor == null || pending.size() >= pagesPerCommit) {
                state = commitPages(conn, state, pending);
                pending.clear();
            }
            if (page.nextCursor == null) {
                System.out.println(
                    "KLAVIYO_SYNC_WINDOW_COMPLETE job=" + jobKey
                    + " watermark=" + state.committedWatermark
                );
                printCycleMetrics(cycleStarted, fetchNanos, rowsFetched, pagesThisRun);
                return;
            }
        }
        if (!pending.isEmpty()) {
            state = commitPages(conn, state, pending);
            pending.clear();
        }
        markIncomplete(conn);
        printCycleMetrics(cycleStarted, fetchNanos, rowsFetched, pagesThisRun);
        System.out.println(
            "KLAVIYO_SYNC_INCOMPLETE job=" + jobKey
            + " pages=" + pagesThisRun
            + " watermark=" + state.committedWatermark
        );
    }

    private void printCycleMetrics(long started, long fetchNanos, long rows, int pages) {
        // Aggregate timing only: never emit profile identifiers, payloads or cursors.
        System.out.println("KLAVIYO_SYNC_TIMING job=" + jobKey
            + " pages=" + pages + " rows=" + rows
            + " fetch_ms=" + fetchNanos / 1_000_000L
            + " cycle_ms=" + (System.nanoTime() - started) / 1_000_000L);
    }

    private ApiPage request(String url, boolean cursorRequest) throws Exception {
        requireKlaviyoUrl(url);
        int attempt = 0;
        while (hasTimeForRequest()) {
            attempt++;
            HttpURLConnection connection = (HttpURLConnection)new URL(url).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
            connection.setReadTimeout(READ_TIMEOUT_MILLIS);
            connection.setUseCaches(false);
            connection.setRequestProperty("accept", "application/vnd.api+json");
            connection.setRequestProperty("Authorization", "Klaviyo-API-Key " + apiKey);
            connection.setRequestProperty("revision", SyncPlan.API_REVISION);

            int code = connection.getResponseCode();
            if (code == 429 || code >= 500) {
                closeQuietly(connection.getErrorStream());
                long sleepMillis = retryDelayMillis(connection, attempt);
                connection.disconnect();
                sleepWithinBudget(sleepMillis);
                continue;
            }
            if (code >= 400) {
                closeQuietly(connection.getErrorStream());
                connection.disconnect();
                if (cursorRequest && (code == 400 || code == 404 || code == 410)) {
                    throw new CursorInvalidException("Saved cursor rejected with HTTP " + code);
                }
                throw new IllegalStateException("Klaviyo API returned HTTP " + code);
            }

            try (InputStream stream = connection.getInputStream();
                 InputStreamReader reader =
                     new InputStreamReader(stream, StandardCharsets.UTF_8)) {
                JSONObject body = (JSONObject)new JSONParser().parse(reader);
                return parsePage(body);
            } finally {
                connection.disconnect();
            }
        }
        throw new IllegalStateException("Klaviyo request budget exhausted");
    }

    private ApiPage parsePage(JSONObject body) {
        if (!(body.get("data") instanceof JSONArray)
            || !(body.get("links") instanceof JSONObject)
            || !((JSONObject)body.get("links")).containsKey("next")) {
            throw new IllegalArgumentException(
                "Klaviyo response is missing required data or links"
            );
        }
        ApiPage page = new ApiPage();
        JSONArray data = array(body.get("data"));
        if (SyncPlan.isProfiles(jobKey)) {
            for (Object item : data) {
                JSONObject profile = object(item);
                JSONObject attributes = object(profile.get("attributes"));
                JSONObject subscriptions = object(attributes.get("subscriptions"));
                JSONObject email = object(object(subscriptions.get("email")).get("marketing"));
                JSONObject sms = object(object(subscriptions.get("sms")).get("marketing"));
                ProfileRow row = new ProfileRow();
                row.id = string(profile.get("id"));
                row.email = nullableString(attributes.get("email"));
                row.updated = Instant.parse(string(attributes.get("updated")));
                row.emailMarketing = consent(
                    email.get("can_receive_email_marketing")
                );
                row.smsMarketing = consent(
                    sms.get("can_receive_sms_marketing")
                );
                page.profiles.add(row);
            }
        } else {
            String metricId = SyncPlan.metricId(jobKey);
            for (Object item : data) {
                JSONObject event = object(item);
                JSONObject attributes = object(event.get("attributes"));
                JSONObject relationships = object(event.get("relationships"));
                JSONObject profileRelationship = object(relationships.get("profile"));
                JSONObject profile = object(profileRelationship.get("data"));
                EventRow row = new EventRow();
                row.id = string(event.get("id"));
                row.metricId = metricId;
                row.profileId = nullableString(profile.get("id"));
                row.eventDatetime = Instant.parse(string(attributes.get("datetime")));
                page.events.add(row);
            }
        }
        JSONObject links = object(body.get("links"));
        page.nextCursor = nullableString(links.get("next"));
        return page;
    }

    /**
     * Commits one group of consecutive pages: staged rows, MERGE, cursor and
     * watermark move in a single lease- and VERSION-guarded transaction.
     */
    private State commitPages(Connection conn, State state, List<ApiPage> pages)
        throws SQLException {
        ApiPage last = pages.get(pages.size() - 1);
        List<ProfileRow> profiles = new ArrayList<ProfileRow>();
        List<EventRow> events = new ArrayList<EventRow>();
        List<Instant> updates = new ArrayList<Instant>();
        int rows = 0;
        for (ApiPage page : pages) {
            profiles.addAll(page.profiles);
            events.addAll(page.events);
            rows += page.rowCount();
            for (ProfileRow row : page.profiles) {
                updates.add(row.updated);
            }
        }
        Instant maxUpdated = SyncPlan.groupMaxUpdated(
            updates,
            state.windowStart,
            state.windowEnd
        );
        try {
            if (SyncPlan.isProfiles(jobKey)) {
                stageProfiles(conn, profiles);
                mergeProfiles(conn);
            } else {
                stageEvents(conn, events);
                mergeEvents(conn);
            }
            maybeFail("after-merge-before-checkpoint");
            boolean windowComplete = last.nextCursor == null;
            Instant watermark = SyncPlan.progressiveWatermark(
                jobKey,
                state.committedWatermark,
                state.windowEnd,
                maxUpdated
            );
            String sql = "UPDATE " + stateTable()
                + " SET NEXT_CURSOR=?,PAGE_SEQ=?,ROWS_WRITTEN=ROWS_WRITTEN+?,"
                + " COMMITTED_WATERMARK_TZ=iff(?,ACTIVE_WINDOW_END_TZ,"
                + "greatest(COMMITTED_WATERMARK_TZ,?::timestamp_tz)),"
                + " ACTIVE_WINDOW_START_TZ=iff(?,null,ACTIVE_WINDOW_START_TZ),"
                + " ACTIVE_WINDOW_END_TZ=iff(?,null,ACTIVE_WINDOW_END_TZ),"
                + " STATUS=?,LEASE_TOKEN=iff(?,null,LEASE_TOKEN),"
                + " LEASE_EXPIRES_AT=iff(?,null,dateadd(second,?,current_timestamp())),"
                + " LAST_PROGRESS_AT=current_timestamp(),"
                + " VERSION=VERSION+1,UPDATED_AT=current_timestamp()"
                + " WHERE JOB_KEY=? AND LEASE_TOKEN=? AND VERSION=?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, last.nextCursor);
                stmt.setInt(2, windowComplete ? 0 : state.pageSeq + pages.size());
                stmt.setInt(3, rows);
                stmt.setBoolean(4, windowComplete);
                stmt.setTimestamp(5, Timestamp.from(watermark));
                stmt.setBoolean(6, windowComplete);
                stmt.setBoolean(7, windowComplete);
                stmt.setString(8, windowComplete ? "PROGRESS" : "RUNNING");
                stmt.setBoolean(9, windowComplete);
                stmt.setBoolean(10, windowComplete);
                stmt.setInt(11, leaseSeconds);
                stmt.setString(12, jobKey);
                stmt.setString(13, leaseToken);
                stmt.setLong(14, state.version);
                requireSingleUpdate(stmt.executeUpdate(), "page checkpoint");
            }
            maybeFail("after-checkpoint-before-commit");
            conn.commit();
            return loadState(conn);
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    private void stageProfiles(Connection conn, List<ProfileRow> rows) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DELETE FROM KLAVIYO_PROFILE_PAGE_STAGE");
        }
        String sql = "INSERT INTO KLAVIYO_PROFILE_PAGE_STAGE"
            + " (ID,EMAIL,EMAIL_MARKETING,SMS_MARKETING,UPDATED) VALUES (?,?,?,?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (ProfileRow row : rows) {
                stmt.setString(1, row.id);
                stmt.setString(2, row.email);
                setNullableBoolean(stmt, 3, row.emailMarketing);
                setNullableBoolean(stmt, 4, row.smsMarketing);
                stmt.setTimestamp(5, Timestamp.from(row.updated));
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    private void stageEvents(Connection conn, List<EventRow> rows) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DELETE FROM KLAVIYO_EVENT_PAGE_STAGE");
        }
        String sql = "INSERT INTO KLAVIYO_EVENT_PAGE_STAGE"
            + " (ID,METRIC_ID,PROFILE_ID,EVENT_DATETIME) VALUES (?,?,?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (EventRow row : rows) {
                stmt.setString(1, row.id);
                stmt.setString(2, row.metricId);
                stmt.setString(3, row.profileId);
                stmt.setTimestamp(4, Timestamp.from(row.eventDatetime));
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    private void mergeProfiles(Connection conn) throws SQLException {
        String sql = "MERGE INTO " + table("RAW_KLAVIYO_PROFILES") + " target USING ("
            + " SELECT ID,EMAIL,EMAIL_MARKETING,SMS_MARKETING,UPDATED"
            + " FROM KLAVIYO_PROFILE_PAGE_STAGE"
            + " QUALIFY row_number() over(partition by ID order by UPDATED desc)=1"
            + ") source ON target.ID=source.ID"
            + " WHEN MATCHED AND source.UPDATED>=target.UPDATED THEN UPDATE SET"
            + " EMAIL=source.EMAIL,EMAIL_MARKETING=source.EMAIL_MARKETING,"
            + " SMS_MARKETING=source.SMS_MARKETING,UPDATED=source.UPDATED"
            + " WHEN NOT MATCHED THEN INSERT"
            + " (ID,EMAIL,EMAIL_MARKETING,SMS_MARKETING,UPDATED) VALUES"
            + " (source.ID,source.EMAIL,source.EMAIL_MARKETING,"
            + " source.SMS_MARKETING,source.UPDATED)";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    private void mergeEvents(Connection conn) throws SQLException {
        String sql = "MERGE INTO " + table("RAW_KLAVIYO_EVENTS") + " target USING ("
            + " SELECT ID,METRIC_ID,PROFILE_ID,EVENT_DATETIME"
            + " FROM KLAVIYO_EVENT_PAGE_STAGE"
            + " QUALIFY row_number() over(partition by METRIC_ID,ID"
            + " order by EVENT_DATETIME desc)=1"
            + ") source ON target.METRIC_ID=source.METRIC_ID AND target.ID=source.ID"
            + " WHEN MATCHED THEN UPDATE SET PROFILE_ID=source.PROFILE_ID,"
            + " EVENT_DATETIME=source.EVENT_DATETIME"
            + " WHEN NOT MATCHED THEN INSERT"
            + " (ID,METRIC_ID,PROFILE_ID,EVENT_DATETIME) VALUES"
            + " (source.ID,source.METRIC_ID,source.PROFILE_ID,source.EVENT_DATETIME)";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    private void markIncomplete(Connection conn) throws SQLException {
        updateTerminalState(conn, "INCOMPLETE", null);
    }

    private void markFailed(Connection conn, String error) throws SQLException {
        updateTerminalState(conn, "FAILED", error);
    }

    private void resetCursor(Connection conn, String error) throws SQLException {
        String sql = "UPDATE " + stateTable()
            + " SET NEXT_CURSOR=null,PAGE_SEQ=0,STATUS='INCOMPLETE',"
            + " LAST_ERROR=?,LEASE_TOKEN=null,LEASE_EXPIRES_AT=null,"
            + " VERSION=VERSION+1,UPDATED_AT=current_timestamp()"
            + " WHERE JOB_KEY=? AND LEASE_TOKEN=?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, error);
            stmt.setString(2, jobKey);
            stmt.setString(3, leaseToken);
            requireSingleUpdate(stmt.executeUpdate(), "cursor reset");
            conn.commit();
        }
    }

    private void updateTerminalState(Connection conn, String status, String error)
        throws SQLException {
        String sql = "UPDATE " + stateTable()
            + " SET STATUS=?,LAST_ERROR=?,LEASE_TOKEN=null,LEASE_EXPIRES_AT=null,"
            + " VERSION=VERSION+1,UPDATED_AT=current_timestamp()"
            + " WHERE JOB_KEY=? AND LEASE_TOKEN=?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, status);
            stmt.setString(2, error);
            stmt.setString(3, jobKey);
            stmt.setString(4, leaseToken);
            requireSingleUpdate(stmt.executeUpdate(), "terminal state");
            conn.commit();
        }
    }

    private String stateTable() {
        return table("KLAVIYO_SYNC_STATE");
    }

    private String table(String name) {
        return session.table(name);
    }

    /**
     * Test-only crash injection between the MERGE and the checkpoint commit.
     * Refused unless the configured schema is the dev schema.
     */
    private void maybeFail(String point) throws SQLException {
        String requested = System.getenv("KLAVIYO_FAILPOINT");
        if (requested == null || requested.length() == 0) {
            return;
        }
        if (!devSchema.equals(session.schema)) {
            throw new SQLException("Klaviyo failpoints are restricted to the dev schema");
        }
        if (!"after-merge-before-checkpoint".equals(requested)
            && !"after-checkpoint-before-commit".equals(requested)) {
            throw new SQLException("Unknown Klaviyo failpoint");
        }
        if (requested.equals(point)) {
            throw new SQLException("Injected Klaviyo failure at " + point);
        }
    }

    /** Pagination links come from the API response; only follow Klaviyo HTTPS URLs. */
    static String requireKlaviyoUrl(String value) {
        URI uri = URI.create(value);
        if (!"https".equalsIgnoreCase(uri.getScheme())
            || !SyncPlan.API_HOST.equalsIgnoreCase(uri.getHost())
            || uri.getUserInfo() != null
            || uri.getPort() != -1) {
            throw new IllegalArgumentException("Untrusted Klaviyo pagination URL");
        }
        return value;
    }

    private boolean hasTimeForRequest() {
        return deadlineNanos - System.nanoTime() > Duration.ofSeconds(10).toNanos();
    }

    private void sleepWithinBudget(long millis) throws InterruptedException {
        long remainingMillis = Duration.ofNanos(
            Math.max(0L, deadlineNanos - System.nanoTime())
        ).toMillis();
        if (remainingMillis <= 10_000L) {
            throw new IllegalStateException("Klaviyo retry budget exhausted");
        }
        Thread.sleep(Math.min(millis, remainingMillis - 10_000L));
    }

    private static long retryDelayMillis(HttpURLConnection connection, int attempt) {
        return SyncPlan.retryDelayMillis(
            connection.getHeaderField("Retry-After"),
            attempt,
            ThreadLocalRandom.current().nextLong(250L, 1000L)
        );
    }

    private static int boundedInt(String name, int fallback, int minimum, int maximum) {
        return SyncPlan.boundedSetting(
            name,
            System.getenv(name),
            fallback,
            minimum,
            maximum
        );
    }

    private static Instant instant(ResultSet rs, int column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        if (value == null) {
            throw new SQLException("Required checkpoint timestamp is null");
        }
        return value.toInstant();
    }

    private static Instant nullableInstant(ResultSet rs, int column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private static void requireSingleUpdate(int changed, String operation)
        throws SQLException {
        if (changed != 1) {
            throw new SQLException(operation + " changed " + changed + " state rows");
        }
    }

    private static void setNullableBoolean(
        PreparedStatement stmt,
        int index,
        Boolean value
    ) throws SQLException {
        if (value == null) {
            stmt.setNull(index, java.sql.Types.BOOLEAN);
        } else {
            stmt.setBoolean(index, value.booleanValue());
        }
    }

    static Boolean consent(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        if ("true".equalsIgnoreCase(text) || "subscribed".equalsIgnoreCase(text)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(text) || "unsubscribed".equalsIgnoreCase(text)) {
            return Boolean.FALSE;
        }
        return null;
    }

    private static JSONObject object(Object value) {
        return value instanceof JSONObject ? (JSONObject)value : new JSONObject();
    }

    private static JSONArray array(Object value) {
        return value instanceof JSONArray ? (JSONArray)value : new JSONArray();
    }

    private static String string(Object value) {
        if (value == null || value.toString().length() == 0) {
            throw new IllegalArgumentException("Required Klaviyo value is missing");
        }
        return value.toString();
    }

    private static String nullableString(Object value) {
        return value == null ? null : value.toString();
    }

    /** Error text stored in the state table: class and message only, one line, bounded. */
    static String safeError(Exception error) {
        String message = error.getClass().getSimpleName() + ": " + error.getMessage();
        message = message.replaceAll("[\\r\\n\\t]+", " ");
        return message.length() <= 500 ? message : message.substring(0, 500);
    }

    private static void closeQuietly(InputStream stream) {
        if (stream == null) {
            return;
        }
        try {
            stream.close();
        } catch (Exception ignored) {
            // Nothing useful to do with a close failure on an error stream.
        }
    }

    private static final class State {
        Instant committedWatermark;
        Instant windowStart;
        Instant windowEnd;
        String nextCursor;
        int pageSeq;
        long version;
        Instant targetCutoff;
    }

    private static final class ApiPage {
        final List<ProfileRow> profiles = new ArrayList<ProfileRow>();
        final List<EventRow> events = new ArrayList<EventRow>();
        String nextCursor;

        int rowCount() {
            return profiles.size() + events.size();
        }
    }

    private static final class ProfileRow {
        String id;
        String email;
        Boolean emailMarketing;
        Boolean smsMarketing;
        Instant updated;
    }

    private static final class EventRow {
        String id;
        String metricId;
        String profileId;
        Instant eventDatetime;
    }

    private static final class CursorInvalidException extends Exception {
        CursorInvalidException(String message) {
            super(message);
        }
    }
}
