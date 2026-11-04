package com.greyfielddata.klaviyo;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * Pure checks for the profile checkpoint protocol. The simulation mirrors the
 * worker loop: Klaviyo pages sorted by updated ascending, grouped commits that
 * save rows + cursor + watermark atomically, and crash/cursor-loss resumes.
 */
final class ProgressiveWatermarkTest {
    private static final Instant WINDOW_START = Instant.parse("2026-01-03T23:23:30.250Z");
    private static final Instant WINDOW_END = Instant.parse("2026-01-04T23:23:30.250Z");
    private static final Pattern LOWER_BOUND =
        Pattern.compile("greater-than\\(updated,([^)]+)\\)");

    @Test
    void advancesToMaxUpdatedMinusOverlap() {
        Instant next = SyncPlan.progressiveWatermark(
            "profiles", WINDOW_START, WINDOW_END, Instant.parse("2026-01-04T06:00:00Z"));
        require(Instant.parse("2026-01-04T05:59:59Z").equals(next),
            "watermark must advance to max(updated) minus the profile overlap");
    }

    @Test
    void neverMovesBackwards() {
        Instant committed = Instant.parse("2026-01-04T06:00:00Z");
        require(committed.equals(SyncPlan.progressiveWatermark(
            "profiles", committed, WINDOW_END, Instant.parse("2026-01-04T05:00:00Z"))),
            "an older page must not move the watermark backwards");
        require(committed.equals(SyncPlan.progressiveWatermark(
            "profiles", committed, WINDOW_END, committed)),
            "a page whose max equals the watermark must not move it backwards");
        require(committed.equals(SyncPlan.progressiveWatermark(
            "profiles", committed, WINDOW_END, null)),
            "an empty page must keep the watermark");
    }

    @Test
    void staysBelowWindowEndUntilWindowCompletes() {
        Instant committed = Instant.parse("2026-01-04T06:00:00Z");
        require(committed.equals(SyncPlan.progressiveWatermark(
            "profiles", committed, WINDOW_END, WINDOW_END.plusSeconds(30))),
            "a row updated at/after the window end must not advance the watermark");
        require(committed.equals(SyncPlan.progressiveWatermark(
            "profiles", committed, WINDOW_END, WINDOW_END)),
            "a row updated exactly at the window end must not advance the watermark");
    }

    @Test
    void anomalousGroupsDoNotAdvance() {
        Instant a = Instant.parse("2026-01-04T01:00:00Z");
        Instant b = Instant.parse("2026-01-04T02:00:00Z");
        require(b.equals(SyncPlan.groupMaxUpdated(List.of(a, a, b), WINDOW_START, WINDOW_END)),
            "an ordered in-window group yields its max updated");
        require(SyncPlan.groupMaxUpdated(List.of(), WINDOW_START, WINDOW_END) == null,
            "an empty group yields no watermark candidate");
        require(SyncPlan.groupMaxUpdated(List.of(b, a), WINDOW_START, WINDOW_END) == null,
            "out-of-order rows (profile edited mid-crawl) must not advance the watermark");
        require(SyncPlan.groupMaxUpdated(List.of(a, WINDOW_END), WINDOW_START, WINDOW_END) == null,
            "a row at/after the window end must not advance the watermark");
        require(SyncPlan.groupMaxUpdated(
                List.of(WINDOW_START.minusSeconds(5), a), WINDOW_START, WINDOW_END) == null,
            "a row before the query lower bound must not advance the watermark");
        require(a.equals(SyncPlan.groupMaxUpdated(
                List.of(WINDOW_START.minusMillis(500), a), WINDOW_START, WINDOW_END)),
            "rows inside the one-second overlap are legitimate");
    }

    @Test
    void leaseOutlivesTheWorkerBudget() {
        require(SyncPlan.leaseSeconds(120) == 900,
            "short workers keep the fifteen-minute lease");
        require(SyncPlan.leaseSeconds(1200) >= 1200 + 300,
            "a long profile cycle must never outlive its lease");
        require(SyncPlan.leaseSeconds(SyncPlan.MAX_WORKER_SECONDS)
                >= SyncPlan.MAX_WORKER_SECONDS + 300,
            "the maximum worker budget must stay inside the lease");
    }

    @Test
    void eventStreamsKeepWindowWatermarks() {
        require(WINDOW_START.equals(SyncPlan.progressiveWatermark(
            "event:MetricA1", WINDOW_START, WINDOW_END, Instant.parse("2026-01-04T06:00:00Z"))),
            "event streams keep their window-level watermark");
    }

    @Test
    void resumesCursorlessProfilesFromCommittedWatermark() {
        Instant committed = Instant.parse("2026-01-04T12:06:11Z");
        String url = SyncPlan.resumeUrl("profiles", WINDOW_START, WINDOW_END, committed);
        require(Instant.parse("2026-01-04T12:06:10Z").equals(lowerBound(url)),
            "cursorless resume must re-query from the watermark minus the overlap");
        require(url.equals(SyncPlan.initialUrl("profiles", committed, WINDOW_END)),
            "resume must reuse the standard bounded profile query");
        require(SyncPlan.resumeUrl("profiles", WINDOW_START, WINDOW_END, WINDOW_START)
                .equals(SyncPlan.initialUrl("profiles", WINDOW_START, WINDOW_END)),
            "a fresh window must start exactly where it did before");
    }

    @Test
    void eventResumeStillStartsAtWindowStart() {
        Instant later = Instant.parse("2026-01-04T12:00:00Z");
        require(SyncPlan.resumeUrl("event:MetricA1", WINDOW_START, WINDOW_END, later)
                .equals(SyncPlan.initialUrl("event:MetricA1", WINDOW_START, WINDOW_END)),
            "event resume semantics must be unchanged");
    }

    @Test
    void tiesAcrossPageBoundariesSurviveEveryResume() {
        List<Row> rows = tieHeavyDataset();
        for (boolean loseCursor : new boolean[] {true, false}) {
            for (int group : new int[] {1, 2, 3, 25, 100}) {
                for (int crashAfterCommit = 0; crashAfterCommit <= 8; crashAfterCommit++) {
                    Result result = simulate(rows, group, crashAfterCommit, loseCursor);
                    require(result.delivered.containsAll(ids(rows)),
                        "rows were skipped on resume (group=" + group
                            + ", crash=" + crashAfterCommit + ", lostCursor=" + loseCursor + ")");
                }
            }
        }
    }

    @Test
    void rateLimitDelaysAreUnchanged() {
        require(SyncPlan.retryDelayMillis("5", 1, 400L) == 5_000L,
            "numeric Retry-After must be honored");
        require(SyncPlan.retryDelayMillis("120", 1, 400L) == 60_000L,
            "Retry-After must stay capped at 60 seconds");
        require(SyncPlan.retryDelayMillis(null, 1, 400L) == 1_400L,
            "first backoff must stay one second plus jitter");
        require(SyncPlan.retryDelayMillis("soon", 3, 250L) == 4_250L,
            "non-numeric Retry-After must fall back to exponential backoff");
        require(SyncPlan.retryDelayMillis(null, 12, 999L) == 30_999L,
            "backoff must stay capped at thirty seconds plus jitter");
    }

    @Test
    void profileCommitGroupsAreBoundedAndEventsStayPerPage() {
        require(SyncPlan.pagesPerCommit("profiles", null) == 100,
            "profiles should amortize the full-table merge across one hundred pages");
        require(SyncPlan.pagesPerCommit("profiles", "25") == 25,
            "operators may choose a profile commit group within bounds");
        require(SyncPlan.pagesPerCommit("event:MetricA1", "25") == 1,
            "event streams keep one commit per page");
        requireRejected(() -> SyncPlan.pagesPerCommit("profiles", "101"));
        requireRejected(() -> SyncPlan.pagesPerCommit("profiles", "0"));
    }

    @Test
    void workerBudgetsAllowLongProfileCycles() {
        require(SyncPlan.boundedSetting("KLAVIYO_MAX_SECONDS", "1200",
                SyncPlan.DEFAULT_MAX_SECONDS, 30, SyncPlan.MAX_WORKER_SECONDS) == 1200,
            "the batch must be able to give profiles a twenty-minute cycle");
        require(SyncPlan.boundedSetting("KLAVIYO_MAX_PAGES", "1000",
                SyncPlan.DEFAULT_MAX_PAGES, 1, SyncPlan.MAX_WORKER_PAGES) == 1000,
            "the batch must be able to lift the ten-page cycle cap");
        require(SyncPlan.boundedSetting("KLAVIYO_MAX_SECONDS", null,
                SyncPlan.DEFAULT_MAX_SECONDS, 30, SyncPlan.MAX_WORKER_SECONDS) == 120,
            "unconfigured callers keep the short default budget");
        require(SyncPlan.boundedSetting("KLAVIYO_MAX_PAGES", null,
                SyncPlan.DEFAULT_MAX_PAGES, 1, SyncPlan.MAX_WORKER_PAGES) <= 100,
            "unconfigured callers keep a bounded default page budget");
        require(SyncPlan.MAX_WORKER_SECONDS <= 1800, "worker budget must stay bounded");
        requireRejected(() -> SyncPlan.boundedSetting("KLAVIYO_MAX_SECONDS",
            "1801", 120, 30, SyncPlan.MAX_WORKER_SECONDS));
    }

    /**
     * Simulates the worker. crashAfterCommit >= 0 injects a crash after that
     * many committed groups, while the next group has one fetched but
     * uncommitted page (its rows roll back). loseCursor also models Klaviyo
     * rejecting the saved cursor, so the worker nulls NEXT_CURSOR and
     * re-queries from the committed watermark.
     */
    private static Result simulate(List<Row> rows, int group, int crashAfterCommit,
                                   boolean loseCursor) {
        Result result = new Result();
        Instant watermark = WINDOW_START;
        List<Row> savedQuery = null;
        int savedCursor = 0;
        boolean windowActive = true;
        int commits = 0;
        boolean crashed = false;
        while (windowActive) {
            List<Row> query = savedQuery;
            int cursor = savedCursor;
            if (query == null) {
                String url = SyncPlan.resumeUrl(
                    "profiles", WINDOW_START, WINDOW_END, watermark);
                query = filter(rows, lowerBound(url), WINDOW_END);
                cursor = 0;
            }
            List<Row> pending = new ArrayList<Row>();
            int pagesInGroup = 0;
            while (true) {
                int end = Math.min(query.size(), cursor + SyncPlan.PROFILE_PAGE_SIZE);
                List<Row> page = query.subList(cursor, end);
                pending.addAll(page);
                pagesInGroup++;
                cursor = end;
                boolean complete = cursor >= query.size();
                if (!crashed && crashAfterCommit >= 0 && commits == crashAfterCommit && !complete) {
                    crashed = true; // uncommitted rows and cursor are rolled back
                    if (loseCursor) {
                        savedQuery = null;
                    }
                    break;
                }
                if (complete || pagesInGroup >= group) {
                    List<Instant> updates = new ArrayList<Instant>();
                    for (Row row : pending) {
                        result.delivered.add(row.id);
                        updates.add(row.updated);
                    }
                    Instant maxUpdated = SyncPlan.groupMaxUpdated(
                        updates, WINDOW_START, WINDOW_END);
                    Instant next = complete ? WINDOW_END
                        : SyncPlan.progressiveWatermark(
                            "profiles", watermark, WINDOW_END, maxUpdated);
                    require(!next.isBefore(watermark), "watermark moved backwards");
                    watermark = next;
                    savedQuery = complete ? null : query;
                    savedCursor = complete ? 0 : cursor;
                    windowActive = !complete;
                    commits++;
                    requireNothingMissingAtOrBelow(rows, result.delivered, watermark);
                    pending.clear();
                    pagesInGroup = 0;
                    if (complete) {
                        break;
                    }
                }
            }
        }
        return result;
    }

    private static void requireNothingMissingAtOrBelow(List<Row> rows, Set<String> delivered,
                                                       Instant watermark) {
        if (watermark.equals(WINDOW_END)) {
            return;
        }
        for (Row row : rows) {
            if (!row.updated.isAfter(watermark) && !delivered.contains(row.id)) {
                throw new AssertionError("watermark " + watermark
                    + " claims a row that was never loaded: " + row.updated);
            }
        }
    }

    /** 36,800 rows with timestamp ties crossing page and 100-page commit boundaries. */
    private static List<Row> tieHeavyDataset() {
        List<Row> rows = new ArrayList<Row>();
        Instant base = Instant.parse("2026-01-04T00:00:00Z");
        int seq = 0;
        int[] runLengths = {95, 12, 1, 130, 3, 1, 90, 20, 108};
        for (int run = 0; run < runLengths.length; run++) {
            Instant updated = base.plusSeconds(run * 7L);
            for (int i = 0; i < runLengths[run] * 80; i++) {
                rows.add(new Row("p" + (seq++), updated));
            }
        }
        return rows;
    }

    private static List<Row> filter(List<Row> rows, Instant lowerExclusive, Instant upperExclusive) {
        List<Row> out = new ArrayList<Row>();
        for (Row row : rows) {
            if (row.updated.isAfter(lowerExclusive) && row.updated.isBefore(upperExclusive)) {
                out.add(row);
            }
        }
        return out;
    }

    private static Instant lowerBound(String url) {
        String decoded = URLDecoder.decode(url, StandardCharsets.UTF_8);
        Matcher matcher = LOWER_BOUND.matcher(decoded);
        require(matcher.find(), "profile query lower bound missing");
        return Instant.parse(matcher.group(1));
    }

    private static Set<String> ids(List<Row> rows) {
        Set<String> out = new HashSet<String>();
        for (Row row : rows) {
            out.add(row.id);
        }
        return out;
    }

    private static void requireRejected(Runnable action) {
        try {
            action.run();
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new AssertionError("expected an out-of-range setting to be rejected");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static final class Row {
        final String id;
        final Instant updated;

        Row(String id, Instant updated) {
            this.id = id;
            this.updated = updated;
        }
    }

    private static final class Result {
        final Set<String> delivered = new HashSet<String>();
    }
}
