package com.greyfielddata.klaviyo;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Pure, side-effect-free planning logic for the Klaviyo sync: job keys,
 * query URLs, watermark rules, commit grouping, lease length and retry delays.
 * Everything here is unit tested without a network or database.
 */
public final class SyncPlan {
    public static final int DEFAULT_MAX_SECONDS = 120;
    public static final int DEFAULT_MAX_PAGES = 50;
    // Upper bounds for callers (the batch runner) that deliberately give one
    // worker a long cycle so session, lease and JVM setup are amortized.
    public static final int MAX_WORKER_SECONDS = 1800;
    public static final int MAX_WORKER_PAGES = 2000;
    // Each profile commit MERGEs into the full profiles table, so its cost is
    // nearly independent of the number of staged rows. Grouping pages
    // amortizes that cost; a crash replays at most one uncommitted group.
    // Event streams keep one commit per page.
    public static final int PROFILE_PAGES_PER_COMMIT = 100;
    public static final int MAX_PAGES_PER_COMMIT = 100;
    public static final int MIN_LEASE_SECONDS = 900;
    public static final int LEASE_MARGIN_SECONDS = 300;
    public static final int PROFILE_PAGE_SIZE = 100;
    public static final int EVENT_PAGE_SIZE = 200;
    public static final int WINDOW_HOURS = 24;
    public static final int LATENESS_SECONDS = 300;
    public static final int PROFILE_OVERLAP_SECONDS = 1;
    public static final int EVENT_OVERLAP_SECONDS = 24 * 60 * 60;
    public static final String API_REVISION = "2026-04-15";
    public static final String API_HOST = "a.klaviyo.com";
    public static final String PROFILES = "profiles";
    public static final String EVENT_PREFIX = "event:";

    private static final Pattern METRIC_ID = Pattern.compile("[A-Za-z0-9]{1,32}");

    private SyncPlan() {
    }

    /** Prints the configured job keys, one per line. Used by the shell runners. */
    public static void main(String[] args) {
        if (args.length != 1 || !"--print-job-keys".equals(args[0])) {
            throw new IllegalArgumentException("Expected --print-job-keys");
        }
        for (String jobKey : configuredJobKeys()) {
            System.out.println(jobKey);
        }
    }

    /** Job keys from the KLAVIYO_EVENT_METRICS environment variable. */
    public static Set<String> configuredJobKeys() {
        return jobKeys(System.getenv("KLAVIYO_EVENT_METRICS"));
    }

    /**
     * Builds the allowlist: "profiles" plus one "event:METRIC_ID" key per
     * comma-separated metric ID. Order is preserved; duplicates are rejected.
     */
    public static Set<String> jobKeys(String metricIdsCsv) {
        LinkedHashSet<String> keys = new LinkedHashSet<String>();
        keys.add(PROFILES);
        if (metricIdsCsv != null && metricIdsCsv.trim().length() > 0) {
            for (String raw : metricIdsCsv.split(",", -1)) {
                String metricId = requireMetricId(raw.trim());
                if (!keys.add(EVENT_PREFIX + metricId)) {
                    throw new IllegalArgumentException("Duplicate Klaviyo metric ID");
                }
            }
        }
        return Collections.unmodifiableSet(keys);
    }

    public static String requireMetricId(String metricId) {
        if (metricId == null || !METRIC_ID.matcher(metricId).matches()) {
            throw new IllegalArgumentException("Invalid Klaviyo metric ID");
        }
        return metricId;
    }

    /** Validates the shape of a job key: "profiles" or "event:METRIC_ID". */
    public static String requireJobKey(String jobKey) {
        if (PROFILES.equals(jobKey)) {
            return jobKey;
        }
        if (jobKey != null && jobKey.startsWith(EVENT_PREFIX)) {
            requireMetricId(jobKey.substring(EVENT_PREFIX.length()));
            return jobKey;
        }
        throw new IllegalArgumentException("Unknown Klaviyo job key");
    }

    /** Validates shape and membership in an allowlist. */
    public static String requireJobKey(String jobKey, Set<String> allowed) {
        requireJobKey(jobKey);
        if (!allowed.contains(jobKey)) {
            throw new IllegalArgumentException("Klaviyo job key is not configured");
        }
        return jobKey;
    }

    public static boolean isProfiles(String jobKey) {
        return PROFILES.equals(requireJobKey(jobKey));
    }

    public static String metricId(String jobKey) {
        if (isProfiles(jobKey)) {
            return null;
        }
        return jobKey.substring(EVENT_PREFIX.length());
    }

    public static String initialUrl(String jobKey, Instant start, Instant end) {
        requireJobKey(jobKey);
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("Window start must be before window end");
        }
        if (isProfiles(jobKey)) {
            Instant queryStart = start.minusSeconds(PROFILE_OVERLAP_SECONDS);
            String filter = "and(greater-than(updated," + queryStart
                + "),less-than(updated," + end + "))";
            return "https://" + API_HOST + "/api/profiles"
                + "?page%5Bsize%5D=" + PROFILE_PAGE_SIZE
                + "&additional-fields%5Bprofile%5D=subscriptions"
                + "&fields%5Bprofile%5D=email%2Cupdated%2Csubscriptions.email.marketing.can_receive_email_marketing%2Csubscriptions.sms.marketing.can_receive_sms_marketing"
                + "&sort=updated"
                + "&filter=" + encode(filter);
        }

        String metricId = metricId(jobKey);
        Instant queryStart = start.minusSeconds(EVENT_OVERLAP_SECONDS);
        String filter = "and(equals(metric_id,'" + metricId + "')"
            + ",greater-or-equal(datetime," + queryStart + ")"
            + ",less-than(datetime," + end + "))";
        return "https://" + API_HOST + "/api/events"
            + "?page%5Bsize%5D=" + EVENT_PAGE_SIZE
            + "&sort=datetime"
            + "&filter=" + encode(filter);
    }

    /**
     * Query for a window that has no usable cursor. Profiles are sorted by
     * updated ascending and every committed watermark W guarantees that all
     * rows with updated <= W are loaded, so a cursorless resume re-queries from
     * W (initialUrl subtracts PROFILE_OVERLAP_SECONDS, so rows tied at W are
     * read again and deduplicated by the ID-keyed MERGE). Event windows keep
     * their late-event overlap and always restart from the window start.
     */
    public static String resumeUrl(
        String jobKey,
        Instant windowStart,
        Instant windowEnd,
        Instant committedWatermark
    ) {
        if (!isProfiles(jobKey) || committedWatermark == null
            || !committedWatermark.isAfter(windowStart)) {
            return initialUrl(jobKey, windowStart, windowEnd);
        }
        return initialUrl(jobKey, committedWatermark, windowEnd);
    }

    /**
     * Watermark to commit with a profile page group whose largest updated
     * value is maxUpdated. Rows sharing maxUpdated may continue on the next
     * page, so the watermark stays PROFILE_OVERLAP_SECONDS behind it. It never
     * moves backwards. A maxUpdated at or after the window end is anomalous
     * (the query excludes it), so it never advances the watermark; window
     * completion commits the window end itself.
     */
    public static Instant progressiveWatermark(
        String jobKey,
        Instant committedWatermark,
        Instant windowEnd,
        Instant maxUpdated
    ) {
        if (!isProfiles(jobKey) || maxUpdated == null) {
            return committedWatermark;
        }
        if (!maxUpdated.isBefore(windowEnd)) {
            return committedWatermark;
        }
        Instant candidate = maxUpdated.minusSeconds(PROFILE_OVERLAP_SECONDS);
        return candidate.isAfter(committedWatermark) ? candidate : committedWatermark;
    }

    /**
     * Largest updated value of one committed page group, in API order, or null
     * when the group gives no safe evidence of progress: empty, not ascending
     * (for example a profile edited while the crawl was running), or any row
     * outside the window query bounds. A null candidate keeps the current
     * watermark; rows are still loaded and the cursor still advances.
     */
    public static Instant groupMaxUpdated(
        List<Instant> updatedInApiOrder,
        Instant windowStart,
        Instant windowEnd
    ) {
        Instant lowest = windowStart.minusSeconds(PROFILE_OVERLAP_SECONDS);
        Instant previous = null;
        for (Instant updated : updatedInApiOrder) {
            if (updated == null
                || !updated.isAfter(lowest)
                || !updated.isBefore(windowEnd)
                || (previous != null && updated.isBefore(previous))) {
                return null;
            }
            previous = updated;
        }
        return previous;
    }

    /** A worker's lease must outlive its whole budget, not just one commit. */
    public static int leaseSeconds(int maxWorkerSeconds) {
        return Math.max(MIN_LEASE_SECONDS, maxWorkerSeconds + LEASE_MARGIN_SECONDS);
    }

    public static int pagesPerCommit(String jobKey, String override) {
        if (!isProfiles(jobKey)) {
            return 1;
        }
        return boundedSetting(
            "KLAVIYO_PAGES_PER_COMMIT",
            override,
            PROFILE_PAGES_PER_COMMIT,
            1,
            MAX_PAGES_PER_COMMIT
        );
    }

    public static int boundedSetting(
        String name,
        String value,
        int fallback,
        int minimum,
        int maximum
    ) {
        int parsed;
        try {
            parsed = value == null || value.length() == 0
                ? fallback
                : Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " is not an integer");
        }
        if (parsed < minimum || parsed > maximum) {
            throw new IllegalArgumentException(name + " is outside the safe range");
        }
        return parsed;
    }

    /** 429/5xx backoff: Retry-After (capped at 60s) or capped exponential plus jitter. */
    public static long retryDelayMillis(String retryAfter, int attempt, long jitterMillis) {
        if (retryAfter != null && retryAfter.matches("[0-9]+")) {
            return Math.min(60_000L, Long.parseLong(retryAfter) * 1000L);
        }
        long base = Math.min(30_000L, 1000L << Math.min(attempt - 1, 5));
        return base + jitterMillis;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8)
            .replace("+", "%20");
    }
}
