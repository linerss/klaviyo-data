package com.greyfielddata.klaviyo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class SyncPlanTest {
    private static final String METRIC_A = "MetricA1";
    private static final String METRIC_B = "MetricB2";

    @Test
    void jobKeysAreProfilesPlusConfiguredMetricsInOrder() {
        Set<String> keys = SyncPlan.jobKeys(METRIC_A + ", " + METRIC_B);
        assertEquals(List.of("profiles", "event:" + METRIC_A, "event:" + METRIC_B),
            List.copyOf(keys));
        assertEquals(Set.of("profiles"), SyncPlan.jobKeys(null));
        assertEquals(Set.of("profiles"), SyncPlan.jobKeys(" "));
    }

    @Test
    void invalidOrDuplicateMetricIdsAreRejected() {
        assertThrows(IllegalArgumentException.class,
            () -> SyncPlan.jobKeys(METRIC_A + "," + METRIC_A));
        assertThrows(IllegalArgumentException.class, () -> SyncPlan.jobKeys("bad id"));
        assertThrows(IllegalArgumentException.class, () -> SyncPlan.jobKeys("x'),or(1"));
        assertThrows(IllegalArgumentException.class, () -> SyncPlan.jobKeys(","));
    }

    @Test
    void jobKeyShapeAndAllowlistAreEnforced() {
        assertThrows(IllegalArgumentException.class,
            () -> SyncPlan.requireJobKey("event:not-allowed"));
        assertThrows(IllegalArgumentException.class, () -> SyncPlan.requireJobKey("orders"));
        Set<String> allowed = SyncPlan.jobKeys(METRIC_A);
        assertEquals("event:" + METRIC_A, SyncPlan.requireJobKey("event:" + METRIC_A, allowed));
        assertThrows(IllegalArgumentException.class,
            () -> SyncPlan.requireJobKey("event:" + METRIC_B, allowed));
        assertTrue(SyncPlan.isProfiles("profiles"));
        assertFalse(SyncPlan.isProfiles("event:" + METRIC_A));
        assertNull(SyncPlan.metricId("profiles"));
        assertEquals(METRIC_A, SyncPlan.metricId("event:" + METRIC_A));
    }

    @Test
    void buildsBoundedSortedProfileWindow() {
        String url = SyncPlan.initialUrl(
            "profiles",
            Instant.parse("2026-01-02T00:00:00Z"),
            Instant.parse("2026-01-03T00:00:00Z")
        );
        assertTrue(url.startsWith("https://a.klaviyo.com/api/profiles?"));
        assertTrue(url.contains("page%5Bsize%5D=100"), "profile page size must be bounded");
        assertTrue(url.contains("sort=updated"), "profiles must be sorted by updated");
        assertTrue(url.contains("greater-than%28updated%2C2026-01-01T23%3A59%3A59Z%29"),
            "overlapping profile lower bound missing");
        assertTrue(url.contains("less-than%28updated%2C2026-01-03T00%3A00%3A00Z%29"),
            "profile upper bound missing");
        assertTrue(url.contains("additional-fields%5Bprofile%5D=subscriptions"),
            "profile subscriptions must remain included");
        assertTrue(url.contains("fields%5Bprofile%5D=email%2Cupdated%2Csubscriptions.email.marketing.can_receive_email_marketing%2Csubscriptions.sms.marketing.can_receive_sms_marketing"),
            "profile projection must retain all fields consumed by the loader");
    }

    @Test
    void buildsBoundedSortedEventWindow() {
        String url = SyncPlan.initialUrl(
            "event:" + METRIC_A,
            Instant.parse("2026-01-02T00:00:00Z"),
            Instant.parse("2026-01-03T00:00:00Z")
        );
        assertTrue(url.startsWith("https://a.klaviyo.com/api/events?"));
        assertTrue(url.contains("page%5Bsize%5D=200"), "event page size must be bounded");
        assertTrue(url.contains("sort=datetime"), "events must be sorted by datetime");
        assertTrue(url.contains("equals%28metric_id%2C%27" + METRIC_A + "%27%29"),
            "event metric filter missing");
        assertTrue(url.contains("greater-or-equal%28datetime%2C2026-01-01T00%3A00%3A00Z%29"),
            "late-event overlap missing");
        assertTrue(url.contains("less-than%28datetime%2C2026-01-03T00%3A00%3A00Z%29"),
            "event upper bound missing");
    }

    @Test
    void emptyOrReversedWindowsAreRejected() {
        Instant t = Instant.parse("2026-01-02T00:00:00Z");
        assertThrows(IllegalArgumentException.class, () -> SyncPlan.initialUrl("profiles", t, t));
        assertThrows(IllegalArgumentException.class,
            () -> SyncPlan.initialUrl("profiles", t, t.minusSeconds(1)));
    }

    @Test
    void untrustedPaginationUrlsAreRejected() {
        assertEquals("https://a.klaviyo.com/api/profiles?page%5Bcursor%5D=abc",
            IncrementalSync.requireKlaviyoUrl("https://a.klaviyo.com/api/profiles?page%5Bcursor%5D=abc"));
        for (String bad : List.of(
            "http://a.klaviyo.com/api/profiles",
            "https://example.com/api/profiles",
            "https://user@a.klaviyo.com/api/profiles",
            "https://a.klaviyo.com:8443/api/profiles")) {
            assertThrows(IllegalArgumentException.class,
                () -> IncrementalSync.requireKlaviyoUrl(bad), bad);
        }
    }

    @Test
    void consentValuesMapToNullableBooleans() {
        assertEquals(Boolean.TRUE, IncrementalSync.consent(true));
        assertEquals(Boolean.TRUE, IncrementalSync.consent("SUBSCRIBED"));
        assertEquals(Boolean.FALSE, IncrementalSync.consent("false"));
        assertEquals(Boolean.FALSE, IncrementalSync.consent("unsubscribed"));
        assertNull(IncrementalSync.consent(null));
        assertNull(IncrementalSync.consent("NEVER_SUBSCRIBED"));
    }

    @Test
    void storedErrorsAreSingleLineAndBounded() {
        String error = IncrementalSync.safeError(new IllegalStateException("a\nb\tc" + "x".repeat(600)));
        assertFalse(error.contains("\n"));
        assertFalse(error.contains("\t"));
        assertEquals(500, error.length());
    }

    @Test
    void snowflakeIdentifiersAreValidated() {
        assertEquals("RAW_KLAVIYO_PROFILES",
            SnowflakeSession.requireIdentifier("RAW_KLAVIYO_PROFILES"));
        for (String bad : new String[] {null, "", "raw", "A;DROP", "A.B", "1ABC"}) {
            assertThrows(IllegalArgumentException.class,
                () -> SnowflakeSession.requireIdentifier(bad));
        }
    }
}
