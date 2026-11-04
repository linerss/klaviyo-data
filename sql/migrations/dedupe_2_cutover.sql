-- Swap the deduplicated copies into place. Workers must be paused.
-- Note: the _DEDUP tables were created TRANSIENT, so after the swap the live
-- tables are transient (no Fail-safe). If you need permanent tables, create
-- the _DEDUP tables as permanent in step 1 instead.
alter table RAW_KLAVIYO_PROFILES swap with RAW_KLAVIYO_PROFILES_DEDUP;
alter table RAW_KLAVIYO_EVENTS swap with RAW_KLAVIYO_EVENTS_DEDUP;
