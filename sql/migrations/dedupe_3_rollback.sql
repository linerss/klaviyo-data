-- Undo the cutover: swapping again restores the pre-dedupe tables.
-- Only valid while the _DEDUP tables still exist and before workers have
-- written new rows you want to keep. Otherwise restore from the _BACKUP clones.
alter table RAW_KLAVIYO_PROFILES swap with RAW_KLAVIYO_PROFILES_DEDUP;
alter table RAW_KLAVIYO_EVENTS swap with RAW_KLAVIYO_EVENTS_DEDUP;
