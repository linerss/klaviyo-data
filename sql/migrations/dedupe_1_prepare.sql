-- One-time dedupe of raw tables that were loaded by an append-only process
-- before switching to the MERGE-based sync. Three steps:
--   1. prepare  (this file): back up, then build deduplicated copies
--   2. cutover : swap the copies in
--   3. rollback: swap back if needed (the swap is its own inverse)
--
-- Pause all sync workers before step 2. Compare row counts and spot-check
-- the _DEDUP tables before swapping. Drop the _BACKUP and _DEDUP tables
-- only after you are satisfied.
--
-- Run in the target database and schema (for example RAW.KLAVIYO).

create table if not exists RAW_KLAVIYO_PROFILES_BACKUP
    clone RAW_KLAVIYO_PROFILES;

create table if not exists RAW_KLAVIYO_EVENTS_BACKUP
    clone RAW_KLAVIYO_EVENTS;

-- Latest row per profile ID wins.
create or replace transient table RAW_KLAVIYO_PROFILES_DEDUP
    like RAW_KLAVIYO_PROFILES;

insert into RAW_KLAVIYO_PROFILES_DEDUP
    (ID, EMAIL, EMAIL_MARKETING, SMS_MARKETING, UPDATED)
select ID, EMAIL, EMAIL_MARKETING, SMS_MARKETING, UPDATED
from RAW_KLAVIYO_PROFILES
qualify row_number() over (
    partition by ID
    order by UPDATED desc nulls last
) = 1;

-- One row per (metric, event ID).
create or replace transient table RAW_KLAVIYO_EVENTS_DEDUP
    like RAW_KLAVIYO_EVENTS;

insert into RAW_KLAVIYO_EVENTS_DEDUP
    (ID, METRIC_ID, PROFILE_ID, EVENT_DATETIME)
select ID, METRIC_ID, PROFILE_ID, EVENT_DATETIME
from RAW_KLAVIYO_EVENTS
qualify row_number() over (
    partition by METRIC_ID, ID
    order by EVENT_DATETIME desc nulls last
) = 1;

-- Checks to run before cutover:
-- select count(*), count(distinct ID) from RAW_KLAVIYO_PROFILES_DEDUP;
-- select count(*), count(distinct METRIC_ID || ':' || ID) from RAW_KLAVIYO_EVENTS_DEDUP;
