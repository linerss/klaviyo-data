-- One row per job key ("profiles" or "event:<METRIC_ID>"). Workers only
-- update existing rows; seed them with:
--   java ... com.greyfielddata.klaviyo.StateBootstrap seed
-- or with the example MERGE at the end of this file.
--
-- Columns:
--   COMMITTED_WATERMARK_TZ  everything at or before this point is loaded
--   TARGET_CUTOFF_TZ        the shared cohort goal for all streams
--   ACTIVE_WINDOW_*_TZ      the 24-hour window currently being paged
--   NEXT_CURSOR, PAGE_SEQ   Klaviyo pagination position inside the window
--   LEASE_TOKEN, LEASE_EXPIRES_AT  single-writer lease per stream
--   VERSION                 optimistic concurrency guard on every update

create table if not exists KLAVIYO_SYNC_STATE (
    JOB_KEY varchar not null,
    DATASET varchar not null,
    METRIC_ID varchar,
    COMMITTED_WATERMARK_TZ timestamp_tz not null,
    TARGET_CUTOFF_TZ timestamp_tz not null,
    ACTIVE_WINDOW_START_TZ timestamp_tz,
    ACTIVE_WINDOW_END_TZ timestamp_tz,
    NEXT_CURSOR varchar,
    PAGE_SEQ number not null default 0,
    STATUS varchar not null default 'IDLE',
    RUN_ID varchar,
    LEASE_TOKEN varchar,
    LEASE_EXPIRES_AT timestamp_tz,
    VERSION number not null default 0,
    ROWS_WRITTEN number not null default 0,
    LAST_ERROR varchar,
    LAST_PROGRESS_AT timestamp_tz not null default current_timestamp(),
    UPDATED_AT timestamp_tz not null default current_timestamp(),
    primary key (JOB_KEY)
);

-- Example seed for profiles plus one event metric. Replace <METRIC_ID>.
-- For an event stream that already has data, starting one second before the
-- latest loaded event avoids re-reading history; the 24-hour late-event
-- overlap still re-reads the most recent day.
merge into KLAVIYO_SYNC_STATE target
using (
    select 'profiles' JOB_KEY, 'profiles' DATASET, null METRIC_ID,
        '2020-01-01T00:00:00Z'::timestamp_tz COMMITTED_WATERMARK_TZ,
        dateadd(minute,-5,current_timestamp()) TARGET_CUTOFF_TZ
    union all
    select 'event:<METRIC_ID>', 'events', '<METRIC_ID>',
        coalesce(
            (select dateadd(second,-1,max(EVENT_DATETIME))
             from RAW_KLAVIYO_EVENTS where METRIC_ID='<METRIC_ID>'),
            '2020-01-01T00:00:00Z'::timestamp_tz),
        dateadd(minute,-5,current_timestamp())
) source
on target.JOB_KEY=source.JOB_KEY
when not matched then insert (
    JOB_KEY, DATASET, METRIC_ID, COMMITTED_WATERMARK_TZ, TARGET_CUTOFF_TZ,
    STATUS, VERSION, ROWS_WRITTEN, LAST_PROGRESS_AT, UPDATED_AT
) values (
    source.JOB_KEY, source.DATASET, source.METRIC_ID,
    source.COMMITTED_WATERMARK_TZ, source.TARGET_CUTOFF_TZ,
    'IDLE', 0, 0, current_timestamp(), current_timestamp()
);
