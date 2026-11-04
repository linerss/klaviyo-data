-- Raw landing tables. Run in the target database and schema, for example:
--   use database RAW; use schema KLAVIYO;
--
-- Loads are MERGEs keyed on ID (profiles) and (METRIC_ID, ID) (events), so
-- these tables hold one row per key. Snowflake does not enforce primary keys;
-- the MERGE is what keeps them unique.

create schema if not exists KLAVIYO;

create table if not exists RAW_KLAVIYO_PROFILES (
    ID varchar(50) not null,
    EMAIL varchar,
    EMAIL_MARKETING boolean,
    SMS_MARKETING boolean,
    UPDATED timestamp_tz(9) not null,
    primary key (ID)
);

create table if not exists RAW_KLAVIYO_EVENTS (
    ID varchar(50) not null,
    METRIC_ID varchar(50) not null,
    PROFILE_ID varchar(50),
    EVENT_DATETIME timestamp_tz(9) not null,
    primary key (METRIC_ID, ID)
);

-- Optional: human-readable names for the metric IDs you sync. Fill it from
-- the Klaviyo Metrics API or by hand; the pipeline does not read it.
create table if not exists LOOKUP_KLAVIYO_METRICS (
    METRIC_ID varchar(50) not null,
    METRIC_NAME varchar(200),
    METRIC_TYPE varchar(50),
    primary key (METRIC_ID)
);
