-- Least-privilege role for the pipeline. Replace the <PLACEHOLDERS> and run
-- as a role that can create roles and grant on the objects below
-- (for example SECURITYADMIN plus the object owner).
--
-- Defaults used by the code: database RAW, schema KLAVIYO,
-- role KLAVIYO_PIPELINE_ROLE. Change them here and in .env together.

create role if not exists KLAVIYO_PIPELINE_ROLE;

grant usage on warehouse <YOUR_WAREHOUSE> to role KLAVIYO_PIPELINE_ROLE;
grant usage on database RAW to role KLAVIYO_PIPELINE_ROLE;
-- CREATE TABLE is needed because workers create session-scoped TEMP
-- stage tables (Snowflake requires the privilege for temporary tables too).
grant usage, create table on schema RAW.KLAVIYO to role KLAVIYO_PIPELINE_ROLE;

grant select, insert, update, delete
    on table RAW.KLAVIYO.RAW_KLAVIYO_PROFILES
    to role KLAVIYO_PIPELINE_ROLE;
grant select, insert, update, delete
    on table RAW.KLAVIYO.RAW_KLAVIYO_EVENTS
    to role KLAVIYO_PIPELINE_ROLE;
grant select, insert, update, delete
    on table RAW.KLAVIYO.KLAVIYO_SYNC_STATE
    to role KLAVIYO_PIPELINE_ROLE;

-- The service user authenticates with a key pair (see README).
grant role KLAVIYO_PIPELINE_ROLE to user <YOUR_SERVICE_USER>;
