{{
    config(
        materialized='incremental',
        unique_key='event_key',
        on_schema_change='append_new_columns'
    )
}}

-- One row per Klaviyo event. Incremental: each run reprocesses the last
-- klaviyo_events_lookback_days days before the latest loaded event.
-- After changing the metric map, run with --full-refresh so older rows get
-- the new names.

select
    event_key,
    event_id,
    metric_id,
    metric_name,
    profile_id,
    event_at,
    cast(event_at as date) as event_date
from {{ ref('stg_klaviyo__events') }}

{% if is_incremental() %}
where event_at >= (
    select {{ dbt.dateadd('day', -1 * var('klaviyo_events_lookback_days', 3), 'max(event_at)') }}
    from {{ this }}
)
{% endif %}
