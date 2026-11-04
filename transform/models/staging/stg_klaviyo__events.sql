-- One row per event, with the metric name from the metric map.
-- metric_name is null for metric IDs that are not in the map yet.

with source as (

    select * from {{ klaviyo_input('klaviyo', 'raw_klaviyo_events', 'example_synthetic_raw_klaviyo_events') }}

),

metric_map as (

    select
        cast(metric_id as {{ dbt.type_string() }}) as metric_id,
        trim(metric_name) as metric_name
    from {{ klaviyo_metric_map_relation() }}

),

events as (

    select
        {{ dbt.concat(["cast(metric_id as " ~ dbt.type_string() ~ ")", "'|'", "cast(id as " ~ dbt.type_string() ~ ")"]) }} as event_key,
        cast(id as {{ dbt.type_string() }}) as event_id,
        cast(metric_id as {{ dbt.type_string() }}) as metric_id,
        cast(profile_id as {{ dbt.type_string() }}) as profile_id,
        {{ klaviyo_to_utc('event_datetime') }} as event_at
    from source
    where id is not null
      and metric_id is not null

)

select
    events.event_key,
    events.event_id,
    events.metric_id,
    metric_map.metric_name,
    events.profile_id,
    events.event_at
from events
left join metric_map
    on events.metric_id = metric_map.metric_id
