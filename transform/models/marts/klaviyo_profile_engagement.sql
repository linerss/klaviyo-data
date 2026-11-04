-- One row per profile: first and last event, counts per configured metric
-- name, and activity flags. Profiles with no events have zero counts and
-- false flags. Events without a profile_id, or for a profile that is not in
-- the profiles table yet, are not counted.

{% set metrics = var('klaviyo_engagement_metrics', ['Opened Email', 'Clicked Email', 'Placed Order']) %}
{% set windows = var('klaviyo_activity_windows_days', [30, 90, 365]) %}

with profiles as (

    select profile_id from {{ ref('dim_klaviyo_profiles') }}

),

events as (

    select profile_id, metric_name, event_at
    from {{ ref('fct_klaviyo_events') }}
    where profile_id is not null

),

event_summary as (

    select
        profile_id,
        min(event_at) as first_event_at,
        max(event_at) as last_event_at,
        count(*) as total_event_count
        {%- for metric in metrics %}
        {%- set slug = klaviyo_metric_slug(metric) %},
        sum(case when metric_name = '{{ metric }}' then 1 else 0 end) as {{ slug }}_count,
        max(case when metric_name = '{{ metric }}' then event_at end) as last_{{ slug }}_at
        {%- endfor %}
    from events
    group by profile_id

),

as_of as (

    select {{ klaviyo_now_utc() }} as as_of_at

)

select
    profiles.profile_id,
    event_summary.first_event_at,
    event_summary.last_event_at,
    coalesce(event_summary.total_event_count, 0) as total_event_count,
    {%- for metric in metrics %}
    {%- set slug = klaviyo_metric_slug(metric) %}
    coalesce(event_summary.{{ slug }}_count, 0) as {{ slug }}_count,
    event_summary.last_{{ slug }}_at,
    {%- endfor %}
    case
        when event_summary.last_event_at is not null
            then {{ dbt.datediff('event_summary.last_event_at', 'as_of.as_of_at', 'day') }}
    end as days_since_last_event,
    {%- for days in windows %}
    coalesce(
        event_summary.last_event_at >= {{ dbt.dateadd('day', -1 * days, 'as_of.as_of_at') }},
        false
    ) as is_active_last_{{ days }}d,
    {%- endfor %}
    as_of.as_of_at as activity_as_of_at
from profiles
cross join as_of
left join event_summary
    on profiles.profile_id = event_summary.profile_id
