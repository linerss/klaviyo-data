-- Profiles that can be emailed: consented to email marketing and have an
-- email address. Set the klaviyo_mailable_active_within_days var to also
-- require any event within that many days.

{% set active_within_days = var('klaviyo_mailable_active_within_days', none) %}

select
    profiles.profile_id,
    profiles.email,
    profiles.email_domain,
    profiles.can_receive_email_marketing,
    profiles.can_receive_sms_marketing,
    profiles.profile_updated_at,
    engagement.last_event_at,
    engagement.days_since_last_event
from {{ ref('dim_klaviyo_profiles') }} as profiles
left join {{ ref('klaviyo_profile_engagement') }} as engagement
    on profiles.profile_id = engagement.profile_id
where profiles.can_receive_email_marketing
  and profiles.email is not null
{%- if active_within_days is not none %}
  and engagement.last_event_at >= {{ dbt.dateadd('day', -1 * (active_within_days | int), 'engagement.activity_as_of_at') }}
{%- endif %}
