-- Current state of each Klaviyo profile, with consent.

select
    profile_id,
    email,
    email is not null as has_email,
    case
        when email is not null then {{ dbt.split_part('email', "'@'", 2) }}
    end as email_domain,
    can_receive_email_marketing,
    can_receive_sms_marketing,
    profile_updated_at
from {{ ref('stg_klaviyo__profiles') }}
