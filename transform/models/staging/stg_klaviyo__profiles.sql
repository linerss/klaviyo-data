-- Latest row per profile ID, with a normalized email and boolean consent flags.
-- The raw table is already unique on ID (MERGE), the dedupe is a safeguard.

with source as (

    select * from {{ klaviyo_input('klaviyo', 'raw_klaviyo_profiles', 'example_synthetic_raw_klaviyo_profiles') }}

),

renamed as (

    select
        cast(id as {{ dbt.type_string() }}) as profile_id,
        nullif(lower(trim(email)), '') as email,
        coalesce(cast(email_marketing as boolean), false) as can_receive_email_marketing,
        coalesce(cast(sms_marketing as boolean), false) as can_receive_sms_marketing,
        {{ klaviyo_to_utc('updated') }} as profile_updated_at
    from source
    where id is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by profile_id
            order by profile_updated_at desc
        ) as row_num
    from renamed

)

select
    profile_id,
    email,
    can_receive_email_marketing,
    can_receive_sms_marketing,
    profile_updated_at
from ranked
where row_num = 1
