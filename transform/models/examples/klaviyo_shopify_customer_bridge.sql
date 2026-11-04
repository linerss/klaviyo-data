{{ config(enabled=var('klaviyo_enable_shopify_bridge', false)) }}

-- EXAMPLE MODEL, disabled by default.
-- Joins Klaviyo profiles to a Shopify customers table that you provide, on
-- normalized email (lowercased and trimmed on both sides). This repo does
-- not load Shopify data. Adjust the shopify_* vars, or copy this model and
-- adapt it to your own Shopify schema.
--
-- An email can belong to more than one profile or customer, so this is a
-- many-to-many bridge. Only rows that match on email are returned.

with shopify_customers as (

    select
        cast({{ var('shopify_customer_id_column', 'ID') }} as {{ dbt.type_string() }}) as shopify_customer_id,
        nullif(lower(trim({{ var('shopify_customer_email_column', 'EMAIL') }})), '') as email
    from {{ klaviyo_input('shopify', 'customers', 'example_synthetic_shopify_customers') }}

),

klaviyo_profiles as (

    select profile_id, email, can_receive_email_marketing
    from {{ ref('dim_klaviyo_profiles') }}
    where email is not null

)

select
    {{ dbt.concat(["klaviyo_profiles.profile_id", "'|'", "shopify_customers.shopify_customer_id"]) }} as bridge_key,
    klaviyo_profiles.profile_id as klaviyo_profile_id,
    shopify_customers.shopify_customer_id,
    klaviyo_profiles.email,
    klaviyo_profiles.can_receive_email_marketing
from klaviyo_profiles
inner join shopify_customers
    on klaviyo_profiles.email = shopify_customers.email
