{{
  config(
    materialized = 'view'
    , alias= 'stg_monzo_datawarehouse__account_created'
    )
}}

with

source as (
    select *
    from {{ source('monzo_accounts', 'account_created') }}
)

, final as (
select
    -- using safe_cast in case the data types upstream change.
    -- ids: formatted as strings to consistency in joining within the project. All IDs should be formatted as strings.
    safe_cast(account_id_hashed as string) as account_id
    , safe_cast(user_id_hashed as string) as user_id
    --strings
    , if(account_type is null, 'unassigned', safe_cast(account_type as string)) as account_type
    --timestamps: no conversion of timestamps as source is already in UTC
    , safe_cast(created_ts as timestamp) as created_at
from source
)

select *
from final
