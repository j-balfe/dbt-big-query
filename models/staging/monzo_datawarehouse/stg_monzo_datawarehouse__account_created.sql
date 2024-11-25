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
    -- ids
    safe_cast(account_id_hashed as string) as account_id
    , safe_cast(user_id_hashed as string) as user_id
    --strings
    , safe_cast(account_type as string) as account_type
    --timestamps
    , safe_cast(created_ts as timestamp) as created_at
from source
)

select *
from final
