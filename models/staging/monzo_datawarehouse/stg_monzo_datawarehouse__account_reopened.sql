{{
  config(
    materialized = 'view'
    , alias= 'stg_monzo_datawarehouse__account_reopened'
    )
}}

with

source as (
    select *
    from {{ source('monzo_accounts', 'account_reopened') }}
)

, final as (
    select
        -- ids
        safe_cast(account_id_hashed as string) as account_id
        --timestamps
        , safe_cast(reopened_ts as timestamp) as reopened_at
    from source
)

select *
from final