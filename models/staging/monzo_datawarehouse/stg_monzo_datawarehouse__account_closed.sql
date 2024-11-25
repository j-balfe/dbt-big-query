{{
  config(
    materialized = 'view'
    , alias= 'stg_monzo_datawarehouse__account_closed'
    )
}}

with

source as (
    select *
    from {{ source('monzo_accounts', 'account_closed') }}
)

, final as (
    select
        -- ids
        safe_cast(account_id_hashed as string) as account_id
        --timestamps
        , safe_cast(closed_ts as timestamp) as closed_at
    from source
    qualify row_number() over (partition by account_id, closed_ts order by closed_at) = 1
)

select *
from final
