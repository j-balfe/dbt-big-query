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
        -- using safe_cast in case the data types upstream change.
        -- ids: cast as strings for consistency
        safe_cast(account_id_hashed as string) as account_id
        --timestamps: source timestamp is formatted as UTC, standard for all timestamps is UTC so I haven't added a _utc suffix.
        , safe_cast(reopened_ts as timestamp) as reopened_at
    from source
)

select *
from final
