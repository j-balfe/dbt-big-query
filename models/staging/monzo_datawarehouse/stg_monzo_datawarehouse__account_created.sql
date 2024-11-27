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
        -- ids: cast as strings for consistency
        safe_cast(account_id_hashed as string) as account_id
        , safe_cast(user_id_hashed as string) as user_id
        --strings: if the account type is null then fill as 'unassigned'. This makes sure stakeholders are not confused by nulls
        -- in downstream reporting.
        , if(account_type is null, 'unassigned', safe_cast(account_type as string)) as account_type
        --timestamps: source timestamp is formatted as UTC, standard for all timestamps is UTC so I haven't added a _utc suffix.
        , safe_cast(created_ts as timestamp) as created_at
    from source
)

select *
from final
