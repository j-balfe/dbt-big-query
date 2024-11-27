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
        -- using safe_cast to account for data types changing from the source.
        -- ids: cast as strings for consistency
        safe_cast(account_id_hashed as string) as account_id
        --timestamps: source timestamp is formatted as UTC, standard for all timestamps is UTC so I haven't added a _utc suffix.
        , safe_cast(closed_ts as timestamp) as closed_at
    from source
    -- qualified by account_id and closed_ts to make sure duplicate rows are removed from this model.
    qualify row_number() over (partition by account_id, closed_ts order by closed_at) = 1
)

select *
from final
