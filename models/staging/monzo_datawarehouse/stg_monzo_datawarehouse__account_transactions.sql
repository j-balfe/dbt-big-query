{{
  config(
    materialized = 'view'
    , alias= 'stg_monzo_datawarehouse__account_transactions'
    )
}}

with

source as (
    select *
    from {{ source('monzo_accounts', 'account_transactions') }}
)

, final as (
    select
        -- ids
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id_hashed']) }} as account_transaction_id
        , safe_cast(account_id_hashed as string) as account_id
        -- numbers
        , safe_cast(transactions_num as int) as number_of_transactions
        -- dates
        , safe_cast(date as date) as transaction_date
    from source
)

select *
from final
