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
        -- using safe_cast in case the data types upstream change.
        -- ids: cast as strings for consistency
        -- created account_transaction_id to show an alternative method of testing for uniqueness instead of using the - dbt_utils.unique_combination_of_columns test.
        {{ dbt_utils.generate_surrogate_key(['date', 'account_id_hashed']) }} as account_transaction_id
        , safe_cast(account_id_hashed as string) as account_id
        -- numbers: all numbers cast as int. No decimal places required.
        , safe_cast(transactions_num as int) as number_of_transactions
        -- dates: cast as date and added the _date suffix.
        , safe_cast(date as date) as transaction_date
    from source
)

select *
from final
