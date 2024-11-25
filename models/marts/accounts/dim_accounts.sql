{{
  config(
    alias = 'dim_accounts'
    )
}}

with

stg_monzo_datawarehouse__account_created as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_created') }}
)

, stg_monzo_datawarehouse__account_closed as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_closed') }}
)

, stg_monzo_datawarehouse__account_reopened as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_reopened') }}
)

, stg_monzo_datawarehouse__account_transactions as (
    select *
    from {{ ref('stg_monzo_datawarehouse__account_transactions') }}
)

, transactions_aggregated_by_account as (
    select
        stg_monzo_datawarehouse__account_created.user_id
        , stg_monzo_datawarehouse__account_created.account_id
        , stg_monzo_datawarehouse__account_created.account_type
        , stg_monzo_datawarehouse__account_created.created_at as account_created_at
        , max(stg_monzo_datawarehouse__account_closed.closed_at) as account_last_closed_at
        , max(stg_monzo_datawarehouse__account_reopened.reopened_at) as account_last_reopened_at
        , max(stg_monzo_datawarehouse__account_transactions.transaction_date) as account_last_transaction_date
    from stg_monzo_datawarehouse__account_created
    left join stg_monzo_datawarehouse__account_closed
        on stg_monzo_datawarehouse__account_created.account_id = stg_monzo_datawarehouse__account_closed.account_id
    left join stg_monzo_datawarehouse__account_reopened
        on stg_monzo_datawarehouse__account_created.account_id = stg_monzo_datawarehouse__account_reopened.account_id
    left join stg_monzo_datawarehouse__account_transactions
        on stg_monzo_datawarehouse__account_created.account_id = stg_monzo_datawarehouse__account_transactions.account_id
    group by 1, 2, 3, 4
)

, final as (
    select
        account_id
        , user_id
        , account_type
        , account_created_at
        , account_last_closed_at
        , account_last_reopened_at
        , account_last_transaction_date
        , case
            when account_last_closed_at is null then true
            when account_last_reopened_at > account_last_closed_at then true
            else false
        end as is_account_open
    from transactions_aggregated_by_account
)

select *
from final
