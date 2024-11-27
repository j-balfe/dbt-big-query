{{
  config(
    alias = 'dim_accounts'
    )
}}

with

-- each source model is defined at the top of the file so it's clear for other users which models are being used.
-- Alternatively, you could check the upstream tables using the lineage feature within dbt-power-user but that
-- assumes they have it installed.
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

-- cte to aggregate information at an account level.
-- A simple left join from each source model means the data is fanned out by all the closure, reopening and transaction events.
-- Then we group by the high level ids and dimension to show the account_last_x values.
, transactions_aggregated_by_account as (
    select
        stg_monzo_datawarehouse__account_created.user_id
        , stg_monzo_datawarehouse__account_created.account_id
        , stg_monzo_datawarehouse__account_created.account_type
        , stg_monzo_datawarehouse__account_created.created_at as account_created_at
        , max(stg_monzo_datawarehouse__account_closed.closed_at) as account_last_closed_at
        , max(stg_monzo_datawarehouse__account_reopened.reopened_at) as account_last_reopened_at
        , max(stg_monzo_datawarehouse__account_transactions.transaction_date) as account_last_transaction_date
        , sum(stg_monzo_datawarehouse__account_transactions.transaction_date) as total_account_transactions
    from stg_monzo_datawarehouse__account_created
    left join stg_monzo_datawarehouse__account_closed
        on stg_monzo_datawarehouse__account_created.account_id = stg_monzo_datawarehouse__account_closed.account_id
    left join stg_monzo_datawarehouse__account_reopened
        on stg_monzo_datawarehouse__account_created.account_id = stg_monzo_datawarehouse__account_reopened.account_id
    left join stg_monzo_datawarehouse__account_transactions
        on stg_monzo_datawarehouse__account_created.account_id = stg_monzo_datawarehouse__account_transactions.account_id
    group by 1, 2, 3, 4
)

-- It's not possible to reference an alias assigned to an aggregation within another calculation in the same cte in BigQuery.
-- To simplify the readability of this model I've moved is_account_open boolean to another cte so I don't have to nest log calcs.
-- The is_account_open logic comes from the take_home_task_document, where it says an account is open if its not been closed,
-- or the account have been closed but has since been reopened.
, final as (
    select
        account_id
        , user_id
        , account_type
        , account_created_at
        , account_last_closed_at
        , account_last_reopened_at
        , account_last_transaction_date
        , total_account_transactions
        , case
            when account_last_closed_at is null then true
            when account_last_reopened_at > account_last_closed_at then true
            else false
        end as is_account_open
    from transactions_aggregated_by_account
)

select *
from final
