{{
  config(
    alias = 'accounts_daily',
    )
}}

with

-- Here is the final model in my DAG.
-- The purpose of this model is to allow denormalisation of the base fct model to make self-serve easier for stakeholders.
-- The logic used in this model is intentionally simple as all the complex aggregating have been applied upstream.

fct_accounts_snapshot_day as (
    select *
    from {{ ref('fct_accounts_snapshot_day') }}
)

, dim_accounts as (
    select *
    from {{ ref('dim_accounts') }}
)

, dim_users as (
    select *
    from {{ ref('dim_users') }}
)

, final as (
    select
        fct_accounts_snapshot_day.date_day
        , dim_accounts.account_type
        , fct_accounts_snapshot_day.account_id
        , fct_accounts_snapshot_day.user_id
        , dim_users.first_account_created_at as user_first_account_created_at
        , fct_accounts_snapshot_day.account_created_at
        , fct_accounts_snapshot_day.account_last_closed_at
        , fct_accounts_snapshot_day.account_last_reopened_at
        , fct_accounts_snapshot_day.number_of_transactions
        , fct_accounts_snapshot_day.number_of_account_transactions_to_date
        , fct_accounts_snapshot_day.number_of_account_transactions_last_7_days
        , fct_accounts_snapshot_day.is_account_open
        , fct_accounts_snapshot_day.is_account_active_last_7_days
    from fct_accounts_snapshot_day
    left join dim_accounts
      on fct_accounts_snapshot_day.account_id = dim_accounts.account_id
    left join dim_users
      on fct_accounts_snapshot_day.user_id = dim_users.user_id
)

select *
from final
